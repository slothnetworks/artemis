package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/hamba/cmd/v2"
	lctx "github.com/hamba/logger/v2/ctx"
	"github.com/slothnetworks/artemis/internal/api"
	"github.com/slothnetworks/artemis/internal/db/clickhouse"
	"github.com/slothnetworks/artemis/internal/db/postgres"
	"github.com/slothnetworks/artemis/internal/manager"
	pb "github.com/slothnetworks/artemis/internal/pb/artemis/v1"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc"
)

func runManager(c *cli.Context) error {
	ctx, cancel := context.WithCancel(c.Context)
	defer cancel()

	log, err := cmd.NewLogger(c)
	if err != nil {
		return err
	}

	var opts []manager.Option
	if dsn := c.String(flagClickHouseDSN); dsn != "" {
		sink, err := clickhouse.Open(ctx, dsn)
		if err != nil {
			return err
		}
		defer func() { _ = sink.Close() }()
		opts = append(opts, manager.WithSampleSink(sink))
		log.Info("ClickHouse sample storage enabled")
	}
	if dsn := c.String(flagPostgresDSN); dsn != "" {
		sink, err := postgres.Open(ctx, dsn)
		if err != nil {
			return err
		}
		defer func() { _ = sink.Close() }()
		opts = append(opts, manager.WithEventSink(sink))
		log.Info("PostgreSQL event storage enabled")
	}

	mgr := manager.New(log, opts...)

	// gRPC server (AgentLink).
	grpcAddr := c.String(flagGRPCAddr)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return err
	}
	gsrv := grpc.NewServer(grpc.ChainStreamInterceptor(manager.TokenStreamInterceptor(c.String(flagAgentToken))))
	pb.RegisterAgentLinkServer(gsrv, mgr)

	go func() {
		log.Info("Serving gRPC AgentLink", lctx.Str("addr", grpcAddr))
		if err := gsrv.Serve(lis); err != nil {
			log.Error("gRPC server error", lctx.Err(err))
			cancel()
		}
	}()

	// HTTP server (REST + SSE).
	httpAddr := c.String(flagHTTPAddr)
	apiSrv := api.New(mgr, log)
	hsrv := &http.Server{ //nolint:gosec // local control-plane endpoint
		Addr:    httpAddr,
		Handler: api.LogMiddleware(log, apiSrv.Handler()),
	}
	go func() {
		log.Info("Serving HTTP API", lctx.Str("addr", httpAddr))
		if err := hsrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server error", lctx.Err(err))
			cancel()
		}
	}()

	go mgr.RunMaintenance(ctx, 30*time.Second)

	<-ctx.Done()
	log.Info("Shutting down")

	gsrv.GracefulStop()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	_ = hsrv.Shutdown(shutCtx)

	return nil
}
