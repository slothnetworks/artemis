package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/hamba/cmd/v2"
	lctx "github.com/hamba/logger/v2/ctx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/slothnetworks/artemis/internal/probe"
	"github.com/slothnetworks/artemis/internal/stats"
	"github.com/urfave/cli/v2"
)

func runClient(c *cli.Context) error {
	ctx, cancel := context.WithCancel(c.Context)
	defer cancel()

	log, err := cmd.NewLogger(c)
	if err != nil {
		return err
	}

	protocol := c.String(flagProtocol)
	if protocol != flagProtocolTCP && protocol != flagProtocolUDP {
		return fmt.Errorf("unsupported protocol: %s", protocol)
	}

	addr := c.String(flagAddr)
	log = log.With(lctx.Str("protocol", protocol))

	// Stats: a rolling collector always, plus Prometheus when metrics are served.
	rolling := stats.NewRollingCollector(0)
	collectors := []stats.Collector{rolling}

	if metricsAddr := c.String(flagMetricsAddr); metricsAddr != "" {
		reg := prometheus.NewRegistry()
		collectors = append(collectors, stats.NewPromCollector(reg))

		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		srv := &http.Server{Addr: metricsAddr, Handler: mux} //nolint:gosec // local metrics endpoint
		go func() {
			log.Info("Serving metrics", lctx.Str("addr", metricsAddr))
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("Metrics server error", lctx.Err(err))
			}
		}()
		defer func() { _ = srv.Close() }()
	}

	coll := stats.MultiCollector(collectors)

	reporter := stats.NewReporter(rolling, log, c.Duration(flagReportInterval))
	go reporter.Run(ctx)

	client := probe.NewClient(
		c.Duration(flagConnBackoff),
		c.Duration(flagSendInterval),
		c.Duration(flagReadTimeout),
		c.Duration(flagWriteTimeout),
		log,
		probe.WithCollector(coll),
		probe.WithPayloadSize(c.Int(flagPayloadSize)),
		probe.WithPayloadSizes(c.IntSlice(flagPayloadSizes)),
		probe.WithBurst(c.Int(flagBurst)),
	)
	go func() {
		client.Run(ctx, protocol, addr)
		cancel()
	}()

	<-ctx.Done()

	log.Info("Shutting down")

	return nil
}
