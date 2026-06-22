package main

import (
	"context"
	"time"

	"github.com/hamba/cmd/v2"
	lctx "github.com/hamba/logger/v2/ctx"
	"github.com/slothnetworks/artemis/internal/agent"
	"github.com/urfave/cli/v2"
)

func runAgent(c *cli.Context) error {
	ctx, cancel := context.WithCancel(c.Context)
	defer cancel()

	log, err := cmd.NewLogger(c)
	if err != nil {
		return err
	}

	protocol := c.String(flagProtocol)

	var targets []agent.Target
	for _, addr := range c.StringSlice(flagTargets) {
		targets = append(targets, agent.Target{Addr: addr, Protocol: protocol})
	}

	cfg := agent.Config{
		Name:          c.String(flagAgentName),
		ManagerAddr:   c.String(flagManagerAddr),
		Token:         c.String(flagAgentToken),
		ListenAddr:    c.String(flagAddr),
		Protocols:     []string{flagProtocolTCP, flagProtocolUDP},
		Tags:          c.StringSlice(flagTags),
		Group:         c.String(flagGroup),
		Targets:       targets,
		FlushInterval: c.Duration(flagFlushInterval),
		Backoff:       c.Duration(flagConnBackoff),
		Interval:      c.Duration(flagSendInterval),
		ReadTimeout:   c.Duration(flagReadTimeout),
		WriteTimeout:  c.Duration(flagWriteTimeout),
		PayloadSize:   c.Int(flagPayloadSize),
	}
	if cfg.Name == "" {
		cfg.Name = cfg.ListenAddr
	}

	if c.Bool(flagDev) {
		log.Warn("Dev mode enabled")
	}

	go func() {
		if err := agent.New(cfg, log).Run(ctx); err != nil {
			log.Error("Agent stopped", lctx.Err(err))
		}
		cancel()
	}()

	<-ctx.Done()
	log.Info("Shutting down")
	time.Sleep(50 * time.Millisecond)
	return nil
}
