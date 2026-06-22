package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/ettle/strcase"
	"github.com/hamba/cmd/v2"
	_ "github.com/joho/godotenv/autoload"
	"github.com/urfave/cli/v2"
)

const (
	flagProtocol    = "protocol"
	flagProtocolTCP = "tcp"
	flagProtocolUDP = "udp"
	flagAddr        = "addr"

	flagReadTimeout  = "read-timeout"
	flagWriteTimeout = "write-timeout"

	flagConnBackoff  = "backoff"
	flagSendInterval = "interval"

	flagPayloadSize    = "payload-size"
	flagPayloadSizes   = "payload-sizes"
	flagBurst          = "burst"
	flagMetricsAddr    = "metrics-addr"
	flagReportInterval = "report-interval"

	flagHTTPAddr      = "http-addr"
	flagGRPCAddr      = "grpc-addr"
	flagAgentToken    = "agent-token"
	flagPostgresDSN   = "postgres-dsn"
	flagClickHouseDSN = "clickhouse-dsn"
	flagAdminUser     = "admin-user"
	flagAdminPassHash = "admin-pass-hash"
	flagDev           = "dev"
	flagManagerAddr   = "manager-addr"
	flagAgentName     = "agent-name"
	flagGroup         = "group"
	flagTags          = "tags"
	flagTargets       = "targets"
	flagFlushInterval = "flush-interval"
)

var version = "¯\\_(ツ)_/¯"

var commands = []*cli.Command{
	{
		Name:  "client",
		Usage: "Run the connqc client",
		Flags: cmd.Flags{
			&cli.StringFlag{
				Name: flagProtocol,
				Usage: fmt.Sprintf(
					"The protocol for the connection. Supported protocols: '%s', '%s'", flagProtocolTCP, flagProtocolUDP,
				),
				Value:   flagProtocolTCP,
				EnvVars: []string{strcase.ToSNAKE(flagProtocol)},
			},
			&cli.StringFlag{
				Name:     flagAddr,
				Usage:    "The address of the connqc server",
				Required: true,
				EnvVars:  []string{strcase.ToSNAKE(flagAddr)},
			},
			&cli.DurationFlag{
				Name:    flagConnBackoff,
				Usage:   "The duration to wait for before retrying to connect to the server",
				Value:   time.Second,
				EnvVars: []string{strcase.ToSNAKE(flagConnBackoff)},
			},
			&cli.DurationFlag{
				Name:    flagSendInterval,
				Usage:   "The interval at which to send probe messages to the server",
				Value:   time.Second,
				EnvVars: []string{strcase.ToSNAKE(flagSendInterval)},
			},
			&cli.DurationFlag{
				Name:    flagReadTimeout,
				Usage:   "The duration after which the client should timeout when reading from a connection",
				Value:   2 * time.Second,
				EnvVars: []string{strcase.ToSNAKE(flagReadTimeout)},
			},
			&cli.DurationFlag{
				Name:    flagWriteTimeout,
				Usage:   "The duration after which the client should timeout when writing to a connection",
				Value:   5 * time.Second,
				EnvVars: []string{strcase.ToSNAKE(flagWriteTimeout)},
			},
			&cli.IntFlag{
				Name:    flagPayloadSize,
				Usage:   "Fixed probe payload size in bytes (0 = default text payload)",
				Value:   0,
				EnvVars: []string{strcase.ToSNAKE(flagPayloadSize)},
			},
			&cli.IntSliceFlag{
				Name:    flagPayloadSizes,
				Usage:   "Payload sizes to rotate through per probe (MTU/fragmentation sweep); overrides --payload-size",
				EnvVars: []string{strcase.ToSNAKE(flagPayloadSizes)},
			},
			&cli.IntFlag{
				Name:    flagBurst,
				Usage:   "Number of probes to send per interval",
				Value:   1,
				EnvVars: []string{strcase.ToSNAKE(flagBurst)},
			},
			&cli.StringFlag{
				Name:    flagMetricsAddr,
				Usage:   "Address to serve Prometheus /metrics on (empty = disabled)",
				EnvVars: []string{strcase.ToSNAKE(flagMetricsAddr)},
			},
			&cli.DurationFlag{
				Name:    flagReportInterval,
				Usage:   "Interval for periodic stats summaries (0 = end-of-run only)",
				Value:   0,
				EnvVars: []string{strcase.ToSNAKE(flagReportInterval)},
			},
		}.Merge(cmd.LogFlags),
		Action: runClient,
	},
	{
		Name:  "server",
		Usage: "Run the connqc server",
		Flags: cmd.Flags{
			&cli.StringFlag{
				Name:    flagAddr,
				Usage:   "The address to listen on for probe messages",
				Value:   ":8123",
				EnvVars: []string{strcase.ToSNAKE(flagAddr)},
			},
			&cli.DurationFlag{
				Name:    flagReadTimeout,
				Usage:   "The duration after which the server should timeout when reading from a connection",
				Value:   2 * time.Second,
				EnvVars: []string{strcase.ToSNAKE(flagReadTimeout)},
			},
			&cli.DurationFlag{
				Name:    flagWriteTimeout,
				Usage:   "The duration after which the server should timeout when writing to a connection",
				Value:   5 * time.Second,
				EnvVars: []string{strcase.ToSNAKE(flagWriteTimeout)},
			},
		}.Merge(cmd.LogFlags),
		Action: runServer,
	},
	{
		Name:  "manager",
		Usage: "Run the artemis manager (control plane)",
		Flags: cmd.Flags{
			&cli.StringFlag{
				Name:    flagHTTPAddr,
				Usage:   "Address to serve the HTTP API + UI on",
				Value:   ":8080",
				EnvVars: []string{strcase.ToSNAKE(flagHTTPAddr)},
			},
			&cli.StringFlag{
				Name:    flagGRPCAddr,
				Usage:   "Address to serve the gRPC AgentLink on",
				Value:   ":8443",
				EnvVars: []string{strcase.ToSNAKE(flagGRPCAddr)},
			},
			&cli.StringFlag{
				Name:    flagAgentToken,
				Usage:   "Shared bearer token required from agents (empty = open)",
				EnvVars: []string{strcase.ToSNAKE(flagAgentToken)},
			},
			&cli.StringFlag{
				Name:    flagPostgresDSN,
				Usage:   "PostgreSQL DSN for events/alerts (empty = in-memory only)",
				EnvVars: []string{strcase.ToSNAKE(flagPostgresDSN)},
			},
			&cli.StringFlag{
				Name:    flagClickHouseDSN,
				Usage:   "ClickHouse DSN for samples (empty = in-memory only)",
				EnvVars: []string{strcase.ToSNAKE(flagClickHouseDSN)},
			},
			&cli.StringFlag{
				Name:    flagAdminUser,
				Usage:   "Admin username for the UI (empty = UI open, no auth)",
				EnvVars: []string{strcase.ToSNAKE(flagAdminUser)},
			},
			&cli.StringFlag{
				Name:    flagAdminPassHash,
				Usage:   "Bcrypt hash of the admin password (generate with 'connqc hashpw')",
				EnvVars: []string{strcase.ToSNAKE(flagAdminPassHash)},
			},
			&cli.BoolFlag{
				Name:    flagDev,
				Usage:   "Enable dev mode (debug + fault-injection endpoints)",
				EnvVars: []string{strcase.ToSNAKE(flagDev)},
			},
		}.Merge(cmd.LogFlags),
		Action: runManager,
	},
	{
		Name:      "hashpw",
		Usage:     "Generate a bcrypt hash of a password for admin-pass-hash",
		ArgsUsage: "<password>",
		Action:    runHashPw,
	},
	{
		Name:  "agent",
		Usage: "Run an artemis agent (echo listener + prober + manager client)",
		Flags: cmd.Flags{
			&cli.StringFlag{
				Name:    flagManagerAddr,
				Usage:   "Address of the manager's gRPC AgentLink",
				Value:   "localhost:8443",
				EnvVars: []string{strcase.ToSNAKE(flagManagerAddr)},
			},
			&cli.StringFlag{
				Name:    flagAgentName,
				Usage:   "This agent's name (defaults to its listen address)",
				EnvVars: []string{strcase.ToSNAKE(flagAgentName)},
			},
			&cli.StringFlag{
				Name:    flagAddr,
				Usage:   "Address for this agent's echo listener",
				Value:   ":8123",
				EnvVars: []string{strcase.ToSNAKE(flagAddr)},
			},
			&cli.StringFlag{
				Name:    flagAgentToken,
				Usage:   "Shared bearer token presented to the manager",
				EnvVars: []string{strcase.ToSNAKE(flagAgentToken)},
			},
			&cli.StringFlag{
				Name:    flagGroup,
				Usage:   "Group this agent belongs to (for scoped mesh)",
				EnvVars: []string{strcase.ToSNAKE(flagGroup)},
			},
			&cli.StringSliceFlag{
				Name:    flagTags,
				Usage:   "Tags for this agent (for scoped mesh / filtering)",
				EnvVars: []string{strcase.ToSNAKE(flagTags)},
			},
			&cli.StringSliceFlag{
				Name:    flagTargets,
				Usage:   "Static peer addresses to probe (host:port); manager assignments override later",
				EnvVars: []string{strcase.ToSNAKE(flagTargets)},
			},
			&cli.StringFlag{
				Name:    flagProtocol,
				Usage:   "Protocol for static targets: 'tcp' or 'udp'",
				Value:   flagProtocolTCP,
				EnvVars: []string{strcase.ToSNAKE(flagProtocol)},
			},
			&cli.DurationFlag{
				Name:    flagFlushInterval,
				Usage:   "Interval at which the agent streams telemetry to the manager",
				Value:   time.Second,
				EnvVars: []string{strcase.ToSNAKE(flagFlushInterval)},
			},
			&cli.DurationFlag{
				Name:    flagConnBackoff,
				Usage:   "Reconnect/backoff duration",
				Value:   time.Second,
				EnvVars: []string{strcase.ToSNAKE(flagConnBackoff)},
			},
			&cli.DurationFlag{
				Name:    flagSendInterval,
				Usage:   "Probe send interval",
				Value:   time.Second,
				EnvVars: []string{strcase.ToSNAKE(flagSendInterval)},
			},
			&cli.DurationFlag{
				Name:    flagReadTimeout,
				Usage:   "Read timeout",
				Value:   2 * time.Second,
				EnvVars: []string{strcase.ToSNAKE(flagReadTimeout)},
			},
			&cli.DurationFlag{
				Name:    flagWriteTimeout,
				Usage:   "Write timeout",
				Value:   5 * time.Second,
				EnvVars: []string{strcase.ToSNAKE(flagWriteTimeout)},
			},
			&cli.IntFlag{
				Name:    flagPayloadSize,
				Usage:   "Probe payload size in bytes (0 = default text payload)",
				Value:   0,
				EnvVars: []string{strcase.ToSNAKE(flagPayloadSize)},
			},
			&cli.BoolFlag{
				Name:    flagDev,
				Usage:   "Enable dev mode (verbose fault-injection logging)",
				EnvVars: []string{strcase.ToSNAKE(flagDev)},
			},
		}.Merge(cmd.LogFlags),
		Action: runAgent,
	},
}

func main() {
	os.Exit(realMain())
}

func realMain() (code int) {
	ui := newTerm()

	defer func() {
		if v := recover(); v != nil {
			ui.Error(fmt.Sprintf("Panic: %v\n%s", v, string(debug.Stack())))
			code = 1
			return
		}
	}()

	app := cli.NewApp()
	app.Name = "connqc"
	app.Description = "Connection quality checker"
	app.Version = version
	app.Commands = commands

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := app.RunContext(ctx, os.Args); err != nil {
		ui.Error(err.Error())
		return 1
	}
	return 0
}
