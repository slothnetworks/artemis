// Package agent runs an echo listener (so peers can probe it), a set of probers
// toward assigned peers, and the AgentLink client that enrolls with the manager
// and streams telemetry up.
package agent

import (
	"context"
	"errors"
	"net"
	"os/exec"
	"sync"
	"time"

	"github.com/hamba/logger/v2"
	lctx "github.com/hamba/logger/v2/ctx"
	"github.com/slothnetworks/artemis/internal/manager"
	"github.com/slothnetworks/artemis/internal/mtr"
	pb "github.com/slothnetworks/artemis/internal/pb/artemis/v1"
	"github.com/slothnetworks/artemis/internal/pbconv"
	"github.com/slothnetworks/artemis/internal/probe"
	"github.com/slothnetworks/artemis/internal/probe/tcp"
	"github.com/slothnetworks/artemis/internal/probe/udp"
	"github.com/slothnetworks/artemis/internal/stats"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Target is one peer this agent should probe.
type Target struct {
	Addr     string
	Protocol string
}

// Config configures an agent.
type Config struct {
	Name        string
	ManagerAddr string
	Token       string
	ListenAddr  string
	Protocols   []string
	Tags        []string
	Group       string

	Targets       []Target
	FlushInterval time.Duration

	Backoff      time.Duration
	Interval     time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PayloadSize  int
}

// Agent ties the echo server, probers and manager link together.
type Agent struct {
	cfg  Config
	coll *stats.RollingCollector
	log  *logger.Logger

	runCtx      context.Context
	echo        *probe.Server
	mu          sync.Mutex
	probers     map[string]context.CancelFunc // keyed by target addr
	targetNames map[string]string             // target addr → peer name
	sendMu      sync.Mutex                    // serializes stream.Send across goroutines
}

// New returns an Agent.
func New(cfg Config, log *logger.Logger) *Agent {
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}
	return &Agent{
		cfg:         cfg,
		coll:        stats.NewRollingCollector(0),
		log:         log,
		probers:     make(map[string]context.CancelFunc),
		targetNames: make(map[string]string),
	}
}

// Run starts the echo server and probers, then maintains the manager link until
// ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	a.runCtx = ctx
	go a.serveEcho(ctx)

	// Seed probers from any static targets; manager assignments replace these.
	a.applyAssignment(staticTargets(a.cfg))

	for {
		if err := a.link(ctx); err != nil {
			a.log.Error("Manager link error", lctx.Err(err))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(a.cfg.Backoff):
		}
	}
}

// applyAssignment reconciles the running prober set against the desired targets:
// it starts probers for new targets and stops those no longer assigned. It is
// idempotent.
func (a *Agent) applyAssignment(targets []*pb.Target) {
	a.log.Debug("Applying assignment", lctx.Int("targets", len(targets)))
	desired := make(map[string]*pb.Target, len(targets))
	for _, t := range targets {
		if t.GetAddr() != "" {
			desired[t.GetAddr()] = t
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Remember peer names so telemetry edges are labeled name→name.
	for addr, t := range desired {
		if t.GetName() != "" {
			a.targetNames[addr] = t.GetName()
		}
	}

	// Stop probers no longer desired.
	for addr, cancel := range a.probers {
		if _, ok := desired[addr]; !ok {
			cancel()
			delete(a.probers, addr)
		}
	}
	// Start probers newly desired.
	for addr, t := range desired {
		if _, ok := a.probers[addr]; ok {
			continue
		}
		a.probers[addr] = a.startProber(t)
	}
}

func (a *Agent) startProber(t *pb.Target) context.CancelFunc {
	proto := t.GetProtocol()
	if proto == "" {
		proto = "tcp"
	}
	interval := a.cfg.Interval
	if t.GetIntervalMs() > 0 {
		interval = time.Duration(t.GetIntervalMs()) * time.Millisecond
	}
	payloadSize := a.cfg.PayloadSize
	if t.GetPayloadSize() > 0 {
		payloadSize = int(t.GetPayloadSize())
	}

	ctx, cancel := context.WithCancel(a.runCtx)
	client := probe.NewClient(
		a.cfg.Backoff, interval, a.cfg.ReadTimeout, a.cfg.WriteTimeout, a.log,
		probe.WithCollector(a.coll),
		probe.WithPayloadSize(payloadSize),
		probe.WithBurst(int(t.GetBurst())),
	)
	go client.Run(ctx, proto, t.GetAddr())
	return cancel
}

// injectFault makes this agent's echo listener drop lossPercent% of incoming
// probes for the given duration, so peers probing it observe packet loss.
func (a *Agent) injectFault(lossPercent float64, dur time.Duration) {
	if a.echo == nil {
		return
	}
	if dur <= 0 {
		dur = 30 * time.Second
	}
	a.log.Warn("Injecting fault", lctx.Interface("loss_percent", lossPercent), lctx.Duration("duration", dur))
	a.echo.SetDropRate(lossPercent / 100)
	time.AfterFunc(dur, func() {
		a.echo.SetDropRate(0)
		a.log.Info("Fault injection cleared")
	})
}

// staticTargets converts the agent's configured static targets to pb.Targets.
func staticTargets(cfg Config) []*pb.Target {
	out := make([]*pb.Target, 0, len(cfg.Targets))
	for _, t := range cfg.Targets {
		out = append(out, &pb.Target{Addr: t.Addr, Protocol: t.Protocol})
	}
	return out
}

func (a *Agent) serveEcho(ctx context.Context) {
	const bufferSize = 65535
	srv := probe.NewServer(bufferSize, a.cfg.ReadTimeout, a.cfg.WriteTimeout, a.log)
	a.echo = srv
	tcpSrv, err := tcp.NewServer(srv)
	if err != nil {
		a.log.Error("Echo TCP server init", lctx.Err(err))
		return
	}
	udpSrv, err := udp.NewServer(srv)
	if err != nil {
		a.log.Error("Echo UDP server init", lctx.Err(err))
		return
	}
	go func() {
		if err := tcpSrv.Listen(ctx, a.cfg.ListenAddr); err != nil && !errors.Is(err, net.ErrClosed) {
			a.log.Error("Echo TCP listen", lctx.Err(err))
		}
	}()
	go func() {
		if err := udpSrv.Listen(ctx, a.cfg.ListenAddr); err != nil && !errors.Is(err, net.ErrClosed) {
			a.log.Error("Echo UDP listen", lctx.Err(err))
		}
	}()
	<-ctx.Done()
}

func (a *Agent) link(ctx context.Context) error {
	conn, err := grpc.NewClient(a.cfg.ManagerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(manager.TokenCredentials(a.cfg.Token)),
	)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	stream, err := pb.NewAgentLinkClient(conn).Link(ctx)
	if err != nil {
		return err
	}

	ipv4, ipv6 := localIPs()
	if err := stream.Send(&pb.AgentMessage{Msg: &pb.AgentMessage_Enroll{Enroll: &pb.Enroll{
		Name:       a.cfg.Name,
		ListenAddr: a.cfg.ListenAddr,
		Protocols:  a.cfg.Protocols,
		Tags:       a.cfg.Tags,
		Group:      a.cfg.Group,
		Ipv4:       ipv4,
		Ipv6:       ipv6,
	}}}); err != nil {
		return err
	}

	ack, err := stream.Recv()
	if err != nil {
		return err
	}
	a.log.Info("Enrolled with manager", lctx.Str("agent_id", ack.GetEnrollAck().GetAgentId()))

	// Receive loop: handle manager messages (assignments/RunMtr land here in
	// later phases).
	go a.recvLoop(stream)

	// Flush telemetry until the stream or context ends.
	tick := time.NewTicker(a.cfg.FlushInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			if err := a.flush(stream); err != nil {
				return err
			}
		}
	}
}

func (a *Agent) recvLoop(stream pb.AgentLink_LinkClient) {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return
		}
		switch v := msg.Msg.(type) {
		case *pb.ManagerMessage_Assignment:
			a.applyAssignment(v.Assignment.GetTargets())
		case *pb.ManagerMessage_Fault:
			a.injectFault(v.Fault.GetLossPercent(), time.Duration(v.Fault.GetDurationMs())*time.Millisecond)
		case *pb.ManagerMessage_RunMtr:
			go a.runMTR(stream, v.RunMtr.GetEdge(), v.RunMtr.GetAlertId())
		}
	}
}

func (a *Agent) flush(stream pb.AgentLink_LinkClient) error {
	now := time.Now()
	for _, snap := range a.coll.Snapshot() {
		dst := snap.Target
		a.mu.Lock()
		if name, ok := a.targetNames[snap.Target]; ok {
			dst = name
		}
		a.mu.Unlock()
		if err := a.send(stream, &pb.AgentMessage{Msg: &pb.AgentMessage_Sample{Sample: &pb.Sample{
			Edge:     &pb.Edge{Src: a.cfg.Name, Dst: dst},
			Snapshot: pbconv.SnapshotToPB(snap),
			Ts:       timestamppb.New(now),
		}}}); err != nil {
			return err
		}
	}
	return nil
}

// send serializes Send calls so the flush loop and MTR goroutine don't race on
// the stream.
func (a *Agent) send(stream pb.AgentLink_LinkClient, msg *pb.AgentMessage) error {
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	return stream.Send(msg)
}

// addrForName reverse-looks-up a peer's probe address from its name.
func (a *Agent) addrForName(name string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	for addr, n := range a.targetNames {
		if n == name {
			return addr
		}
	}
	return ""
}

// runMTR runs a one-shot mtr/traceroute toward the edge's destination and streams
// the captured output back to the manager as evidence for the alert.
func (a *Agent) runMTR(stream pb.AgentLink_LinkClient, edge *pb.Edge, alertID string) {
	addr := a.addrForName(edge.GetDst())
	if addr == "" {
		addr = edge.GetDst()
	}
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	started := time.Now()
	out, hops, tool := a.mtrCapture(host)
	a.log.Info("MTR captured", lctx.Str("dst", edge.GetDst()), lctx.Str("tool", tool), lctx.Int("hops", len(hops)))
	_ = a.send(stream, &pb.AgentMessage{Msg: &pb.AgentMessage_MtrSnapshot{MtrSnapshot: &pb.MtrSnapshot{
		Edge:      edge,
		AlertId:   alertID,
		RawOutput: out,
		Hops:      hops,
		Tool:      tool,
		StartedAt: timestamppb.New(started),
	}}})
}

// mtrCapture runs the native Go MTR engine toward host, returning a rendered
// text report, the structured hops, and the tool name. On failure (e.g. missing
// CAP_NET_RAW) it falls back to the system traceroute.
func (a *Agent) mtrCapture(host string) (string, []*pb.MtrHop, string) {
	ctx, cancel := context.WithTimeout(a.runCtx, 25*time.Second)
	defer cancel()

	if res, err := mtr.Capture(ctx, host, 3); err == nil {
		hops := make([]*pb.MtrHop, 0, len(res.Hops))
		for _, h := range res.Hops {
			hops = append(hops, &pb.MtrHop{Hop: int32(h.Number), Host: h.IP, LossPct: h.LossPct, AvgMs: h.AvgMs})
		}
		return mtr.Render(res), hops, "native"
	} else {
		a.log.Debug("Native MTR failed, falling back to traceroute", lctx.Err(err))
	}

	if path, err := exec.LookPath("traceroute"); err == nil {
		out, _ := exec.CommandContext(ctx, path, "-n", "-w", "2", host).CombinedOutput()
		return string(out), nil, "traceroute"
	}
	return "native MTR failed and traceroute not available", nil, "none"
}

// localIPs returns the agent's own non-loopback, non-link-local IPv4 and IPv6
// addresses (agents run host networking, so these are routable).
func localIPs() (ipv4, ipv6 []string) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, nil
	}
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() || ipNet.IP.IsLinkLocalUnicast() {
			continue
		}
		if v4 := ipNet.IP.To4(); v4 != nil {
			ipv4 = append(ipv4, v4.String())
		} else {
			ipv6 = append(ipv6, ipNet.IP.String())
		}
	}
	return ipv4, ipv6
}
