// Package manager is the artemis control plane: it terminates agent AgentLink
// streams, keeps the live mesh view, and fans updates out to the web UI.
package manager

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/logger/v2"
	lctx "github.com/hamba/logger/v2/ctx"
	pb "github.com/slothnetworks/artemis/internal/pb/artemis/v1"
	"github.com/slothnetworks/artemis/internal/pbconv"
	"github.com/slothnetworks/artemis/internal/store"
)

func eventKindString(k pb.EventKind) string {
	switch k {
	case pb.EventKind_EVENT_KIND_DROPPED:
		return "packet_loss"
	case pb.EventKind_EVENT_KIND_TIMEOUT:
		return "timeout"
	case pb.EventKind_EVENT_KIND_SIZE_MISMATCH:
		return "size_mismatch"
	default:
		return "unspecified"
	}
}

func directionString(d pb.Direction) string {
	switch d {
	case pb.Direction_DIRECTION_FORWARD:
		return "forward"
	case pb.Direction_DIRECTION_RETURN:
		return "return"
	default:
		return "none"
	}
}

// Manager is the control plane. It implements the AgentLink gRPC service.
type Manager struct {
	pb.UnimplementedAgentLinkServer

	reg        *Registry
	store      *Store
	bcast      *Broadcast
	mesh       *Mesh
	sampleSink store.SampleSink
	eventSink  store.EventSink
	log        *logger.Logger
}

// Option configures a Manager.
type Option func(*Manager)

// WithSampleSink sets the durable time-series sink for samples.
func WithSampleSink(s store.SampleSink) Option {
	return func(m *Manager) { m.sampleSink = s }
}

// WithEventSink sets the durable relational sink for events.
func WithEventSink(s store.EventSink) Option {
	return func(m *Manager) { m.eventSink = s }
}

// New returns a Manager. By default it persists nowhere (in-memory only); pass
// WithSampleSink / WithEventSink to enable durable storage.
func New(log *logger.Logger, opts ...Option) *Manager {
	m := &Manager{
		reg:        NewRegistry(),
		store:      NewStore(0),
		bcast:      NewBroadcast(),
		mesh:       NewMesh(),
		sampleSink: store.NopSampleSink{},
		eventSink:  store.NopEventSink{},
		log:        log,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Mesh returns the mesh configuration.
func (m *Manager) Mesh() *Mesh { return m.mesh }

// Registry returns the agent registry.
func (m *Manager) Registry() *Registry { return m.reg }

// Store returns the live edge store.
func (m *Manager) Store() *Store { return m.store }

// Subscribe registers an SSE subscriber.
func (m *Manager) Subscribe() chan []byte { return m.bcast.Subscribe() }

// Unsubscribe removes an SSE subscriber.
func (m *Manager) Unsubscribe(ch chan []byte) { m.bcast.Unsubscribe(ch) }

// Topology returns the current nodes (agents) and edges (latest stats).
func (m *Manager) Topology() TopologyView {
	return TopologyView{Nodes: m.reg.List(), Edges: m.store.Edges()}
}

type edgeEvent struct {
	Type     string `json:"type"`
	Src      string `json:"src"`
	Dst      string `json:"dst"`
	Snapshot any    `json:"snapshot"`
	At       int64  `json:"at"`
}

// Link implements the AgentLink bidirectional stream.
func (m *Manager) Link(stream pb.AgentLink_LinkServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if err != nil {
		return err
	}
	enroll := first.GetEnroll()
	if enroll == nil {
		return errors.New("first message must be Enroll")
	}

	id := uuid.NewString()
	p, _ := peerAddr(ctx)
	info := AgentInfo{
		ID:         id,
		Name:       enroll.GetName(),
		RemoteAddr: p,
		ListenAddr: enroll.GetListenAddr(),
		Group:      enroll.GetGroup(),
		Protocols:  enroll.GetProtocols(),
		Tags:       enroll.GetTags(),
		IPv4:       enroll.GetIpv4(),
		IPv6:       enroll.GetIpv6(),
		Status:     "online",
		LastSeen:   time.Now(),
	}
	agent := m.reg.Add(info)
	log := m.log.With(lctx.Str("agent_id", id), lctx.Str("agent", info.Name))
	log.Info("Agent enrolled", lctx.Str("group", info.Group), lctx.Str("listen_addr", info.ListenAddr))

	defer func() {
		m.reg.Remove(id)
		m.RecomputeMesh()
		log.Info("Agent disconnected")
	}()

	if err := stream.Send(&pb.ManagerMessage{
		Msg: &pb.ManagerMessage_EnrollAck{EnrollAck: &pb.EnrollAck{AgentId: id}},
	}); err != nil {
		return err
	}

	// Sender goroutine: drain queued manager messages to the stream.
	sendErr := make(chan error, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-agent.send:
				if err := stream.Send(msg); err != nil {
					sendErr <- err
					return
				}
			}
		}
	}()

	// Now that this agent can receive, recompute and push mesh assignments to
	// the whole fleet (the new agent and its new peers).
	m.RecomputeMesh()

	src := info.Name
	for {
		select {
		case err := <-sendErr:
			return err
		default:
		}

		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		m.reg.Touch(id)
		m.handleAgentMessage(src, msg)
	}
}

func (m *Manager) handleAgentMessage(src string, msg *pb.AgentMessage) {
	switch v := msg.Msg.(type) {
	case *pb.AgentMessage_Sample:
		s := v.Sample
		dst := s.GetEdge().GetDst()
		snap := pbconv.SnapshotFromPB(s.GetSnapshot())
		at := s.GetTs().AsTime()
		if at.IsZero() {
			at = time.Now()
		}
		m.store.Ingest(src, dst, snap, at)
		m.sampleSink.WriteSample(store.SampleRow{Ts: at, Src: src, Dst: dst, Snap: snap})
		m.publishEdge(src, dst, snap, at)
	case *pb.AgentMessage_Event:
		e := v.Event
		m.log.Info("Fault event",
			lctx.Str("src", src),
			lctx.Str("dst", e.GetEdge().GetDst()),
			lctx.Str("kind", e.GetKind().String()),
		)
		if err := m.eventSink.WriteEvent(context.Background(), store.Event{
			Ts:         e.GetTs().AsTime(),
			Src:        src,
			Dst:        e.GetEdge().GetDst(),
			Kind:       eventKindString(e.GetKind()),
			ProbeID:    e.GetProbeId(),
			SentBytes:  int(e.GetSentBytes()),
			RecvBytes:  int(e.GetRecvBytes()),
			ChecksumOK: e.GetChecksumOk(),
			Direction:  directionString(e.GetDirection()),
		}); err != nil {
			m.log.Error("Persist event", lctx.Err(err))
		}
	case *pb.AgentMessage_MtrSnapshot:
		// Persisted/attached in a later phase.
		m.log.Info("MTR snapshot", lctx.Str("src", src), lctx.Str("dst", v.MtrSnapshot.GetEdge().GetDst()))
	}
}

func (m *Manager) publishEdge(src, dst string, snap any, at time.Time) {
	data, err := json.Marshal(edgeEvent{Type: "edge", Src: src, Dst: dst, Snapshot: snap, At: at.UnixMilli()})
	if err != nil {
		return
	}
	m.bcast.Publish(data)
}

// markStale is invoked periodically to flag agents that have gone quiet.
func (m *Manager) markStale(timeout time.Duration) {
	now := time.Now()
	m.reg.mu.Lock()
	for _, a := range m.reg.agents {
		if now.Sub(a.Info.LastSeen) > timeout {
			a.Info.Status = "stale"
		} else {
			a.Info.Status = "online"
		}
	}
	m.reg.mu.Unlock()
}

// RunMaintenance periodically marks stale agents until ctx is done.
func (m *Manager) RunMaintenance(ctx context.Context, staleAfter time.Duration) {
	t := time.NewTicker(staleAfter / 2)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.markStale(staleAfter)
		}
	}
}
