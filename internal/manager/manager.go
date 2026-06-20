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
)

// Manager is the control plane. It implements the AgentLink gRPC service.
type Manager struct {
	pb.UnimplementedAgentLinkServer

	reg   *Registry
	store *Store
	bcast *Broadcast
	log   *logger.Logger
}

// New returns a Manager.
func New(log *logger.Logger) *Manager {
	return &Manager{
		reg:   NewRegistry(),
		store: NewStore(0),
		bcast: NewBroadcast(),
		log:   log,
	}
}

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
		m.publishEdge(src, dst, snap, at)
	case *pb.AgentMessage_Event:
		// Fault events drive alerting in a later phase; surface for now.
		e := v.Event
		m.log.Info("Fault event",
			lctx.Str("src", src),
			lctx.Str("dst", e.GetEdge().GetDst()),
			lctx.Str("kind", e.GetKind().String()),
		)
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
