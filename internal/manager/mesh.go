package manager

import (
	"net"
	"sync"

	pb "github.com/slothnetworks/artemis/internal/pb/artemis/v1"
)

// Mesh modes.
const (
	// MeshFull / MeshAuto: every agent probes every other agent.
	MeshFull = "full"
	// MeshIntraGroup: agents probe only peers in the same group.
	MeshIntraGroup = "intra-group"
	// MeshInterGroup: agents probe only peers in other groups.
	MeshInterGroup = "inter-group"
	// MeshOff: no automatic assignments.
	MeshOff = "off"
)

// MeshParams are the per-edge test parameters applied to every assignment.
type MeshParams struct {
	Protocol    string `json:"protocol"`
	IntervalMS  int64  `json:"interval_ms"`
	PayloadSize int    `json:"payload_size"`
	Burst       int    `json:"burst"`
}

// Mesh holds the current meshing mode and default test parameters.
type Mesh struct {
	mu     sync.RWMutex
	mode   string
	params MeshParams
}

// NewMesh returns a Mesh with sensible defaults (full mesh, 1s TCP probes).
func NewMesh() *Mesh {
	return &Mesh{
		mode:   MeshFull,
		params: MeshParams{Protocol: "tcp", IntervalMS: 1000, PayloadSize: 0, Burst: 1},
	}
}

// Get returns the current mode and params.
func (m *Mesh) Get() (string, MeshParams) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.mode, m.params
}

// Set updates the mode and params.
func (m *Mesh) Set(mode string, params MeshParams) {
	m.mu.Lock()
	m.mode = mode
	m.params = params
	m.mu.Unlock()
}

// meshEdge reports whether agent a should probe peer b under the given mode.
func meshEdge(mode string, a, b AgentInfo) bool {
	if a.ID == b.ID {
		return false
	}
	switch mode {
	case MeshFull:
		return true
	case MeshIntraGroup:
		return a.Group == b.Group
	case MeshInterGroup:
		return a.Group != b.Group
	default:
		return false
	}
}

// dialAddr returns an address peers can use to probe the given agent, filling in
// the host from the agent's reported IPv4 when the listen address is host-less.
func dialAddr(a AgentInfo) string {
	host, port, err := net.SplitHostPort(a.ListenAddr)
	if err != nil {
		return a.ListenAddr
	}
	if host == "" && len(a.IPv4) > 0 {
		host = a.IPv4[0]
	}
	return net.JoinHostPort(host, port)
}

// RecomputeMesh recomputes every agent's target assignment from the current
// registry + mesh mode and pushes it down each agent's stream.
func (m *Manager) RecomputeMesh() {
	mode, params := m.mesh.Get()
	agents := m.reg.List()

	for _, a := range agents {
		ag, ok := m.reg.Get(a.ID)
		if !ok {
			continue
		}
		var targets []*pb.Target
		for _, b := range agents {
			if !meshEdge(mode, a, b) {
				continue
			}
			targets = append(targets, &pb.Target{
				Name:        b.Name,
				Addr:        dialAddr(b),
				Protocol:    params.Protocol,
				IntervalMs:  params.IntervalMS,
				PayloadSize: int32(params.PayloadSize),
				Burst:       int32(params.Burst),
			})
		}
		ag.Send(&pb.ManagerMessage{Msg: &pb.ManagerMessage_Assignment{
			Assignment: &pb.Assignment{Targets: targets},
		}})
	}
}
