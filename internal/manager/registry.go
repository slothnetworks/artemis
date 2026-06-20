package manager

import (
	"sync"
	"time"

	pb "github.com/slothnetworks/artemis/internal/pb/artemis/v1"
)

// AgentInfo is the manager's view of a connected agent.
type AgentInfo struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	RemoteAddr string    `json:"remote_addr"`
	ListenAddr string    `json:"listen_addr"`
	Group      string    `json:"group"`
	Protocols  []string  `json:"protocols"`
	Tags       []string  `json:"tags"`
	IPv4       []string  `json:"ipv4"`
	IPv6       []string  `json:"ipv6"`
	Status     string    `json:"status"`
	LastSeen   time.Time `json:"last_seen"`
}

// Agent is a registered agent plus the channel used to push manager messages
// down its AgentLink stream.
type Agent struct {
	Info AgentInfo
	send chan *pb.ManagerMessage
}

// Send queues a manager message for delivery to the agent. It drops the message
// if the agent's buffer is full, so a slow/stuck agent can't block the manager.
func (a *Agent) Send(msg *pb.ManagerMessage) bool {
	select {
	case a.send <- msg:
		return true
	default:
		return false
	}
}

// Registry tracks connected agents. Safe for concurrent use.
type Registry struct {
	mu     sync.RWMutex
	agents map[string]*Agent
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{agents: make(map[string]*Agent)}
}

// Add registers an agent with the given info and returns its handle.
func (r *Registry) Add(info AgentInfo) *Agent {
	a := &Agent{Info: info, send: make(chan *pb.ManagerMessage, 16)}
	r.mu.Lock()
	r.agents[info.ID] = a
	r.mu.Unlock()
	return a
}

// Remove deregisters an agent by id.
func (r *Registry) Remove(id string) {
	r.mu.Lock()
	delete(r.agents, id)
	r.mu.Unlock()
}

// Touch updates an agent's last-seen time.
func (r *Registry) Touch(id string) {
	r.mu.Lock()
	if a, ok := r.agents[id]; ok {
		a.Info.LastSeen = time.Now()
	}
	r.mu.Unlock()
}

// Get returns the agent with the given id.
func (r *Registry) Get(id string) (*Agent, bool) {
	r.mu.RLock()
	a, ok := r.agents[id]
	r.mu.RUnlock()
	return a, ok
}

// List returns a snapshot of all agents' info.
func (r *Registry) List() []AgentInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]AgentInfo, 0, len(r.agents))
	for _, a := range r.agents {
		out = append(out, a.Info)
	}
	return out
}

// ForEach calls fn for each registered agent.
func (r *Registry) ForEach(fn func(*Agent)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, a := range r.agents {
		fn(a)
	}
}
