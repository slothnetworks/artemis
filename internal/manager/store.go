package manager

import (
	"sync"
	"time"

	"github.com/slothnetworks/artemis/internal/stats"
)

// edgeKey is the map key for a directed edge.
func edgeKey(src, dst string) string { return src + "\x00" + dst }

// TimedSnapshot is a snapshot tagged with the time it was ingested.
type TimedSnapshot struct {
	At   time.Time      `json:"at"`
	Snap stats.Snapshot `json:"snap"`
}

// EdgeView is the latest state of a directed edge.
type EdgeView struct {
	Src       string         `json:"src"`
	Dst       string         `json:"dst"`
	Snapshot  stats.Snapshot `json:"snapshot"`
	UpdatedAt time.Time      `json:"updated_at"`
}

// TopologyView is the full mesh view: agents (nodes) and their edges.
type TopologyView struct {
	Nodes []AgentInfo `json:"nodes"`
	Edges []EdgeView  `json:"edges"`
}

type edgeState struct {
	src, dst  string
	latest    stats.Snapshot
	updatedAt time.Time
	hist      []TimedSnapshot // bounded ring of recent snapshots
	histNext  int
}

// Store holds the in-memory, per-edge rolling view that feeds the live UI. It is
// the hot path; durable persistence is layered on in a later phase.
type Store struct {
	histCap int

	mu    sync.RWMutex
	edges map[string]*edgeState
}

// NewStore returns a Store retaining up to histCap recent snapshots per edge.
func NewStore(histCap int) *Store {
	if histCap <= 0 {
		histCap = 600
	}
	return &Store{histCap: histCap, edges: make(map[string]*edgeState)}
}

// Ingest records the latest snapshot for the src→dst edge.
func (s *Store) Ingest(src, dst string, snap stats.Snapshot, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := edgeKey(src, dst)
	e := s.edges[k]
	if e == nil {
		e = &edgeState{src: src, dst: dst, hist: make([]TimedSnapshot, 0, s.histCap)}
		s.edges[k] = e
	}
	e.latest = snap
	e.updatedAt = at

	ts := TimedSnapshot{At: at, Snap: snap}
	if len(e.hist) < s.histCap {
		e.hist = append(e.hist, ts)
		return
	}
	e.hist[e.histNext] = ts
	e.histNext = (e.histNext + 1) % s.histCap
}

// Edges returns the latest view of every edge.
func (s *Store) Edges() []EdgeView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]EdgeView, 0, len(s.edges))
	for _, e := range s.edges {
		out = append(out, EdgeView{Src: e.src, Dst: e.dst, Snapshot: e.latest, UpdatedAt: e.updatedAt})
	}
	return out
}

// Series returns the recent snapshot history for an edge in chronological order.
func (s *Store) Series(src, dst string) []TimedSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e := s.edges[edgeKey(src, dst)]
	if e == nil {
		return nil
	}
	out := make([]TimedSnapshot, 0, len(e.hist))
	if len(e.hist) < s.histCap {
		out = append(out, e.hist...)
		return out
	}
	out = append(out, e.hist[e.histNext:]...)
	out = append(out, e.hist[:e.histNext]...)
	return out
}
