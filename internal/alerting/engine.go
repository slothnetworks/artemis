// Package alerting evaluates connection-quality rules over the live mesh and
// manages stateful, grouped, silence-aware alerts.
package alerting

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/slothnetworks/artemis/internal/stats"
	"github.com/slothnetworks/artemis/internal/store"
)

// Config holds alert rule thresholds. Zero values fall back to sane defaults.
type Config struct {
	LossPct       float64       // open packet_loss at/above this loss percentage
	HighLatency   time.Duration // open high_latency when p95 >= this
	HighJitter    time.Duration // open high_jitter when jitter >= this
	BlackoutGrace time.Duration // edge with no update for this long → blackout
	MismatchQuiet time.Duration // resolve size_mismatch after this quiet period
}

func (c Config) withDefaults() Config {
	if c.LossPct == 0 {
		c.LossPct = 20
	}
	if c.HighLatency == 0 {
		c.HighLatency = 250 * time.Millisecond
	}
	if c.HighJitter == 0 {
		c.HighJitter = 100 * time.Millisecond
	}
	if c.BlackoutGrace == 0 {
		c.BlackoutGrace = 15 * time.Second
	}
	if c.MismatchQuiet == 0 {
		c.MismatchQuiet = 60 * time.Second
	}
	return c
}

// EdgeInput is the engine's view of one edge at evaluation time.
type EdgeInput struct {
	Src       string
	Dst       string
	Snap      stats.Snapshot
	UpdatedAt time.Time
}

// AgentInput is the engine's view of one agent at evaluation time.
type AgentInput struct {
	Name  string
	Stale bool
}

type condition struct {
	src, dst, rule, severity, reason string
}

// Engine evaluates rules and tracks active alerts.
type Engine struct {
	cfg      Config
	sink     store.AlertSink
	notifier Notifier
	silences *Silences
	onChange func(store.Alert)

	mu           sync.Mutex
	active       map[string]*store.Alert
	lastMismatch map[string]time.Time // edgeKey → last size-mismatch time
}

// New returns an Engine. onChange (may be nil) is called on every alert
// transition (e.g. to broadcast over SSE).
func New(cfg Config, sink store.AlertSink, notifier Notifier, silences *Silences, onChange func(store.Alert)) *Engine {
	return &Engine{
		cfg:          cfg.withDefaults(),
		sink:         sink,
		notifier:     notifier,
		silences:     silences,
		onChange:     onChange,
		active:       make(map[string]*store.Alert),
		lastMismatch: make(map[string]time.Time),
	}
}

func alertKey(src, dst, rule string) string { return src + "|" + dst + "|" + rule }

// RecordSizeMismatch notes a size-mismatch observation on an edge; the next
// Evaluate opens/refreshes a size_mismatch alert.
func (e *Engine) RecordSizeMismatch(src, dst string) {
	e.mu.Lock()
	e.lastMismatch[src+"|"+dst] = time.Now()
	e.mu.Unlock()
}

// Active returns a snapshot of currently firing alerts.
func (e *Engine) Active() []store.Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]store.Alert, 0, len(e.active))
	for _, a := range e.active {
		out = append(out, *a)
	}
	return out
}

// Evaluate runs all rules against the current inputs and opens/resolves alerts.
func (e *Engine) Evaluate(edges []EdgeInput, agents []AgentInput, now time.Time) {
	desired := map[string]condition{}

	for _, ed := range edges {
		s := ed.Snap
		stale := !ed.UpdatedAt.IsZero() && now.Sub(ed.UpdatedAt) > e.cfg.BlackoutGrace
		switch {
		case stale || (s.Sent > 0 && s.Received == 0):
			desired[alertKey(ed.Src, ed.Dst, "blackout")] = condition{
				ed.Src, ed.Dst, "blackout", "critical", "no echoes received",
			}
		default:
			if s.LossPct >= e.cfg.LossPct {
				sev := "warning"
				if s.LossPct >= 80 {
					sev = "critical"
				}
				desired[alertKey(ed.Src, ed.Dst, "packet_loss")] = condition{
					ed.Src, ed.Dst, "packet_loss", sev, fmt.Sprintf("loss %.1f%%", s.LossPct),
				}
			}
			if e.cfg.HighLatency > 0 && s.P95 >= e.cfg.HighLatency {
				desired[alertKey(ed.Src, ed.Dst, "high_latency")] = condition{
					ed.Src, ed.Dst, "high_latency", "warning", fmt.Sprintf("p95 %s", s.P95),
				}
			}
			if e.cfg.HighJitter > 0 && s.Jitter >= e.cfg.HighJitter {
				desired[alertKey(ed.Src, ed.Dst, "high_jitter")] = condition{
					ed.Src, ed.Dst, "high_jitter", "warning", fmt.Sprintf("jitter %s", s.Jitter),
				}
			}
		}
	}

	for _, ag := range agents {
		if ag.Stale {
			desired[alertKey(ag.Name, "", "agent_down")] = condition{
				ag.Name, "", "agent_down", "critical", "agent stale / disconnected",
			}
		}
	}

	e.mu.Lock()
	for edgeKey, t := range e.lastMismatch {
		if now.Sub(t) <= e.cfg.MismatchQuiet {
			src, dst := splitEdge(edgeKey)
			desired[alertKey(src, dst, "size_mismatch")] = condition{
				src, dst, "size_mismatch", "critical", "payload modified in flight",
			}
		} else {
			delete(e.lastMismatch, edgeKey)
		}
	}

	// Open new / refresh existing.
	for key, c := range desired {
		if a, ok := e.active[key]; ok {
			a.LastEventAt = now
			a.EventCount++
			a.Reason = c.reason
			e.persist(*a)
			continue
		}
		e.openLocked(key, c, now)
	}
	// Resolve cleared.
	for key, a := range e.active {
		if _, ok := desired[key]; !ok {
			e.resolveLocked(key, a, now)
		}
	}
	e.mu.Unlock()
}

func (e *Engine) openLocked(key string, c condition, now time.Time) {
	a := &store.Alert{
		ID: key, Src: c.src, Dst: c.dst, Rule: c.rule, Severity: c.severity,
		State: "firing", Reason: c.reason, StartedAt: now, LastEventAt: now, EventCount: 1,
	}
	a.Silenced = e.silences != nil && e.silences.Matches(*a)
	e.active[key] = a
	e.persist(*a)
	if !a.Silenced && e.notifier != nil {
		e.notifier.Notify(*a, "firing")
	}
	if e.onChange != nil {
		e.onChange(*a)
	}
}

func (e *Engine) resolveLocked(key string, a *store.Alert, now time.Time) {
	a.State = "resolved"
	a.ResolvedAt = now
	e.persist(*a)
	if !a.Silenced && e.notifier != nil {
		e.notifier.Notify(*a, "resolved")
	}
	if e.onChange != nil {
		e.onChange(*a)
	}
	delete(e.active, key)
}

func (e *Engine) persist(a store.Alert) {
	if e.sink == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = e.sink.UpsertAlert(ctx, a)
}

func splitEdge(k string) (src, dst string) {
	for i := 0; i < len(k); i++ {
		if k[i] == '|' {
			return k[:i], k[i+1:]
		}
	}
	return k, ""
}
