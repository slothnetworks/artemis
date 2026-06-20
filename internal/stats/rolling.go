package stats

import (
	"math"
	"sort"
	"sync"
	"time"
)

// DefaultReservoir is the default number of recent latency samples retained per
// target for percentile estimation.
const DefaultReservoir = 4096

// RollingCollector aggregates events per target. Latency percentiles are
// estimated from a bounded reservoir of recent samples, so per-event cost is
// O(1) and sorting happens only at snapshot time. Safe for concurrent use.
type RollingCollector struct {
	reservoir int

	mu   sync.Mutex
	aggs map[string]*aggregate
}

type aggregate struct {
	sent, received, dropped, timeout uint64
	bytes                            uint64

	lat     []time.Duration // bounded ring of recent latencies
	latNext int             // next write index once full

	min, max time.Duration
	sum      time.Duration
	count    uint64

	jitter  float64 // RFC 3550 interarrival jitter, in nanoseconds
	hasPrev bool
	prevLat time.Duration

	pathMTU, effMSS int

	startedAt time.Time
}

// NewRollingCollector returns a RollingCollector retaining up to reservoir
// recent latency samples per target. A reservoir <= 0 uses DefaultReservoir.
func NewRollingCollector(reservoir int) *RollingCollector {
	if reservoir <= 0 {
		reservoir = DefaultReservoir
	}
	return &RollingCollector{reservoir: reservoir, aggs: make(map[string]*aggregate)}
}

// Record implements Collector.
func (c *RollingCollector) Record(e Event) {
	c.mu.Lock()
	defer c.mu.Unlock()

	a := c.aggs[e.Target]
	if a == nil {
		a = &aggregate{lat: make([]time.Duration, 0, c.reservoir), startedAt: e.At}
		c.aggs[e.Target] = a
	}
	if a.startedAt.IsZero() {
		a.startedAt = e.At
	}

	switch e.Kind {
	case Sent:
		a.sent++
		a.bytes += uint64(e.Bytes)
	case Received:
		a.received++
		a.addLatency(e.Latency, c.reservoir)
	case Dropped:
		a.dropped++
	case Timeout:
		a.timeout++
	case SizeMismatch:
		// The probe reached the far side (so not a loss) but was modified; count
		// it as received for loss accounting — the mismatch drives its own alert.
		a.received++
		if e.Latency > 0 {
			a.addLatency(e.Latency, c.reservoir)
		}
	}
}

func (a *aggregate) addLatency(d time.Duration, reservoir int) {
	if a.count == 0 || d < a.min {
		a.min = d
	}
	if d > a.max {
		a.max = d
	}
	a.sum += d
	a.count++

	if a.hasPrev {
		diff := d - a.prevLat
		if diff < 0 {
			diff = -diff
		}
		a.jitter += (float64(diff) - a.jitter) / 16
	}
	a.prevLat = d
	a.hasPrev = true

	if len(a.lat) < reservoir {
		a.lat = append(a.lat, d)
		return
	}
	a.lat[a.latNext] = d
	a.latNext = (a.latNext + 1) % reservoir
}

// SetPath records the observed path MTU and effective MSS for a target.
func (c *RollingCollector) SetPath(target string, pathMTU, effMSS int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	a := c.aggs[target]
	if a == nil {
		a = &aggregate{lat: make([]time.Duration, 0, c.reservoir), startedAt: time.Now()}
		c.aggs[target] = a
	}
	if pathMTU > 0 {
		a.pathMTU = pathMTU
	}
	if effMSS > 0 {
		a.effMSS = effMSS
	}
}

// Snapshot returns a snapshot per target, sorted by target, without resetting
// any state.
func (c *RollingCollector) Snapshot() []Snapshot {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]Snapshot, 0, len(c.aggs))
	for t, a := range c.aggs {
		out = append(out, a.snapshot(t, now))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Target < out[j].Target })
	return out
}

// SnapshotTarget returns a snapshot for a single target and whether it exists.
func (c *RollingCollector) SnapshotTarget(target string) (Snapshot, bool) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	a, ok := c.aggs[target]
	if !ok {
		return Snapshot{}, false
	}
	return a.snapshot(target, now), true
}

func (a *aggregate) snapshot(target string, now time.Time) Snapshot {
	s := Snapshot{
		Target:       target,
		Sent:         a.sent,
		Received:     a.received,
		Dropped:      a.dropped,
		Timeout:      a.timeout,
		Bytes:        a.bytes,
		Jitter:       time.Duration(a.jitter),
		PathMTU:      a.pathMTU,
		EffectiveMSS: a.effMSS,
		StartedAt:    a.startedAt,
		EndedAt:      now,
	}

	if a.sent > 0 {
		lost := int64(a.sent) - int64(a.received)
		if lost < 0 {
			lost = 0
		}
		s.LossPct = float64(lost) / float64(a.sent) * 100
	}

	if a.count > 0 {
		s.Min = a.min
		s.Max = a.max
		s.Avg = a.sum / time.Duration(a.count)

		sorted := make([]time.Duration, len(a.lat))
		copy(sorted, a.lat)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		s.P50 = percentile(sorted, 50)
		s.P95 = percentile(sorted, 95)
		s.P99 = percentile(sorted, 99)
	}

	if w := now.Sub(a.startedAt); w > 0 {
		s.Window = w
		s.Throughput = float64(a.bytes) / w.Seconds()
	}
	return s
}

// percentile returns the p-th percentile (0..100) of a pre-sorted slice using
// the nearest-rank method (rank = ceil(p/100 * n)). It returns 0 for an empty
// slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(n)))
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}
