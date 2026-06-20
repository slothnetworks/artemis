package stats_test

import (
	"testing"
	"time"

	"github.com/slothnetworks/artemis/internal/stats"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRollingCollector_CountsAndLoss(t *testing.T) {
	c := stats.NewRollingCollector(0)
	now := time.Now()

	for i := 0; i < 10; i++ {
		c.Record(stats.Event{Target: "a", Kind: stats.Sent, Bytes: 100, At: now})
	}
	// 7 received, 2 dropped, 1 timeout => 3 lost of 10 => 30% loss.
	for i := 0; i < 7; i++ {
		c.Record(stats.Event{Target: "a", Kind: stats.Received, Latency: time.Millisecond, Bytes: 100, At: now})
	}
	c.Record(stats.Event{Target: "a", Kind: stats.Dropped, At: now})
	c.Record(stats.Event{Target: "a", Kind: stats.Dropped, At: now})
	c.Record(stats.Event{Target: "a", Kind: stats.Timeout, At: now})

	snaps := c.Snapshot()
	require.Len(t, snaps, 1)
	s := snaps[0]

	assert.Equal(t, "a", s.Target)
	assert.Equal(t, uint64(10), s.Sent)
	assert.Equal(t, uint64(7), s.Received)
	assert.Equal(t, uint64(2), s.Dropped)
	assert.Equal(t, uint64(1), s.Timeout)
	assert.InDelta(t, 30.0, s.LossPct, 0.001)
	assert.Equal(t, uint64(1000), s.Bytes)
}

func TestRollingCollector_Percentiles(t *testing.T) {
	c := stats.NewRollingCollector(0)
	now := time.Now()

	// Latencies 1..100 ms.
	for i := 1; i <= 100; i++ {
		c.Record(stats.Event{
			Target:  "edge",
			Kind:    stats.Received,
			Latency: time.Duration(i) * time.Millisecond,
			At:      now,
		})
	}

	s, ok := c.SnapshotTarget("edge")
	require.True(t, ok)

	assert.Equal(t, 1*time.Millisecond, s.Min)
	assert.Equal(t, 100*time.Millisecond, s.Max)
	// Mean of 1..100 = 50.5ms.
	assert.InDelta(t, float64(50500*time.Microsecond), float64(s.Avg), float64(time.Microsecond))
	// Nearest-rank on 100 sorted samples.
	assert.Equal(t, 50*time.Millisecond, s.P50)
	assert.Equal(t, 95*time.Millisecond, s.P95)
	assert.Equal(t, 99*time.Millisecond, s.P99)
}

func TestRollingCollector_Jitter(t *testing.T) {
	c := stats.NewRollingCollector(0)
	now := time.Now()

	// Constant latency => jitter converges to 0.
	for i := 0; i < 50; i++ {
		c.Record(stats.Event{Target: "flat", Kind: stats.Received, Latency: 10 * time.Millisecond, At: now})
	}
	flat, _ := c.SnapshotTarget("flat")
	assert.Equal(t, time.Duration(0), flat.Jitter)

	// Alternating latency => non-zero jitter.
	for i := 0; i < 50; i++ {
		d := 10 * time.Millisecond
		if i%2 == 0 {
			d = 30 * time.Millisecond
		}
		c.Record(stats.Event{Target: "varied", Kind: stats.Received, Latency: d, At: now})
	}
	varied, _ := c.SnapshotTarget("varied")
	assert.Greater(t, varied.Jitter, time.Duration(0))
}

func TestRollingCollector_ReservoirBounded(t *testing.T) {
	c := stats.NewRollingCollector(8)
	now := time.Now()
	for i := 0; i < 1000; i++ {
		c.Record(stats.Event{Target: "t", Kind: stats.Received, Latency: time.Millisecond, At: now})
	}
	s, _ := c.SnapshotTarget("t")
	// Counts are exact even though the reservoir is small.
	assert.Equal(t, uint64(1000), s.Received)
}

func TestRollingCollector_Throughput(t *testing.T) {
	c := stats.NewRollingCollector(0)
	start := time.Now().Add(-2 * time.Second)
	c.Record(stats.Event{Target: "t", Kind: stats.Sent, Bytes: 2000, At: start})
	s, _ := c.SnapshotTarget("t")
	// ~2000 bytes over ~2s => ~1000 B/s (allow slack for wall-clock).
	assert.Greater(t, s.Throughput, 500.0)
}

func TestNopCollector(t *testing.T) {
	var c stats.Collector = stats.NopCollector{}
	assert.NotPanics(t, func() { c.Record(stats.Event{Kind: stats.Sent}) })
}

func TestMultiCollector_FansOut(t *testing.T) {
	a := stats.NewRollingCollector(0)
	b := stats.NewRollingCollector(0)
	m := stats.MultiCollector{a, b}

	m.Record(stats.Event{Target: "x", Kind: stats.Sent, At: time.Now()})

	sa, _ := a.SnapshotTarget("x")
	sb, _ := b.SnapshotTarget("x")
	assert.Equal(t, uint64(1), sa.Sent)
	assert.Equal(t, uint64(1), sb.Sent)
}
