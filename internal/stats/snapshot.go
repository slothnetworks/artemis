package stats

import "time"

// Snapshot is a point-in-time view of a single target's connection-quality
// statistics. It is the shape streamed agent→manager→browser.
type Snapshot struct {
	Target string `json:"target"`

	Sent     uint64  `json:"sent"`
	Received uint64  `json:"received"`
	Dropped  uint64  `json:"dropped"`
	Timeout  uint64  `json:"timeout"`
	LossPct  float64 `json:"loss_pct"`

	Min time.Duration `json:"min"`
	Avg time.Duration `json:"avg"`
	P50 time.Duration `json:"p50"`
	P95 time.Duration `json:"p95"`
	P99 time.Duration `json:"p99"`
	Max time.Duration `json:"max"`

	Jitter time.Duration `json:"jitter"`

	Bytes      uint64  `json:"bytes"`
	Throughput float64 `json:"throughput"` // bytes/sec over the window

	PathMTU      int `json:"path_mtu"`      // 0 = unknown
	EffectiveMSS int `json:"effective_mss"` // 0 = unknown

	Window    time.Duration `json:"window"`
	StartedAt time.Time     `json:"started_at"`
	EndedAt   time.Time     `json:"ended_at"`
}
