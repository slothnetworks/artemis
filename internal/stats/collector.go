// Package stats aggregates probe events into rolling connection-quality
// statistics (loss, latency percentiles, jitter, throughput) that the prober,
// agent and manager feed into and snapshot for reporting.
package stats

import "time"

// EventKind classifies a probe observation.
type EventKind int

const (
	// Sent marks a probe that was written to the connection.
	Sent EventKind = iota
	// Received marks a probe echo that matched an outstanding expectation.
	Received
	// Dropped marks a probe lost in transit (e.g. an unexpected echo ID).
	Dropped
	// Timeout marks an outstanding probe abandoned on connection teardown.
	Timeout
	// SizeMismatch marks a probe whose payload was modified in flight.
	SizeMismatch
)

// Direction identifies which path a modification was observed on.
type Direction int

const (
	// DirNone is used when direction is not applicable.
	DirNone Direction = iota
	// DirForward is the source→destination path (validated at the destination).
	DirForward
	// DirReturn is the destination→source echo path (validated by the source).
	DirReturn
)

// Event is a single probe observation fed to a Collector.
type Event struct {
	Target     string        // edge/target the event belongs to
	Kind       EventKind     // what happened
	Latency    time.Duration // round-trip, valid for Received
	Bytes      int           // payload size observed
	SentBytes  int           // declared/sent size, for SizeMismatch
	Direction  Direction     // path, for SizeMismatch
	ChecksumOK bool          // payload checksum matched, for SizeMismatch
	At         time.Time     // observation time
}

// Collector receives probe events. Implementations must be safe for concurrent
// use by multiple goroutines.
type Collector interface {
	Record(Event)
}

// NopCollector is a Collector that discards every event. It is the default,
// preserving the prober's log-only behaviour with zero overhead.
type NopCollector struct{}

// Record implements Collector.
func (NopCollector) Record(Event) {}
