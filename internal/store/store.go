// Package store defines the durable-storage abstraction for samples (time
// series) and events/alerts, plus no-op implementations so the manager runs
// fully in-memory when no database is configured.
package store

import (
	"context"
	"time"

	"github.com/slothnetworks/artemis/internal/stats"
)

// SampleRow is one downsampled per-edge statistics row destined for the
// time-series store (ClickHouse).
type SampleRow struct {
	Ts         time.Time
	Src        string
	Dst        string
	Snap       stats.Snapshot
	Resolution time.Duration
}

// Event is a discrete fault observation destined for the relational store
// (Postgres).
type Event struct {
	Ts         time.Time
	Src        string
	Dst        string
	Protocol   string
	Kind       string // packet_loss | timeout | size_mismatch
	ProbeID    uint64
	SentBytes  int
	RecvBytes  int
	ChecksumOK bool
	Direction  string // forward | return | none
}

// SampleSink persists time-series samples and answers history queries. WriteSample
// must be non-blocking (buffered/async) so it never stalls telemetry ingest.
type SampleSink interface {
	WriteSample(SampleRow)
	QuerySeries(ctx context.Context, src, dst string, from, to time.Time) ([]SampleRow, error)
	Close() error
}

// Alert is a stateful, grouped alert derived from events/samples.
type Alert struct {
	ID          string    `json:"id"`  // deterministic dedup key: src|dst|rule
	UID         string    `json:"uid"` // stable per-incident UUID (for URLs)
	Src         string    `json:"src"`
	Dst         string    `json:"dst"`
	Rule        string    `json:"rule"`
	Severity    string    `json:"severity"`
	State       string    `json:"state"` // firing | resolved
	Reason      string    `json:"reason"`
	Silenced    bool      `json:"silenced"`
	StartedAt   time.Time `json:"started_at"`
	ResolvedAt  time.Time `json:"resolved_at"`
	LastEventAt time.Time `json:"last_event_at"`
	EventCount  int       `json:"event_count"`
}

// EventSink persists discrete events.
type EventSink interface {
	WriteEvent(ctx context.Context, e Event) error
	Close() error
}

// AlertSink persists stateful alerts (upsert by Alert.ID).
type AlertSink interface {
	UpsertAlert(ctx context.Context, a Alert) error
}

// NopAlertSink discards alerts.
type NopAlertSink struct{}

// UpsertAlert implements AlertSink.
func (NopAlertSink) UpsertAlert(context.Context, Alert) error { return nil }

// NopSampleSink discards samples; QuerySeries returns nothing.
type NopSampleSink struct{}

// WriteSample implements SampleSink.
func (NopSampleSink) WriteSample(SampleRow) {}

// QuerySeries implements SampleSink.
func (NopSampleSink) QuerySeries(context.Context, string, string, time.Time, time.Time) ([]SampleRow, error) {
	return nil, nil
}

// Close implements SampleSink.
func (NopSampleSink) Close() error { return nil }

// NopEventSink discards events.
type NopEventSink struct{}

// WriteEvent implements EventSink.
func (NopEventSink) WriteEvent(context.Context, Event) error { return nil }

// Close implements EventSink.
func (NopEventSink) Close() error { return nil }
