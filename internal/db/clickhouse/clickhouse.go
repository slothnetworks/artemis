// Package clickhouse implements the time-series SampleSink backed by ClickHouse.
package clickhouse

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/slothnetworks/artemis/internal/stats"
	"github.com/slothnetworks/artemis/internal/store"
	"github.com/slothnetworks/artemis/migrations"
)

// Sink is a ClickHouse-backed store.SampleSink. It buffers rows and flushes them
// in batches, which ClickHouse strongly favors.
type Sink struct {
	conn      driver.Conn
	ch        chan store.SampleRow
	done      chan struct{}
	batchSize int
	flushIvl  time.Duration
}

// Open connects to ClickHouse, applies migrations and starts the batch writer.
func Open(ctx context.Context, dsn string) (*Sink, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	s := &Sink{
		conn:      conn,
		ch:        make(chan store.SampleRow, 8192),
		done:      make(chan struct{}),
		batchSize: 1000,
		flushIvl:  time.Second,
	}
	if err := s.migrate(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	go s.run()
	return s, nil
}

func (s *Sink) migrate(ctx context.Context) error {
	entries, err := fs.ReadDir(migrations.FS, "clickhouse")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		b, err := fs.ReadFile(migrations.FS, "clickhouse/"+name)
		if err != nil {
			return err
		}
		if err := s.conn.Exec(ctx, string(b)); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

// WriteSample enqueues a row; it never blocks (drops on a full buffer).
func (s *Sink) WriteSample(r store.SampleRow) {
	select {
	case s.ch <- r:
	default:
	}
}

func (s *Sink) run() {
	t := time.NewTicker(s.flushIvl)
	defer t.Stop()
	buf := make([]store.SampleRow, 0, s.batchSize)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		if err := s.insert(buf); err == nil {
			buf = buf[:0]
		}
	}
	for {
		select {
		case <-s.done:
			flush()
			return
		case r := <-s.ch:
			buf = append(buf, r)
			if len(buf) >= s.batchSize {
				flush()
			}
		case <-t.C:
			flush()
		}
	}
}

func (s *Sink) insert(rows []store.SampleRow) error {
	batch, err := s.conn.PrepareBatch(context.Background(), "INSERT INTO samples")
	if err != nil {
		return err
	}
	for _, r := range rows {
		sn := r.Snap
		if err := batch.Append(
			r.Ts, r.Src, r.Dst, "",
			sn.Sent, sn.Received, sn.Dropped, sn.Timeout, sn.LossPct,
			int64(sn.Min), int64(sn.Avg), int64(sn.P50), int64(sn.P95), int64(sn.P99), int64(sn.Max),
			int64(sn.Jitter), sn.Bytes, sn.Throughput,
			int32(sn.PathMTU), int32(sn.EffectiveMSS), int64(r.Resolution),
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

// QuerySeries returns samples for an edge in the given time range, ordered by ts.
func (s *Sink) QuerySeries(ctx context.Context, src, dst string, from, to time.Time) ([]store.SampleRow, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT ts, sent, received, dropped, timeout, loss_pct,
		       min_ns, avg_ns, p50_ns, p95_ns, p99_ns, max_ns, jitter_ns,
		       bytes, throughput, path_mtu, effective_mss
		FROM samples
		WHERE src = ? AND dst = ? AND ts BETWEEN ? AND ?
		ORDER BY ts`, src, dst, from, to)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []store.SampleRow
	for rows.Next() {
		var (
			ts                                                 time.Time
			sent, received, dropped, timeout, bytes            uint64
			lossPct, throughput                                float64
			minNs, avgNs, p50Ns, p95Ns, p99Ns, maxNs, jitterNs int64
			pathMTU, effMSS                                    int32
		)
		if err := rows.Scan(&ts, &sent, &received, &dropped, &timeout, &lossPct,
			&minNs, &avgNs, &p50Ns, &p95Ns, &p99Ns, &maxNs, &jitterNs,
			&bytes, &throughput, &pathMTU, &effMSS); err != nil {
			return nil, err
		}
		out = append(out, store.SampleRow{Ts: ts, Src: src, Dst: dst, Snap: stats.Snapshot{
			Target: dst, Sent: sent, Received: received, Dropped: dropped, Timeout: timeout, LossPct: lossPct,
			Min: time.Duration(minNs), Avg: time.Duration(avgNs), P50: time.Duration(p50Ns),
			P95: time.Duration(p95Ns), P99: time.Duration(p99Ns), Max: time.Duration(maxNs),
			Jitter: time.Duration(jitterNs), Bytes: bytes, Throughput: throughput,
			PathMTU: int(pathMTU), EffectiveMSS: int(effMSS),
		}})
	}
	return out, rows.Err()
}

// Close flushes pending rows and closes the connection.
func (s *Sink) Close() error {
	close(s.done)
	time.Sleep(50 * time.Millisecond) // allow final flush
	return s.conn.Close()
}
