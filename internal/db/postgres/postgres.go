// Package postgres implements the relational EventSink backed by PostgreSQL.
package postgres

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/slothnetworks/artemis/internal/store"
	"github.com/slothnetworks/artemis/migrations"
)

// Sink is a PostgreSQL-backed store.EventSink.
type Sink struct {
	pool *pgxpool.Pool
}

// Open connects to PostgreSQL and applies migrations.
func Open(ctx context.Context, dsn string) (*Sink, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	s := &Sink{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *Sink) migrate(ctx context.Context) error {
	entries, err := fs.ReadDir(migrations.FS, "postgres")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, name := range names {
		b, err := fs.ReadFile(migrations.FS, "postgres/"+name)
		if err != nil {
			return err
		}
		for _, stmt := range splitStatements(string(b)) {
			if _, err := s.pool.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	return nil
}

// WriteEvent inserts a fault event.
func (s *Sink) WriteEvent(ctx context.Context, e store.Event) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO events (ts, src, dst, protocol, kind, probe_id, sent_bytes, recv_bytes, checksum_ok, direction)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		e.Ts, e.Src, e.Dst, e.Protocol, e.Kind, int64(e.ProbeID), e.SentBytes, e.RecvBytes, e.ChecksumOK, e.Direction)
	return err
}

// Close closes the connection pool.
func (s *Sink) Close() error {
	s.pool.Close()
	return nil
}

// splitStatements splits a SQL file on semicolons, dropping empty statements.
func splitStatements(sql string) []string {
	parts := strings.Split(sql, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if strings.TrimSpace(p) != "" {
			out = append(out, p)
		}
	}
	return out
}
