package probe

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/hamba/logger/v2"
	lctx "github.com/hamba/logger/v2/ctx"
	"github.com/slothnetworks/artemis/internal/probe/tcp"
	"github.com/slothnetworks/artemis/internal/probe/udp"
	"github.com/slothnetworks/artemis/internal/stats"
)

// Option configures a Client.
type Option func(*Client)

// WithCollector sets the stats collector the client feeds probe events into.
// Defaults to stats.NopCollector (log-only, zero overhead).
func WithCollector(coll stats.Collector) Option {
	return func(c *Client) { c.coll = coll }
}

// WithPayloadSize sets a fixed probe payload size in bytes. When > 0 the client
// sends self-describing payloads (see NewPayload) instead of the default text.
func WithPayloadSize(size int) Option {
	return func(c *Client) { c.payloadSize = size }
}

// WithPayloadSizes sets a set of payload sizes to rotate through per probe — an
// MTU/fragmentation sweep. Takes precedence over WithPayloadSize.
func WithPayloadSizes(sizes []int) Option {
	return func(c *Client) { c.payloadSizes = sizes }
}

// WithBurst sets how many probes to send per interval (default 1).
func WithBurst(n int) Option {
	return func(c *Client) { c.burst = n }
}

// Client attempts to hold a connection with a server, sending probe messages at a configured interval.
type Client struct {
	backoff      time.Duration
	sendInterval time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	payloadSize  int
	payloadSizes []int
	burst        int

	coll stats.Collector
	log  *logger.Logger
}

// payloadFor returns the payload data for the probe with the given id, honoring
// the configured fixed size or sweep. With no size configured it preserves the
// original "Hello N" text for backward compatibility.
func (c *Client) payloadFor(id uint64) string {
	size := c.payloadSize
	if n := len(c.payloadSizes); n > 0 {
		size = c.payloadSizes[int(id-1)%n]
	}
	if size > 0 {
		return NewPayload(size, id)
	}
	return fmt.Sprintf("Hello %d", id)
}

// NewClient returns a client. By default it uses a no-op stats collector;
// pass WithCollector to feed probe events into an aggregator.
func NewClient(backoff, sendInterval, readTimeout, writeTimeout time.Duration, log *logger.Logger, opts ...Option) *Client {
	c := &Client{
		backoff:      backoff,
		sendInterval: sendInterval,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		coll:         stats.NopCollector{},
		log:          log,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Run sends probe messages to the server continuously.
// If the connection fails, it retries at the configured backoff interval.
func (c *Client) Run(ctx context.Context, protocol, addr string) {
	var (
		conn net.Conn
		err  error
		idx  int
	)
	for {
		log := c.log.With(lctx.Str("protocol", protocol), lctx.Str("addr", addr), lctx.Int("reconnect", idx))
		idx++

		switch protocol {
		case "tcp":
			conn, err = tcp.Connect(addr)
		case "udp":
			conn, err = udp.Connect(addr)
		default:
			log.Error("Unexpected protocol")
			return
		}
		if err != nil {
			log.Error("Could not connect to server", lctx.Err(err))

			select {
			case <-ctx.Done():
				return
			case <-time.After(c.backoff):
				continue
			}
		}

		if err = c.handleConn(ctx, conn, addr); err != nil {
			log.Error("Connection error", lctx.Err(err))
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

type expectation struct {
	timestamp time.Time
	probe     Probe
}

func (c *Client) handleConn(ctx context.Context, conn net.Conn, target string) error { //nolint:funlen // Simplify readability.
	defer func() { _ = conn.Close() }()

	readCh := make(chan readResponse)
	go c.readLoop(conn, readCh)

	enc := NewEncoder(conn)

	id := uint64(1)
	var expect []expectation
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(c.sendInterval):
			bursts := c.burst
			if bursts < 1 {
				bursts = 1
			}
			for b := 0; b < bursts; b++ {
				_ = conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))

				p := Probe{ID: id, Data: c.payloadFor(id)}
				if err := enc.Encode(p); err != nil {
					return fmt.Errorf("writing message: %w", err)
				}

				c.log.Info("Message sent", lctx.Interface("probe", p))
				c.coll.Record(stats.Event{Target: target, Kind: stats.Sent, Bytes: len(p.Data), At: time.Now()})

				id++
				expect = append(expect, expectation{timestamp: time.Now(), probe: p})
			}
		case resp, ok := <-readCh:
			if !ok {
				// The read loop ended; any still-outstanding probes never came
				// back, so account for them as connection-loss timeouts.
				c.recordTimeouts(target, expect)
				return nil
			}
			if resp.err != nil {
				c.recordTimeouts(target, expect)
				return fmt.Errorf("reading response: %w", resp.err)
			}

			var (
				exp   expectation
				found bool
			)
			for {
				if len(expect) == 0 {
					break
				}
				exp, expect = expect[0], expect[1:]
				if exp.probe.ID == resp.probe.ID {
					found = true
					break
				}

				c.log.Warn("Message dropped",
					lctx.Str("error", "unexpected ID"),
					lctx.Uint64("expected_id", resp.probe.ID),
					lctx.Uint64("id", exp.probe.ID),
					lctx.Str("data", exp.probe.Data),
				)
				c.coll.Record(stats.Event{Target: target, Kind: stats.Dropped, At: time.Now()})
			}
			if !found {
				c.log.Error("No expectation found")
				continue
			}

			// Round-trip integrity check: the echo server returns bytes
			// unchanged, so any difference means the payload was modified in
			// flight. From the source we can only attribute this to the
			// round-trip (return) path; the destination agent's forward check
			// disambiguates in the mesh.
			if resp.probe.Data != exp.probe.Data {
				c.log.Warn("Message modified",
					lctx.Uint64("id", exp.probe.ID),
					lctx.Int("sent_bytes", len(exp.probe.Data)),
					lctx.Int("recv_bytes", len(resp.probe.Data)),
				)
				c.coll.Record(stats.Event{
					Target:     target,
					Kind:       stats.SizeMismatch,
					Latency:    resp.timestamp.Sub(exp.timestamp),
					SentBytes:  len(exp.probe.Data),
					Bytes:      len(resp.probe.Data),
					Direction:  stats.DirReturn,
					ChecksumOK: false,
					At:         resp.timestamp,
				})
				continue
			}

			c.log.Info("Message received",
				lctx.Uint64("id", exp.probe.ID),
				lctx.Str("data", exp.probe.Data),
				lctx.Duration("took", resp.timestamp.Sub(exp.timestamp)),
			)
			c.coll.Record(stats.Event{
				Target:  target,
				Kind:    stats.Received,
				Latency: resp.timestamp.Sub(exp.timestamp),
				Bytes:   len(exp.probe.Data),
				At:      resp.timestamp,
			})
		}
	}
}

// recordTimeouts accounts for outstanding probes abandoned on connection
// teardown so loss reflects connection loss, not just unexpected echo IDs.
func (c *Client) recordTimeouts(target string, expect []expectation) {
	now := time.Now()
	for range expect {
		c.coll.Record(stats.Event{Target: target, Kind: stats.Timeout, At: now})
	}
}

type readResponse struct {
	timestamp time.Time
	probe     Probe
	err       error
}

func (c *Client) readLoop(conn net.Conn, ch chan readResponse) {
	defer close(ch)

	dec := NewDecoder(conn)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(c.readTimeout))

		msg, err := dec.Decode()
		if err != nil {
			ch <- readResponse{err: err}
			return
		}

		p, ok := msg.(Probe)
		if !ok {
			ch <- readResponse{err: fmt.Errorf("message not a probe: %T", msg)}
			continue
		}

		ch <- readResponse{timestamp: time.Now(), probe: p}
	}
}
