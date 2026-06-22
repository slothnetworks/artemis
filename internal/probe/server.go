package probe

import (
	"errors"
	"io"
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"github.com/hamba/logger/v2"
	lctx "github.com/hamba/logger/v2/ctx"
)

// Server handles connections from clients.
type Server struct {
	bufSize      int
	readTimeout  time.Duration
	writeTimeout time.Duration

	dropPermille atomic.Int64 // fault injection: per-mille of probes to drop

	log *logger.Logger
}

// SetDropRate sets the fraction (0..1) of received probes the server silently
// drops instead of echoing. Used for fault injection in tests/demos.
func (s *Server) SetDropRate(p float64) {
	if p < 0 {
		p = 0
	}
	if p > 1 {
		p = 1
	}
	s.dropPermille.Store(int64(p * 1000))
}

// NewServer returns a server.
func NewServer(bufSize int, readTimeout, writeTimeout time.Duration, log *logger.Logger) *Server {
	return &Server{
		bufSize:      bufSize,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		log:          log,
	}
}

// Serve handles a connection from a client.
//
// The handler provides an identical response to every message it receives.
// The caller who initiated the connection is responsible for ensuring its closure.
func (s *Server) Serve(conn net.PacketConn) { //nolint:cyclop // Simplify readability.
	buf := make([]byte, s.bufSize)
	for {
		log := s.log

		_ = conn.SetReadDeadline(time.Now().Add(s.readTimeout))
		n, addr, err := conn.ReadFrom(buf)

		if addr != nil {
			log = log.With(lctx.Str("protocol", addr.Network()), lctx.Str("addr", addr.String()))
		}

		if err != nil {
			var netErr net.Error
			switch {
			case errors.Is(err, io.EOF):
				return
			case errors.Is(err, net.ErrClosed):
				return
			case errors.As(err, &netErr) && netErr.Timeout():
				log.Error("Reading from connection timed out", lctx.Err(err))
				continue
			default:
				s.log.Error("Could not read request", lctx.Err(err))
				continue
			}
		}
		log.Debug("Message received", lctx.Str("data", string(buf[:n])))

		// Fault injection: silently drop a fraction of probes so peers see loss.
		if dp := s.dropPermille.Load(); dp > 0 && rand.Int63n(1000) < dp {
			log.Debug("Dropping probe (fault injection)")
			continue
		}

		_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))

		wn, err := conn.WriteTo(buf[:n], addr)
		switch {
		case err != nil && errors.Is(err, net.ErrClosed):
			return
		case err != nil:
			log.Error("Could not write response", lctx.Err(err))
			continue
		}
		if wn != n {
			log.Error("Unexpected write length", lctx.Int("expected", n), lctx.Int("actual", wn))
			continue
		}
		log.Debug("Message sent", lctx.Str("data", string(buf[:n])))
	}
}
