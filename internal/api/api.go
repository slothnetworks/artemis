// Package api serves the manager's browser-facing HTTP/SSE/REST surface.
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/hamba/logger/v2"
	lctx "github.com/hamba/logger/v2/ctx"
	"github.com/slothnetworks/artemis/internal/manager"
)

// Server is the manager's HTTP API.
type Server struct {
	mgr *manager.Manager
	log *logger.Logger
}

// New returns an API server backed by the given manager.
func New(mgr *manager.Manager, log *logger.Logger) *Server {
	return &Server{mgr: mgr, log: log}
}

// Handler returns the HTTP handler with all routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /api/topology", s.topology)
	mux.HandleFunc("GET /api/agents", s.agents)
	mux.HandleFunc("GET /api/groups", s.groups)
	mux.HandleFunc("GET /api/mesh", s.getMesh)
	mux.HandleFunc("PUT /api/mesh", s.putMesh)
	mux.HandleFunc("GET /api/edges/{src}/{dst}/series", s.series)
	mux.HandleFunc("GET /api/stream", s.stream)
	return mux
}

type meshDoc struct {
	Mode   string             `json:"mode"`
	Params manager.MeshParams `json:"params"`
}

func (s *Server) groups(w http.ResponseWriter, _ *http.Request) {
	seen := map[string]struct{}{}
	var groups []string
	for _, a := range s.mgr.Registry().List() {
		if _, ok := seen[a.Group]; !ok {
			seen[a.Group] = struct{}{}
			groups = append(groups, a.Group)
		}
	}
	writeJSON(w, groups)
}

func (s *Server) getMesh(w http.ResponseWriter, _ *http.Request) {
	mode, params := s.mgr.Mesh().Get()
	writeJSON(w, meshDoc{Mode: mode, Params: params})
}

func (s *Server) putMesh(w http.ResponseWriter, r *http.Request) {
	var doc meshDoc
	if err := json.NewDecoder(r.Body).Decode(&doc); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mgr.Mesh().Set(doc.Mode, doc.Params)
	s.mgr.RecomputeMesh()
	mode, params := s.mgr.Mesh().Get()
	writeJSON(w, meshDoc{Mode: mode, Params: params})
}

func (s *Server) topology(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.mgr.Topology())
}

func (s *Server) agents(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.mgr.Registry().List())
}

func (s *Server) series(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.mgr.Store().Series(r.PathValue("src"), r.PathValue("dst")))
}

// stream serves Server-Sent Events of live edge updates.
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	ch := s.mgr.Subscribe()
	defer s.mgr.Unsubscribe(ch)

	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-keepalive.C:
			_, _ = fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case data, ok := <-ch:
			if !ok {
				return
			}
			_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// LogMiddleware logs each request at debug level.
func LogMiddleware(log *logger.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Debug("HTTP request",
			lctx.Str("method", r.Method),
			lctx.Str("path", r.URL.Path),
			lctx.Duration("took", time.Since(start)),
		)
	})
}
