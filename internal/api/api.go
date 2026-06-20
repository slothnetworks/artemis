// Package api serves the manager's browser-facing HTTP/SSE/REST surface.
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/hamba/logger/v2"
	lctx "github.com/hamba/logger/v2/ctx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/slothnetworks/artemis/internal/alerting"
	"github.com/slothnetworks/artemis/internal/manager"
	"github.com/slothnetworks/artemis/internal/stats"
)

// Server is the manager's HTTP API + UI.
type Server struct {
	mgr  *manager.Manager
	log  *logger.Logger
	ui   *ui
	auth *Auth
}

// New returns an API server backed by the given manager. auth may be nil (open).
func New(mgr *manager.Manager, log *logger.Logger, auth *Auth) (*Server, error) {
	u, err := newUI()
	if err != nil {
		return nil, err
	}
	if auth == nil {
		auth = NewAuth("", "")
	}
	return &Server{mgr: mgr, log: log, ui: u, auth: auth}, nil
}

// Handler returns the HTTP handler with all routes registered.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	// Open endpoints.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /login", s.handleLoginPage)
	mux.HandleFunc("POST /auth/local", s.handleLocalAuth)
	mux.HandleFunc("GET /auth/logout", s.handleLogout)
	mux.Handle("GET /static/", staticHandler())

	// Prometheus scrape endpoint (open, like a standard exporter).
	reg := prometheus.NewRegistry()
	reg.MustRegister(s.mgr.MetricsCollector())
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	// UI pages (guarded).
	mux.HandleFunc("GET /{$}", s.auth.guard(false, s.page("dashboard")))
	mux.HandleFunc("GET /agents", s.auth.guard(false, s.page("agents")))
	mux.HandleFunc("GET /mesh", s.auth.guard(false, s.page("mesh")))
	mux.HandleFunc("GET /alerts", s.auth.guard(false, s.page("alerts")))
	mux.HandleFunc("GET /diff", s.auth.guard(false, s.page("diff")))

	// API (guarded).
	api := func(p string, h http.HandlerFunc) { mux.HandleFunc(p, s.auth.guard(true, h)) }
	api("GET /api/topology", s.topology)
	api("GET /api/agents", s.agents)
	api("GET /api/groups", s.groups)
	api("GET /api/mesh", s.getMesh)
	api("PUT /api/mesh", s.putMesh)
	api("GET /api/edges/{src}/{dst}/series", s.series)
	api("GET /api/edges/{a}/{b}/diff", s.diff)
	api("GET /api/alerts", s.alerts)
	api("GET /api/silences", s.listSilences)
	api("POST /api/silences", s.addSilence)
	api("DELETE /api/silences/{id}", s.deleteSilence)
	api("GET /api/stream", s.stream)
	return mux
}

func (s *Server) alerts(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.mgr.Alerts())
}

func (s *Server) listSilences(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.mgr.Silences().List())
}

func (s *Server) addSilence(w http.ResponseWriter, r *http.Request) {
	var sl alerting.Silence
	if err := json.NewDecoder(r.Body).Decode(&sl); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, s.mgr.Silences().Add(sl))
}

func (s *Server) deleteSilence(w http.ResponseWriter, r *http.Request) {
	s.mgr.Silences().Remove(r.PathValue("id"))
	w.WriteHeader(http.StatusNoContent)
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

// diff returns the reciprocal A→B vs B→A series plus summary deltas of the
// latest snapshots, exposing one-way / asymmetric faults.
func (s *Server) diff(w http.ResponseWriter, r *http.Request) {
	a, b := r.PathValue("a"), r.PathValue("b")
	ab := s.mgr.Store().Series(a, b)
	ba := s.mgr.Store().Series(b, a)
	latAB := latestSnap(ab)
	latBA := latestSnap(ba)
	writeJSON(w, map[string]any{
		"a": a, "b": b,
		"ab": ab, "ba": ba,
		"delta": map[string]float64{
			"loss_pct":  latAB.LossPct - latBA.LossPct,
			"p95_ms":    float64(latAB.P95-latBA.P95) / 1e6,
			"jitter_ms": float64(latAB.Jitter-latBA.Jitter) / 1e6,
		},
	})
}

func latestSnap(series []manager.TimedSnapshot) stats.Snapshot {
	if len(series) == 0 {
		return stats.Snapshot{}
	}
	return series[len(series)-1].Snap
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
