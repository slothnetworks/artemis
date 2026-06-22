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
	_ "github.com/slothnetworks/artemis/docs/swagger" // registers the OpenAPI spec
	"github.com/slothnetworks/artemis/internal/alerting"
	"github.com/slothnetworks/artemis/internal/manager"
	"github.com/slothnetworks/artemis/internal/stats"
	"github.com/slothnetworks/artemis/internal/store"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

// @title       Artemis API
// @version     1.0
// @description Control-plane REST/SSE API for the artemis connection-quality mesh.
// @BasePath    /

// Server is the manager's HTTP API + UI.
type Server struct {
	mgr  *manager.Manager
	log  *logger.Logger
	ui   *ui
	auth *Auth
	dev  bool
}

// New returns an API server backed by the given manager. auth may be nil (open).
// When dev is true, the debug/fault-injection endpoints are enabled.
func New(mgr *manager.Manager, log *logger.Logger, auth *Auth, dev bool) (*Server, error) {
	u, err := newUI()
	if err != nil {
		return nil, err
	}
	if auth == nil {
		auth = NewAuth("", "")
	}
	return &Server{mgr: mgr, log: log, ui: u, auth: auth, dev: dev}, nil
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

	// Swagger UI (guarded like the rest of the UI).
	mux.HandleFunc("GET /swagger/", s.auth.guard(false, httpSwagger.Handler(httpSwagger.URL("/swagger/doc.json")).ServeHTTP))

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

	// Debug / fault-injection endpoints (dev mode only).
	if s.dev {
		api("POST /api/debug/inject", s.injectFault)
		api("GET /api/debug/enabled", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, true) })
	}
	return mux
}

// injectFault makes an agent drop a fraction of received probes for a duration.
// @Summary  Inject packet loss (dev only)
// @Tags     debug
// @Accept   json
// @Param    body body object true "agent, loss_percent, duration_ms"
// @Success  200 {object} map[string]bool
// @Router   /api/debug/inject [post]
func (s *Server) injectFault(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Agent       string  `json:"agent"`
		LossPercent float64 `json:"loss_percent"`
		DurationMs  int64   `json:"duration_ms"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if body.DurationMs <= 0 {
		body.DurationMs = 30000
	}
	ok := s.mgr.InjectFault(body.Agent, body.LossPercent, body.DurationMs)
	if !ok {
		http.Error(w, `{"error":"unknown agent"}`, http.StatusNotFound)
		return
	}
	s.log.Warn("Fault injected", lctx.Str("agent", body.Agent), lctx.Interface("loss_percent", body.LossPercent))
	writeJSON(w, map[string]bool{"ok": true})
}

// alerts lists currently firing alerts.
// @Summary  Active alerts
// @Tags     alerts
// @Produce  json
// @Success  200 {array} store.Alert
// @Router   /api/alerts [get]
func (s *Server) alerts(w http.ResponseWriter, _ *http.Request) {
	var out []store.Alert = s.mgr.Alerts()
	writeJSON(w, out)
}

// listSilences lists active silences.
// @Summary  List silences
// @Tags     alerts
// @Produce  json
// @Success  200 {array} alerting.Silence
// @Router   /api/silences [get]
func (s *Server) listSilences(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.mgr.Silences().List())
}

// addSilence creates a silence.
// @Summary  Add silence
// @Tags     alerts
// @Accept   json
// @Produce  json
// @Param    silence body alerting.Silence true "silence matcher + expiry"
// @Success  200 {object} alerting.Silence
// @Router   /api/silences [post]
func (s *Server) addSilence(w http.ResponseWriter, r *http.Request) {
	var sl alerting.Silence
	if err := json.NewDecoder(r.Body).Decode(&sl); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, s.mgr.Silences().Add(sl))
}

// deleteSilence removes a silence by id.
// @Summary  Delete silence
// @Tags     alerts
// @Param    id path string true "silence id"
// @Success  204
// @Router   /api/silences/{id} [delete]
func (s *Server) deleteSilence(w http.ResponseWriter, r *http.Request) {
	s.mgr.Silences().Remove(r.PathValue("id"))
	w.WriteHeader(http.StatusNoContent)
}

type meshDoc struct {
	Mode   string             `json:"mode"`
	Params manager.MeshParams `json:"params"`
}

// groups lists the distinct agent groups.
// @Summary  Agent groups
// @Tags     mesh
// @Produce  json
// @Success  200 {array} string
// @Router   /api/groups [get]
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

// getMesh returns the current mesh mode and parameters.
// @Summary  Get mesh config
// @Tags     mesh
// @Produce  json
// @Success  200 {object} meshDoc
// @Router   /api/mesh [get]
func (s *Server) getMesh(w http.ResponseWriter, _ *http.Request) {
	mode, params := s.mgr.Mesh().Get()
	writeJSON(w, meshDoc{Mode: mode, Params: params})
}

// putMesh sets the mesh mode and parameters and recomputes assignments.
// @Summary  Set mesh config
// @Tags     mesh
// @Accept   json
// @Produce  json
// @Param    mesh body meshDoc true "mesh mode + params"
// @Success  200 {object} meshDoc
// @Router   /api/mesh [put]
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

// topology returns the mesh nodes and edges with latest metrics.
// @Summary  Mesh topology
// @Tags     mesh
// @Produce  json
// @Success  200 {object} manager.TopologyView
// @Router   /api/topology [get]
func (s *Server) topology(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.mgr.Topology())
}

// agents lists enrolled agents.
// @Summary  List agents
// @Tags     agents
// @Produce  json
// @Success  200 {array} manager.AgentInfo
// @Router   /api/agents [get]
func (s *Server) agents(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.mgr.Registry().List())
}

// series returns the recent snapshot history for an edge.
// @Summary  Edge time-series
// @Tags     edges
// @Produce  json
// @Param    src path string true "source agent"
// @Param    dst path string true "destination agent"
// @Success  200 {array} manager.TimedSnapshot
// @Router   /api/edges/{src}/{dst}/series [get]
func (s *Server) series(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.mgr.Store().Series(r.PathValue("src"), r.PathValue("dst")))
}

// diff returns the reciprocal A→B vs B→A series plus summary deltas of the
// latest snapshots, exposing one-way / asymmetric faults.
// @Summary  Reciprocal edge diff
// @Tags     edges
// @Produce  json
// @Param    a path string true "agent A"
// @Param    b path string true "agent B"
// @Success  200 {object} map[string]interface{}
// @Router   /api/edges/{a}/{b}/diff [get]
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
