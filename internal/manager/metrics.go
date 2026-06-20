package manager

import "github.com/prometheus/client_golang/prometheus"

// metricsCollector exposes the live mesh state as Prometheus metrics, read from
// the in-memory store on each scrape (no per-event overhead).
type metricsCollector struct {
	m *Manager

	loss       *prometheus.Desc
	p95        *prometheus.Desc
	jitter     *prometheus.Desc
	throughput *prometheus.Desc
	alerts     *prometheus.Desc
	agents     *prometheus.Desc
}

// MetricsCollector returns a prometheus.Collector for the manager's mesh state.
func (m *Manager) MetricsCollector() prometheus.Collector {
	edgeLabels := []string{"src", "dst"}
	return &metricsCollector{
		m:          m,
		loss:       prometheus.NewDesc("artemis_edge_loss_percent", "Per-edge packet loss percentage.", edgeLabels, nil),
		p95:        prometheus.NewDesc("artemis_edge_latency_p95_seconds", "Per-edge p95 round-trip latency.", edgeLabels, nil),
		jitter:     prometheus.NewDesc("artemis_edge_jitter_seconds", "Per-edge interarrival jitter.", edgeLabels, nil),
		throughput: prometheus.NewDesc("artemis_edge_throughput_bytes", "Per-edge throughput (bytes/sec).", edgeLabels, nil),
		alerts:     prometheus.NewDesc("artemis_alerts_active", "Active alerts by severity.", []string{"severity"}, nil),
		agents:     prometheus.NewDesc("artemis_agents", "Enrolled agents by status.", []string{"status"}, nil),
	}
}

// Describe implements prometheus.Collector.
func (c *metricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.loss
	ch <- c.p95
	ch <- c.jitter
	ch <- c.throughput
	ch <- c.alerts
	ch <- c.agents
}

// Collect implements prometheus.Collector.
func (c *metricsCollector) Collect(ch chan<- prometheus.Metric) {
	g := prometheus.GaugeValue
	for _, e := range c.m.store.Edges() {
		s := e.Snapshot
		ch <- prometheus.MustNewConstMetric(c.loss, g, s.LossPct, e.Src, e.Dst)
		ch <- prometheus.MustNewConstMetric(c.p95, g, s.P95.Seconds(), e.Src, e.Dst)
		ch <- prometheus.MustNewConstMetric(c.jitter, g, s.Jitter.Seconds(), e.Src, e.Dst)
		ch <- prometheus.MustNewConstMetric(c.throughput, g, s.Throughput, e.Src, e.Dst)
	}

	bySeverity := map[string]int{}
	for _, a := range c.m.alerts.Active() {
		bySeverity[a.Severity]++
	}
	for sev, n := range bySeverity {
		ch <- prometheus.MustNewConstMetric(c.alerts, g, float64(n), sev)
	}

	byStatus := map[string]int{}
	for _, a := range c.m.reg.List() {
		byStatus[a.Status]++
	}
	for status, n := range byStatus {
		ch <- prometheus.MustNewConstMetric(c.agents, g, float64(n), status)
	}
}
