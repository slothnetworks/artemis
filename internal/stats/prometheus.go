package stats

import "github.com/prometheus/client_golang/prometheus"

// Namespace is the Prometheus metric namespace for all artemis metrics.
const Namespace = "artemis"

// PromCollector is a Collector that exports probe events as Prometheus metrics.
type PromCollector struct {
	sent     *prometheus.CounterVec
	received *prometheus.CounterVec
	dropped  *prometheus.CounterVec // labels: target, reason
	mismatch *prometheus.CounterVec // labels: target, direction
	bytes    *prometheus.CounterVec
	latency  *prometheus.HistogramVec
}

// NewPromCollector creates a PromCollector and registers its metrics with reg.
func NewPromCollector(reg prometheus.Registerer) *PromCollector {
	c := &PromCollector{
		sent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "probe", Name: "sent_total",
			Help: "Total probes sent.",
		}, []string{"target"}),
		received: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "probe", Name: "received_total",
			Help: "Total probe echoes received and matched.",
		}, []string{"target"}),
		dropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "probe", Name: "dropped_total",
			Help: "Total probes that did not reach, by reason.",
		}, []string{"target", "reason"}),
		mismatch: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "probe", Name: "size_mismatch_total",
			Help: "Total probes modified in flight, by direction.",
		}, []string{"target", "direction"}),
		bytes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace, Subsystem: "probe", Name: "bytes_total",
			Help: "Total probe payload bytes sent.",
		}, []string{"target"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace, Subsystem: "probe", Name: "latency_seconds",
			Help:    "Probe round-trip latency in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14), // 0.5ms .. ~4s
		}, []string{"target"}),
	}
	reg.MustRegister(c.sent, c.received, c.dropped, c.mismatch, c.bytes, c.latency)
	return c
}

// Record implements Collector.
func (c *PromCollector) Record(e Event) {
	switch e.Kind {
	case Sent:
		c.sent.WithLabelValues(e.Target).Inc()
		if e.Bytes > 0 {
			c.bytes.WithLabelValues(e.Target).Add(float64(e.Bytes))
		}
	case Received:
		c.received.WithLabelValues(e.Target).Inc()
		c.latency.WithLabelValues(e.Target).Observe(e.Latency.Seconds())
	case Dropped:
		c.dropped.WithLabelValues(e.Target, "id_mismatch").Inc()
	case Timeout:
		c.dropped.WithLabelValues(e.Target, "timeout").Inc()
	case SizeMismatch:
		c.mismatch.WithLabelValues(e.Target, e.Direction.String()).Inc()
	}
}

// String returns a stable label value for a Direction.
func (d Direction) String() string {
	switch d {
	case DirForward:
		return "forward"
	case DirReturn:
		return "return"
	default:
		return "none"
	}
}
