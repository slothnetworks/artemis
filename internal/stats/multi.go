package stats

// MultiCollector fans each event out to several collectors (e.g. a
// RollingCollector for live stats plus a Prometheus collector for scraping).
type MultiCollector []Collector

// Record implements Collector.
func (m MultiCollector) Record(e Event) {
	for _, c := range m {
		c.Record(e)
	}
}
