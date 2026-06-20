package stats

// PathSetter is implemented by collectors that can record observed path
// properties (MTU / effective MSS) for a target.
type PathSetter interface {
	SetPath(target string, pathMTU, effMSS int)
}

// MultiCollector fans each event out to several collectors (e.g. a
// RollingCollector for live stats plus a Prometheus collector for scraping).
type MultiCollector []Collector

// Record implements Collector.
func (m MultiCollector) Record(e Event) {
	for _, c := range m {
		c.Record(e)
	}
}

// SetPath forwards observed path properties to any member implementing
// PathSetter, so MultiCollector itself satisfies PathSetter.
func (m MultiCollector) SetPath(target string, pathMTU, effMSS int) {
	for _, c := range m {
		if ps, ok := c.(PathSetter); ok {
			ps.SetPath(target, pathMTU, effMSS)
		}
	}
}
