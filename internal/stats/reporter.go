package stats

import (
	"context"
	"time"

	"github.com/hamba/logger/v2"
	lctx "github.com/hamba/logger/v2/ctx"
)

// Reporter periodically logs a summary of a RollingCollector's snapshots and
// emits a final summary when its context is cancelled.
type Reporter struct {
	coll     *RollingCollector
	log      *logger.Logger
	interval time.Duration
}

// NewReporter returns a Reporter. A non-positive interval disables periodic
// summaries (only the end-of-run summary is emitted).
func NewReporter(coll *RollingCollector, log *logger.Logger, interval time.Duration) *Reporter {
	return &Reporter{coll: coll, log: log, interval: interval}
}

// Run logs periodic summaries until ctx is done, then logs a final summary.
func (r *Reporter) Run(ctx context.Context) {
	if r.interval > 0 {
		tick := time.NewTicker(r.interval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				r.summary("Final summary")
				return
			case <-tick.C:
				r.summary("Summary")
			}
		}
	}
	<-ctx.Done()
	r.summary("Final summary")
}

func (r *Reporter) summary(msg string) {
	for _, s := range r.coll.Snapshot() {
		r.log.Info(msg,
			lctx.Str("target", s.Target),
			lctx.Uint64("sent", s.Sent),
			lctx.Uint64("received", s.Received),
			lctx.Uint64("dropped", s.Dropped),
			lctx.Uint64("timeout", s.Timeout),
			lctx.Interface("loss_pct", round2(s.LossPct)),
			lctx.Duration("min", s.Min),
			lctx.Duration("avg", s.Avg),
			lctx.Duration("p50", s.P50),
			lctx.Duration("p95", s.P95),
			lctx.Duration("p99", s.P99),
			lctx.Duration("max", s.Max),
			lctx.Duration("jitter", s.Jitter),
		)
	}
}

func round2(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}
