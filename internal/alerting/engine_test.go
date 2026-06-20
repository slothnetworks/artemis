package alerting_test

import (
	"testing"
	"time"

	"github.com/slothnetworks/artemis/internal/alerting"
	"github.com/slothnetworks/artemis/internal/stats"
	"github.com/slothnetworks/artemis/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newEngine(changes *[]store.Alert) *alerting.Engine {
	return alerting.New(alerting.Config{LossPct: 20, HighLatency: 200 * time.Millisecond},
		store.NopAlertSink{}, nil, alerting.NewSilences(), func(a store.Alert) {
			*changes = append(*changes, a)
		})
}

func TestEngine_OpensAndResolvesPacketLoss(t *testing.T) {
	var changes []store.Alert
	e := newEngine(&changes)
	now := time.Now()

	// 50% loss → open packet_loss.
	bad := stats.Snapshot{Sent: 10, Received: 5, LossPct: 50}
	e.Evaluate([]alerting.EdgeInput{{Src: "a", Dst: "b", Snap: bad, UpdatedAt: now}}, nil, now)

	active := e.Active()
	require.Len(t, active, 1)
	assert.Equal(t, "packet_loss", active[0].Rule)
	assert.Equal(t, "firing", active[0].State)

	// Recovered → resolve.
	good := stats.Snapshot{Sent: 20, Received: 20, LossPct: 0}
	e.Evaluate([]alerting.EdgeInput{{Src: "a", Dst: "b", Snap: good, UpdatedAt: now}}, nil, now)
	assert.Empty(t, e.Active())

	// Saw a firing then a resolved transition.
	require.GreaterOrEqual(t, len(changes), 2)
	assert.Equal(t, "firing", changes[0].State)
	assert.Equal(t, "resolved", changes[len(changes)-1].State)
}

func TestEngine_BlackoutOnStaleEdge(t *testing.T) {
	var changes []store.Alert
	e := newEngine(&changes)
	now := time.Now()
	old := now.Add(-1 * time.Minute) // older than BlackoutGrace default

	e.Evaluate([]alerting.EdgeInput{{Src: "a", Dst: "b", Snap: stats.Snapshot{Sent: 5, Received: 5}, UpdatedAt: old}}, nil, now)
	active := e.Active()
	require.Len(t, active, 1)
	assert.Equal(t, "blackout", active[0].Rule)
}

func TestEngine_AgentDown(t *testing.T) {
	var changes []store.Alert
	e := newEngine(&changes)
	now := time.Now()
	e.Evaluate(nil, []alerting.AgentInput{{Name: "a1", Stale: true}}, now)
	require.Len(t, e.Active(), 1)
	assert.Equal(t, "agent_down", e.Active()[0].Rule)
}

func TestEngine_SizeMismatch(t *testing.T) {
	var changes []store.Alert
	e := newEngine(&changes)
	now := time.Now()
	e.RecordSizeMismatch("a", "b")
	e.Evaluate(nil, nil, now)
	require.Len(t, e.Active(), 1)
	assert.Equal(t, "size_mismatch", e.Active()[0].Rule)
}

func TestEngine_SilencedSuppressesNotify(t *testing.T) {
	sil := alerting.NewSilences()
	sil.Add(alerting.Silence{Rule: "packet_loss", Until: time.Now().Add(time.Hour)})

	var notified bool
	notifier := notifyFunc(func(store.Alert, string) { notified = true })
	e := alerting.New(alerting.Config{LossPct: 20}, store.NopAlertSink{}, notifier, sil, nil)

	now := time.Now()
	// Received > 0 so this is packet_loss (not blackout), matching the silence.
	e.Evaluate([]alerting.EdgeInput{{Src: "a", Dst: "b", Snap: stats.Snapshot{Sent: 10, Received: 2, LossPct: 80}, UpdatedAt: now}}, nil, now)

	require.Len(t, e.Active(), 1)
	assert.Equal(t, "packet_loss", e.Active()[0].Rule)
	assert.True(t, e.Active()[0].Silenced)
	assert.False(t, notified, "silenced alert should not notify")
}

type notifyFunc func(store.Alert, string)

func (f notifyFunc) Notify(a store.Alert, event string) { f(a, event) }
