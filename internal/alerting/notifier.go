package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/hamba/logger/v2"
	lctx "github.com/hamba/logger/v2/ctx"
	"github.com/slothnetworks/artemis/internal/store"
)

// Notifier is notified when an alert transitions (firing or resolved).
type Notifier interface {
	Notify(a store.Alert, event string)
}

// LogNotifier logs alert transitions via the structured logger.
type LogNotifier struct {
	log *logger.Logger
}

// NewLogNotifier returns a LogNotifier.
func NewLogNotifier(log *logger.Logger) LogNotifier { return LogNotifier{log: log} }

// Notify implements Notifier.
func (n LogNotifier) Notify(a store.Alert, event string) {
	fields := []logger.Field{
		lctx.Str("event", event),
		lctx.Str("rule", a.Rule),
		lctx.Str("severity", a.Severity),
		lctx.Str("src", a.Src),
		lctx.Str("dst", a.Dst),
		lctx.Str("reason", a.Reason),
	}
	if event == "firing" {
		n.log.Warn("Alert firing", fields...)
		return
	}
	n.log.Info("Alert resolved", fields...)
}

// WebhookNotifier POSTs alert transitions as JSON to a URL.
type WebhookNotifier struct {
	url    string
	client *http.Client
	log    *logger.Logger
}

// NewWebhookNotifier returns a WebhookNotifier posting to url.
func NewWebhookNotifier(url string, log *logger.Logger) WebhookNotifier {
	return WebhookNotifier{url: url, client: &http.Client{Timeout: 10 * time.Second}, log: log}
}

// Notify implements Notifier.
func (n WebhookNotifier) Notify(a store.Alert, event string) {
	body, _ := json.Marshal(map[string]any{"event": event, "alert": a})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.url, bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.client.Do(req)
	if err != nil {
		n.log.Error("Webhook notify failed", lctx.Err(err))
		return
	}
	_ = resp.Body.Close()
}

// MultiNotifier fans a notification out to several notifiers.
type MultiNotifier []Notifier

// Notify implements Notifier.
func (m MultiNotifier) Notify(a store.Alert, event string) {
	for _, n := range m {
		n.Notify(a, event)
	}
}
