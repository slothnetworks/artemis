CREATE TABLE IF NOT EXISTS alerts (
    id            TEXT PRIMARY KEY,
    src           TEXT NOT NULL,
    dst           TEXT,
    rule          TEXT NOT NULL,
    severity      TEXT,
    state         TEXT NOT NULL,
    reason        TEXT,
    silenced      BOOLEAN DEFAULT FALSE,
    started_at    TIMESTAMPTZ,
    resolved_at   TIMESTAMPTZ,
    last_event_at TIMESTAMPTZ,
    event_count   INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS alerts_state_idx ON alerts (state);

CREATE INDEX IF NOT EXISTS alerts_edge_idx ON alerts (src, dst)
