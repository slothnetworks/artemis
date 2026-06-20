CREATE TABLE IF NOT EXISTS events (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL,
    src         TEXT        NOT NULL,
    dst         TEXT        NOT NULL,
    protocol    TEXT,
    kind        TEXT        NOT NULL,
    probe_id    BIGINT,
    sent_bytes  INTEGER,
    recv_bytes  INTEGER,
    checksum_ok BOOLEAN,
    direction   TEXT,
    alert_id    BIGINT
);

CREATE INDEX IF NOT EXISTS events_edge_ts_idx ON events (src, dst, ts);

CREATE INDEX IF NOT EXISTS events_kind_ts_idx ON events (kind, ts)
