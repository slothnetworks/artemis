CREATE TABLE IF NOT EXISTS samples (
    ts            DateTime64(3),
    src           String,
    dst           String,
    protocol      String,
    sent          UInt64,
    received      UInt64,
    dropped       UInt64,
    timeout       UInt64,
    loss_pct      Float64,
    min_ns        Int64,
    avg_ns        Int64,
    p50_ns        Int64,
    p95_ns        Int64,
    p99_ns        Int64,
    max_ns        Int64,
    jitter_ns     Int64,
    bytes         UInt64,
    throughput    Float64,
    path_mtu      Int32,
    effective_mss Int32,
    resolution_ns Int64
)
ENGINE = MergeTree
PARTITION BY toDate(ts)
ORDER BY (src, dst, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY
