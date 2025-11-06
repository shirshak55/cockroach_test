-- ==========================================================
-- Complete benchmark schema (single CREATE TABLE)
-- ==========================================================

CREATE TABLE IF NOT EXISTS bench_kv (
    k INT8 PRIMARY KEY,
    v STRING NOT NULL,
    v2 STRING NOT NULL DEFAULT '',
    details JSONB NOT NULL DEFAULT '{}',
    qty INT8 NOT NULL DEFAULT 0,
    amount FLOAT8 NOT NULL DEFAULT 0.0,
    active BOOL NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ==========================================================
-- Indexes for benchmarking read/write tradeoffs
-- ==========================================================

-- Use hash-sharded indexes to avoid hotspots on sequential created_at writes.
-- The default bucket_count is fine for most clusters; explicitly set to 16 here for clarity.
CREATE INDEX IF NOT EXISTS bench_kv_created_at_idx 
    ON bench_kv (created_at DESC) USING HASH WITH (bucket_count = 16);

CREATE INDEX IF NOT EXISTS bench_kv_active_created_idx 
    ON bench_kv (active, created_at DESC) USING HASH WITH (bucket_count = 16);

CREATE INDEX IF NOT EXISTS bench_kv_qty_idx 
    ON bench_kv (qty);

CREATE INVERTED INDEX IF NOT EXISTS bench_kv_details_inverted_idx 
    ON bench_kv (details);