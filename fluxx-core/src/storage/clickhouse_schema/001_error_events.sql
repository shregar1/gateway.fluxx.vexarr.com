-- Migration 001: error_events table + group/release rollup mat views.
-- Idempotent: every CREATE has IF NOT EXISTS so a re-run is a no-op.
-- Database is created if missing so a fresh CH instance bootstraps cleanly.

CREATE DATABASE IF NOT EXISTS fluxx;

CREATE TABLE IF NOT EXISTS fluxx.error_events (
  event_id        UUID,
  org_id          String,
  project_id      String,
  service         LowCardinality(String),
  ts              DateTime64(6, 'UTC'),

  fingerprint     FixedString(16),
  error_type      LowCardinality(String),
  message         String,

  release         LowCardinality(String),
  environment     LowCardinality(String) DEFAULT 'production',

  stack_raw       String CODEC(ZSTD(6)),
  frames_function Array(String),
  frames_file     Array(String),
  frames_line     Array(UInt32),
  frames_column   Array(UInt32),
  frames_resolved UInt8,

  session_id      String,
  user_id         String,
  url             String,
  user_agent      String,

  tags Map(LowCardinality(String), String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (org_id, project_id, fingerprint, ts)
TTL toDateTime(ts) + INTERVAL 90 DAY;

CREATE TABLE IF NOT EXISTS fluxx.error_groups (
  org_id        String,
  project_id    String,
  fingerprint   FixedString(16),
  error_type    SimpleAggregateFunction(any, LowCardinality(String)),
  message       SimpleAggregateFunction(any, String),
  first_seen    SimpleAggregateFunction(min, DateTime64(6, 'UTC')),
  last_seen     SimpleAggregateFunction(max, DateTime64(6, 'UTC')),
  count         SimpleAggregateFunction(sum, UInt64),
  services      AggregateFunction(groupUniqArray, LowCardinality(String)),
  first_release SimpleAggregateFunction(any, LowCardinality(String)),
  last_release  SimpleAggregateFunction(max, LowCardinality(String))
)
ENGINE = AggregatingMergeTree
ORDER BY (org_id, project_id, fingerprint);

CREATE TABLE IF NOT EXISTS fluxx.error_group_by_release (
  org_id      String,
  project_id  String,
  fingerprint FixedString(16),
  release     LowCardinality(String),
  first_seen  SimpleAggregateFunction(min, DateTime64(6, 'UTC')),
  last_seen   SimpleAggregateFunction(max, DateTime64(6, 'UTC')),
  count       SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (org_id, project_id, fingerprint, release);

CREATE MATERIALIZED VIEW IF NOT EXISTS fluxx.mv_error_groups
TO fluxx.error_groups AS
SELECT
  org_id,
  project_id,
  fingerprint,
  any(error_type)              AS error_type,
  any(message)                 AS message,
  min(ts)                      AS first_seen,
  max(ts)                      AS last_seen,
  count()                      AS count,
  groupUniqArrayState(service) AS services,
  any(release)                 AS first_release,
  max(release)                 AS last_release
FROM fluxx.error_events
GROUP BY org_id, project_id, fingerprint;

CREATE MATERIALIZED VIEW IF NOT EXISTS fluxx.mv_error_group_by_release
TO fluxx.error_group_by_release AS
SELECT
  org_id,
  project_id,
  fingerprint,
  release,
  min(ts) AS first_seen,
  max(ts) AS last_seen,
  count() AS count
FROM fluxx.error_events
GROUP BY org_id, project_id, fingerprint, release;
