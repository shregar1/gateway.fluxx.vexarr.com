-- Migration 002: first-class breadcrumbs + source-line context on
-- error_events.
--
-- v1 carried breadcrumbs as a JSON string stuffed into the
-- `tags Map(LowCardinality(String), String)` column, which works but
-- (a) abuses the tags map as an arbitrary blob channel and (b) makes
-- the column LowCardinality even though every value is unique.
-- Promote it to a proper String column with ZSTD compression. The
-- agent now writes both during a transition window; the read side
-- prefers the column.
--
-- `frames_context` holds the source line + a couple of lines on
-- either side for each resolved frame — Sentry's "snippet" feature.
-- We project it as `Array(String)` so it lines up positionally with
-- frames_function / frames_file / frames_line, with empty strings
-- where the sourcemap didn't supply `sourcesContent`.
--
-- Idempotent: ALTER … ADD COLUMN IF NOT EXISTS skips on re-run.

ALTER TABLE fluxx.error_events
  ADD COLUMN IF NOT EXISTS breadcrumbs    String CODEC(ZSTD(6))      DEFAULT '';

ALTER TABLE fluxx.error_events
  ADD COLUMN IF NOT EXISTS frames_context Array(String) CODEC(ZSTD(6)) DEFAULT [];
