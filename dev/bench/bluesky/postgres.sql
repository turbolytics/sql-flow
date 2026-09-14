-- One row per minute per language. The primary key leads with bucket, so a
-- time-range scan uses it and no second index is needed.
--
-- The pipeline's upsert depends on this being a primary key. The DuckDB
-- postgres extension honours ON CONFLICT against a primary key and fails with a
-- binder error against a unique index alone.
CREATE TABLE IF NOT EXISTS posts_per_minute_by_lang (
  bucket     TIMESTAMPTZ NOT NULL,
  lang       TEXT        NOT NULL,
  posts      INTEGER     NOT NULL,
  -- Wall-clock time of the last write. bucket is event time. A reader needs
  -- both to tell "the stream is behind" from "the stream stopped".
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (bucket, lang)
);
