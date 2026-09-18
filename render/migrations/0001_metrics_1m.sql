-- One row per minute per series. A series is a name, a type and a set of
-- dimensions, and dimensions_key is that set in canonical form: keys sorted,
-- every value a string, compact JSON. The pipeline builds it.
--
-- The five values are what every coarser grain can be merged from exactly:
-- sums add, min and max nest, and last is the latest bucket's. An average is
-- value_sum / value_count at any grain. An average stored per minute could
-- not be merged: a minute of one sample would weigh as much as one of 1,000.
CREATE TABLE metrics_1m (
  bucket         TIMESTAMPTZ      NOT NULL,
  name           TEXT             NOT NULL,
  type           TEXT             NOT NULL CHECK (type IN ('count', 'gauge')),
  dimensions_key TEXT             NOT NULL,
  value_sum      DOUBLE PRECISION NOT NULL,
  value_count    BIGINT           NOT NULL,
  value_min      DOUBLE PRECISION NOT NULL,
  value_max      DOUBLE PRECISION NOT NULL,
  value_last     DOUBLE PRECISION NOT NULL,
  -- The event time of value_last.
  last_at        TIMESTAMPTZ      NOT NULL,
  -- The Postgres clock at the first write. A republished minute keeps it.
  updated_at     TIMESTAMPTZ      NOT NULL DEFAULT now(),
  PRIMARY KEY (bucket, name, type, dimensions_key)
);

-- The API reads one name over a range.
CREATE INDEX metrics_1m_name_bucket ON metrics_1m (name, bucket);
