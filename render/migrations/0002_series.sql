-- Every series seen, and where a key turns back into dimensions. The rollup
-- tables carry dimensions_key and never jsonb, so the API joins here to
-- filter by dimension and to return dimensions.
CREATE TABLE series (
  name           TEXT        NOT NULL,
  type           TEXT        NOT NULL,
  dimensions_key TEXT        NOT NULL,
  -- dimensions_key is JSON text, so the cast is the whole conversion.
  dimensions     JSONB       NOT NULL,
  first_bucket   TIMESTAMPTZ NOT NULL,
  last_bucket    TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (name, type, dimensions_key)
);

-- Kept in the writer's transaction, as the rollups are. least and greatest
-- because a republished minute may be older than what is recorded.
CREATE FUNCTION metrics_series() RETURNS trigger
LANGUAGE plpgsql AS $fn$
BEGIN
  INSERT INTO series (name, type, dimensions_key, dimensions, first_bucket, last_bucket)
  SELECT name, type, dimensions_key, dimensions_key::jsonb, min(bucket), max(bucket)
  FROM changed
  GROUP BY name, type, dimensions_key
  ON CONFLICT (name, type, dimensions_key) DO UPDATE SET
    first_bucket = least(series.first_bucket, excluded.first_bucket),
    last_bucket  = greatest(series.last_bucket, excluded.last_bucket);
  RETURN NULL;
END $fn$;

-- Two triggers: Postgres refuses a transition table on a trigger with more
-- than one event. An upsert fires both, each with its own rows.
CREATE TRIGGER metrics_series_ins AFTER INSERT ON metrics_1m
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION metrics_series();
CREATE TRIGGER metrics_series_upd AFTER UPDATE ON metrics_1m
  REFERENCING NEW TABLE AS changed
  FOR EACH STATEMENT EXECUTE FUNCTION metrics_series();
