WITH want AS (
  SELECT date_bin(INTERVAL '15 minutes', f."bucket", TIMESTAMPTZ '2000-01-01 00:00:00+00') AS "bucket", f."lang" AS "lang", sum(f."posts")::bigint AS "posts"
  FROM "posts_by_lang_5m" AS f
  WHERE f."bucket" >= $1 AND f."bucket" < $2
  GROUP BY 1, 2
), got AS (
  SELECT "bucket", "lang", "posts" FROM "posts_by_lang_15m" WHERE "bucket" >= $1 AND "bucket" < $2
), pairs AS (
  SELECT "bucket", "lang", bool_or("~side" = 'w') AS "~in_want", bool_or("~side" = 'g') AS "~in_got", (array_agg("posts") FILTER (WHERE "~side" = 'w'))[1] AS "w:posts", (array_agg("posts") FILTER (WHERE "~side" = 'g'))[1] AS "g:posts"
  FROM (SELECT 'w' AS "~side", "bucket", "lang", "posts" FROM want UNION ALL SELECT 'g', "bucket", "lang", "posts" FROM got) AS u
  GROUP BY "bucket", "lang"
), diff AS (
  SELECT "bucket" AS bucket_at, concat_ws(', ', 'lang=' || coalesce("lang"::text, 'null')) AS key,
         CASE WHEN NOT "~in_got" THEN 'missing' WHEN NOT "~in_want" THEN 'extra' ELSE 'differs' END AS kind,
         CASE WHEN "w:posts" IS DISTINCT FROM "g:posts" THEN 'posts' END AS measure,
         CASE WHEN "w:posts" IS DISTINCT FROM "g:posts" THEN "g:posts"::text END AS stored,
         CASE WHEN "w:posts" IS DISTINCT FROM "g:posts" THEN "w:posts"::text END AS recomputed
  FROM pairs
  WHERE NOT "~in_want" OR NOT "~in_got" OR "w:posts" IS DISTINCT FROM "g:posts"
)
SELECT (SELECT count(DISTINCT "bucket") FROM pairs),
       (SELECT count(DISTINCT bucket_at) FROM diff),
       coalesce((SELECT jsonb_agg(s ORDER BY s.bucket_at, s.key)
                 FROM (SELECT * FROM diff ORDER BY bucket_at, key LIMIT 10) AS s), '[]'::jsonb)
