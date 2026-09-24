"""Checks the POC's sink against what the broker acknowledged.

    mqtt_poc_check.py <data dir>          loss check; exits 1 on any missing reading
    mqtt_poc_check.py --peak-mem <csv>    peak MiB per service from docker stats samples

The loss check passes when every (device_id, metric, seq) in acked.csv
appears in iot.readings at least once. Duplicates are reported, never failed:
the pipeline is at-least-once by design.
"""
import json
import sys

import duckdb


def loss(data):
    c = duckdb.connect()
    c.sql(f"ATTACH '{data}/iot.duckdb' AS iot (READ_ONLY)")
    c.sql(f"CREATE VIEW acked AS SELECT * FROM read_csv('{data}/acked.csv', header=true, columns={{'device_id':'VARCHAR','metric':'VARCHAR','seq':'BIGINT'}})")
    acked = c.sql("SELECT count(*) FROM acked").fetchone()[0]
    missing = c.sql("SELECT count(*) FROM acked a ANTI JOIN iot.readings r USING (device_id, metric, seq)").fetchone()[0]
    rows = c.sql("SELECT count(*) FROM iot.readings").fetchone()[0]
    distinct = c.sql("SELECT count(*) FROM (SELECT DISTINCT device_id, metric, seq FROM iot.readings)").fetchone()[0]
    print(json.dumps({"acked": acked, "missing": missing, "rows": rows, "duplicates": rows - distinct}))
    return 1 if missing else 0


def peak_mem(csv):
    # docker stats prints usage as "3.8MiB / 128MiB", in B, KiB, MiB or GiB
    # depending on size. Only this stack's containers count.
    rows = duckdb.sql(f"""
        WITH s AS (
          SELECT column1 AS service,
                 split_part(column2, ' / ', 1) AS used
          FROM read_csv('{csv}', header=false, columns={{'column0':'BIGINT','column1':'VARCHAR','column2':'VARCHAR'}})
          WHERE column1 LIKE 'sqlflow-mqtt-%'
        )
        SELECT regexp_replace(service, '^sqlflow-mqtt-(.*)-[0-9]+$', '\\1') AS service,
               round(max(CAST(regexp_extract(used, '^([0-9.]+)', 1) AS DOUBLE) *
                 CASE regexp_extract(used, '([KMG]?i?B)$', 1)
                   WHEN 'GiB' THEN 1024 WHEN 'MiB' THEN 1
                   WHEN 'KiB' THEN 1.0 / 1024 ELSE 1.0 / 1048576 END), 1) AS peak_mib
        FROM s GROUP BY 1 ORDER BY 1
    """).fetchall()
    print(json.dumps(dict(rows)))
    return 0


if __name__ == "__main__":
    if sys.argv[1] == "--peak-mem":
        sys.exit(peak_mem(sys.argv[2]))
    sys.exit(loss(sys.argv[1]))
