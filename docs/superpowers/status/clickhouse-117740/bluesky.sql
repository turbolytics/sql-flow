CREATE TABLE bluesky_posts (
    ts    DateTime,
    did   String,
    langs Array(String),
    text  String
) ENGINE = MergeTree ORDER BY ts;
