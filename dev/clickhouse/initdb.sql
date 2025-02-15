CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE IF NOT EXISTS test.user_actions (
    timestamp DateTime,
    user_id UInt64,
    action String,
    browser String
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id);
