CREATE SCHEMA IF NOT EXISTS iceberg.monitoring;

CREATE TABLE IF NOT EXISTS iceberg.monitoring.server_metrics (
    ts TIMESTAMP(6),
    server_name VARCHAR,
    ip VARCHAR,
    cpu_util DOUBLE,
    ram_util DOUBLE,
    disk_util DOUBLE,
    io_stat DOUBLE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(ts)']
);
