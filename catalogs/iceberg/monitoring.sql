CREATE SCHEMA IF NOT EXISTS iceberg.monitoring;

-- 1. Bảng Log Thô (Fact Table - Raw Layer)
CREATE TABLE IF NOT EXISTS iceberg.monitoring.raw_sftp_table (
    ts TIMESTAMP(6),
    server_id INT,
    cpu_util DOUBLE,
    ram_util DOUBLE,
    disk_util DOUBLE,
    io_stat DOUBLE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(ts)']
);

-- 2. Bảng Cấu Hình Server (Dim Table - Silver Layer)
CREATE TABLE IF NOT EXISTS iceberg.monitoring.server_config (
    server_id INT,
    server_name VARCHAR,
    ip VARCHAR,
    province VARCHAR,
    station VARCHAR
)
WITH (
    format = 'PARQUET'
);

-- 3. Bảng KPI Tổng Hợp (Gold Table - Gold Layer)
CREATE TABLE IF NOT EXISTS iceberg.monitoring.kpi_summary (
    window_start TIMESTAMP(6),
    window_end TIMESTAMP(6),
    province VARCHAR,
    station VARCHAR,
    avg_cpu_util DOUBLE,
    max_ram_util DOUBLE,
    avg_disk_util DOUBLE,
    avg_io_stat DOUBLE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(window_start)']
);
