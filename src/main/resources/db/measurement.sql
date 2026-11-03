CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE pipeline.systems (
    id INTEGER NOT NULL,
    name VARCHAR NOT NULL
);

CREATE TABLE pipeline.readings (
    system_id INTEGER NOT NULL,
    read_on TIMESTAMP NOT NULL,
    power DECIMAL(10, 3) NOT NULL
);

CREATE TABLE pipeline.monthly_usage (
    system_id INTEGER NOT NULL,
    system_name VARCHAR NOT NULL,
    reading_year INTEGER NOT NULL,
    reading_month INTEGER NOT NULL,
    reading_count BIGINT NOT NULL,
    power_sum DECIMAL(24, 3) NOT NULL,
    power_average DECIMAL(24, 6) NOT NULL
);
