CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE pipeline.prices (
    id INTEGER NOT NULL,
    value DECIMAL(5, 2) NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL
);

CREATE TABLE pipeline.monthly_price (
    system_id INTEGER NOT NULL,
    reading_year INTEGER NOT NULL,
    reading_month INTEGER NOT NULL,
    priced_power_sum DECIMAL(38, 5) NOT NULL
);
