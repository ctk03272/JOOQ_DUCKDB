CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE pipeline.capacity (
    date INTEGER,
    center_code VARCHAR,
    center_management_type VARCHAR,
    capacity_type VARCHAR,
    capacity INTEGER,
    overflow_not_allowed BOOLEAN,
    hardcap DOUBLE,
    revised_capacity INTEGER,
    reserved_capacity INTEGER,
    backlog_version DOUBLE,
    hardcap_type VARCHAR
);

CREATE TABLE pipeline.purchase_order (
    date VARCHAR,
    sku BIGINT,
    fc VARCHAR,
    type VARCHAR,
    quantity INTEGER,
    confirmedQuantity INTEGER,
    modelAllocatedQuantity INTEGER
);

CREATE TABLE pipeline.fc_shutdown (
    centerCode VARCHAR,
    shutdownDt VARCHAR
);

-- The caller supplies the already group-filtered file, or omits this input.
CREATE TABLE pipeline.fc_early_shuffling_config (
    fc VARCHAR,
    capacityType VARCHAR,
    earlyShufflingRatio DOUBLE,
    groupName VARCHAR
);

CREATE TABLE pipeline.capacity_result (
    date INTEGER,
    center_code VARCHAR,
    center_management_type VARCHAR,
    capacity_type VARCHAR,
    capacity INTEGER,
    overflow_not_allowed BOOLEAN,
    hardcap DOUBLE,
    revised_capacity INTEGER,
    reserved_capacity INTEGER,
    backlog_version DOUBLE,
    hardcap_type VARCHAR,
    old_capacity INTEGER,
    original_capacity INTEGER,
    original_revised_capacity INTEGER,
    original_reserved_capacity INTEGER,
    open_order INTEGER,
    model_allocated INTEGER,
    early_shuffling_reservation INTEGER,
    early_shuffling_ratio DOUBLE,
    blocked_fc BOOLEAN
);
