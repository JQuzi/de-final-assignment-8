CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.houses
(
    house_id UInt64,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    maintenance_year Nullable(Int32),
    square Nullable(Float64),
    population Nullable(Int32),
    region LowCardinality(Nullable(String)),
    locality_name Nullable(String),
    address Nullable(String),
    full_address Nullable(String),
    communal_service_id Nullable(Int64),
    description Nullable(String),
    decade Nullable(Int32),
    source_filename LowCardinality(String),
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY house_id;

