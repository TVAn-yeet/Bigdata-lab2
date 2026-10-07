-- Supplied normalization; curation and quality SQL are student work.

-- DuckDB 1.4 LTS; the patch build is pinned in the prepared image.

SET TimeZone='UTC';

SET threads=2;

SET memory_limit='512MB';

CREATE OR REPLACE TABLE raw_envelopes AS
SELECT *
FROM read_csv(
    'envelopes.csv',
    header=true,
    all_varchar=true
);

CREATE OR REPLACE VIEW typed AS

WITH normalized AS (

    SELECT
        * EXCLUDE(
            record_id,
            sensor_id,
            unit,
            source_row,
            parse_ok
        ),

        upper(trim(record_id)) AS record_id,

        upper(trim(sensor_id)) AS sensor_id,

        upper(trim(unit)) AS unit,

        cast(source_row AS BIGINT) AS source_row,

        cast(parse_ok AS BOOLEAN) AS parse_ok

    FROM raw_envelopes
)

SELECT
    *,

    try_cast(reading AS DOUBLE) AS value_num,

    try_strptime(
        event_time,
        '%Y-%m-%dT%H:%M:%SZ'
    ) AS event_ts,

    try_strptime(
        ingest_time,
        '%Y-%m-%dT%H:%M:%SZ'
    ) AS ingest_ts

FROM normalized;

CREATE OR REPLACE TABLE registry AS

SELECT *
FROM read_csv(
    'snapshot/sensors.csv',
    all_varchar=true
);

CREATE OR REPLACE TABLE qa_reference AS

SELECT
    record_id,
    cast(reference_c AS DECIMAL(8,2)) AS reference_c

FROM read_csv(
    'snapshot/qa_reference.csv',
    all_varchar=true
);
