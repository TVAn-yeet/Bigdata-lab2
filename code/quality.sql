-- T2 quality populations and SQL-derived measurements.
-- Run after typed.sql and after creating contract_parameters and curated_candidate.

CREATE OR REPLACE TEMP VIEW normalized_registry AS
SELECT DISTINCT
    upper(trim(sensor_id)) AS sensor_id,
    site
FROM registry
WHERE nullif(trim(sensor_id), '') IS NOT NULL;

CREATE OR REPLACE TEMP VIEW intake_required_flags AS
SELECT
    t.*,
    (
        nullif(trim(record_id), '') IS NOT NULL
        AND nullif(trim(sensor_id), '') IS NOT NULL
        AND nullif(trim(event_time), '') IS NOT NULL
        AND nullif(trim(ingest_time), '') IS NOT NULL
        AND nullif(trim(reading), '') IS NOT NULL
        AND nullif(trim(unit), '') IS NOT NULL
    ) AS required_complete
FROM typed AS t
WHERE coalesce(parse_ok, false);

-- Key eligibility is deliberately decided before value/time validity.
-- Parsed records with an invalid ID or arrival time remain visible in key_rejected.
CREATE OR REPLACE TEMP VIEW key_rejected AS
SELECT t.*
FROM typed AS t
CROSS JOIN contract_parameters AS p
WHERE coalesce(t.parse_ok, false)
  AND (
      NOT coalesce(regexp_full_match(t.record_id, p.record_id_pattern), false)
      OR t.ingest_ts IS NULL
  );

CREATE OR REPLACE TEMP VIEW key_eligible AS
SELECT t.*
FROM typed AS t
CROSS JOIN contract_parameters AS p
WHERE coalesce(t.parse_ok, false)
  AND coalesce(regexp_full_match(t.record_id, p.record_id_pattern), false)
  AND t.ingest_ts IS NOT NULL;

-- The winner population W is fixed before row-validity checks.
CREATE OR REPLACE TEMP VIEW ranked_key_eligible AS
SELECT
    k.*,
    row_number() OVER (
        PARTITION BY record_id
        ORDER BY ingest_ts DESC NULLS LAST, source_object ASC, source_row ASC
    ) AS winner_rank
FROM key_eligible AS k;

CREATE OR REPLACE TEMP VIEW winners AS
SELECT * EXCLUDE (winner_rank)
FROM ranked_key_eligible
WHERE winner_rank = 1;

CREATE OR REPLACE TEMP VIEW duplicate_versions AS
SELECT * EXCLUDE (winner_rank)
FROM ranked_key_eligible
WHERE winner_rank > 1;

CREATE OR REPLACE TEMP VIEW quality_before_flags AS
WITH converted AS (
    SELECT
        w.*,
        (
            nullif(trim(record_id), '') IS NOT NULL
            AND nullif(trim(sensor_id), '') IS NOT NULL
            AND nullif(trim(event_time), '') IS NOT NULL
            AND nullif(trim(ingest_time), '') IS NOT NULL
            AND nullif(trim(reading), '') IS NOT NULL
            AND nullif(trim(unit), '') IS NOT NULL
        ) AS required_complete,
        CASE
            WHEN unit = p.celsius_unit THEN value_num
            WHEN unit = p.fahrenheit_unit THEN (value_num - 32.0) * 5.0 / 9.0
            ELSE NULL
        END AS temperature_c_calculated,
        CASE
            WHEN event_ts IS NOT NULL AND ingest_ts IS NOT NULL
                THEN date_diff('second', event_ts, ingest_ts)
            ELSE NULL
        END AS lag_seconds,
        p.as_of_utc,
        p.min_temperature_c,
        p.max_temperature_c,
        p.late_threshold_seconds,
        p.celsius_unit,
        p.fahrenheit_unit
    FROM winners AS w
    CROSS JOIN contract_parameters AS p
), flagged AS (
    SELECT
        c.*,
        coalesce(value_num IS NOT NULL AND isfinite(value_num), false) AS finite_numeric,
        coalesce(unit IN (celsius_unit, fahrenheit_unit), false) AS unit_supported,
        EXISTS (
            SELECT 1
            FROM normalized_registry AS r
            WHERE r.sensor_id = c.sensor_id
        ) AS sensor_known,
        coalesce(
            event_ts IS NOT NULL
            AND ingest_ts IS NOT NULL
            AND event_ts <= ingest_ts
            AND ingest_ts <= as_of_utc,
            false
        ) AS chronology_valid
    FROM converted AS c
)
SELECT
    f.*,
    coalesce(
        finite_numeric
        AND unit_supported
        AND temperature_c_calculated IS NOT NULL
        AND isfinite(temperature_c_calculated)
        AND temperature_c_calculated BETWEEN min_temperature_c AND max_temperature_c,
        false
    ) AS value_valid,
    coalesce(chronology_valid AND lag_seconds <= late_threshold_seconds, false) AS timely,
    coalesce(chronology_valid AND lag_seconds > late_threshold_seconds, false) AS is_late
FROM flagged AS f;

CREATE OR REPLACE TEMP VIEW quality_before_counts AS
SELECT
    (SELECT count(*) FROM raw_envelopes)::BIGINT AS raw_records,
    (SELECT count(*) FROM typed WHERE NOT coalesce(parse_ok, false))::BIGINT AS parse_failures,
    (SELECT count(*) FROM intake_required_flags)::BIGINT AS parsed_intake_records,
    (SELECT count(*) FROM key_rejected)::BIGINT AS key_rejected_records,
    (SELECT count(*) FROM key_eligible)::BIGINT AS key_eligible_records,
    (SELECT count(*) FROM winners)::BIGINT AS winners_before_validity,
    (SELECT count(*) FROM duplicate_versions)::BIGINT AS duplicate_excess,
    (
        SELECT count(*)
        FROM intake_required_flags
        WHERE NOT coalesce(required_complete, false)
    )::BIGINT AS parsed_intake_required_failures,
    (
        SELECT count(*)
        FROM quality_before_flags
        WHERE is_late
    )::BIGINT AS late_winners_before_curation;

CREATE OR REPLACE TEMP VIEW quality_before_metrics AS
WITH metric_rows AS (
    SELECT
        'Q_REQUIRED_COMPLETENESS' AS rule_id,
        'Required-field completeness' AS metric_name,
        'Deterministic winners W before row-validity filtering' AS cohort,
        count(*) FILTER (WHERE required_complete)::BIGINT AS numerator,
        count(*)::BIGINT AS denominator,
        (SELECT required_completeness_threshold FROM contract_parameters)::DOUBLE AS threshold,
        'BLOCK' AS severity,
        'complete_rows' AS measure
    FROM quality_before_flags

    UNION ALL
    SELECT
        'Q_INTAKE_REQUIRED_FAILURES',
        'Required-field failures in parsed intake',
        'Parsed intake rows; before key filtering',
        count(*) FILTER (WHERE NOT coalesce(required_complete, false))::BIGINT,
        count(*)::BIGINT,
        NULL::DOUBLE,
        'INFO',
        'failure_rows'
    FROM intake_required_flags

    UNION ALL
    SELECT
        'Q_BUSINESS_KEY_UNIQUENESS',
        'Business-key uniqueness before deduplication',
        'Key-eligible records before deduplication',
        count(DISTINCT record_id)::BIGINT,
        count(*)::BIGINT,
        (SELECT business_key_uniqueness_threshold FROM contract_parameters)::DOUBLE,
        'BLOCK',
        'distinct_record_ids'
    FROM key_eligible

    UNION ALL
    SELECT
        'Q_VALUE_VALIDITY',
        'Finite, supported, in-range temperature values',
        'Deterministic winners W before row-validity filtering',
        count(*) FILTER (WHERE value_valid)::BIGINT,
        count(*)::BIGINT,
        (SELECT value_validity_threshold FROM contract_parameters)::DOUBLE,
        'BLOCK',
        'valid_values'
    FROM quality_before_flags

    UNION ALL
    SELECT
        'Q_REFERENTIAL_CONSISTENCY',
        'Sensor registry consistency',
        'Deterministic winners W before row-validity filtering',
        count(*) FILTER (WHERE sensor_known)::BIGINT,
        count(*)::BIGINT,
        (SELECT sensor_consistency_threshold FROM contract_parameters)::DOUBLE,
        'BLOCK',
        'known_sensor_rows'
    FROM quality_before_flags

    UNION ALL
    SELECT
        'Q_TEMPORAL_CONSISTENCY',
        'Timestamp parsing and chronology',
        'Deterministic winners W before row-validity filtering',
        count(*) FILTER (WHERE chronology_valid)::BIGINT,
        count(*)::BIGINT,
        (SELECT chronology_threshold FROM contract_parameters)::DOUBLE,
        'BLOCK',
        'chronologically_valid_rows'
    FROM quality_before_flags

    UNION ALL
    SELECT
        'Q_TIMELINESS',
        'Arrival within the contract lag threshold',
        'Chronologically valid winners only',
        count(*) FILTER (WHERE timely)::BIGINT,
        count(*)::BIGINT,
        (SELECT timeliness_threshold FROM contract_parameters)::DOUBLE,
        'BLOCK',
        'timely_rows'
    FROM quality_before_flags
    WHERE chronology_valid
)
SELECT
    metric_rows.*,
    CASE
        WHEN denominator = 0 THEN NULL
        ELSE numerator::DOUBLE / denominator
    END AS value
FROM metric_rows;

-- Diagnostic rows intentionally contain provenance references, never raw_payload or email.
CREATE OR REPLACE TEMP VIEW quality_defect_rows AS
SELECT
    'PARSE_FAILURE' AS defect_code,
    'Parsed intake' AS population,
    source_object,
    source_row,
    record_id,
    sensor_id
FROM typed
WHERE NOT coalesce(parse_ok, false)

UNION ALL
SELECT
    'INVALID_RECORD_ID', 'Parsed intake', t.source_object, t.source_row, t.record_id, t.sensor_id
FROM typed AS t
CROSS JOIN contract_parameters AS p
WHERE coalesce(t.parse_ok, false)
  AND NOT coalesce(regexp_full_match(t.record_id, p.record_id_pattern), false)

UNION ALL
SELECT
    'INGEST_TIME_PARSE', 'Parsed intake', t.source_object, t.source_row, t.record_id, t.sensor_id
FROM typed AS t
CROSS JOIN contract_parameters AS p
WHERE coalesce(t.parse_ok, false)
  AND coalesce(regexp_full_match(t.record_id, p.record_id_pattern), false)
  AND t.ingest_ts IS NULL

UNION ALL
SELECT
    'DUPLICATE_VERSION', 'Key-eligible records before deduplication',
    source_object, source_row, record_id, sensor_id
FROM duplicate_versions

UNION ALL
SELECT
    'REQUIRED_FIELD_MISSING', 'Deterministic winners W',
    source_object, source_row, record_id, sensor_id
FROM quality_before_flags
WHERE NOT coalesce(required_complete, false)

UNION ALL
SELECT
    'NUMERIC_INVALID', 'Deterministic winners W',
    source_object, source_row, record_id, sensor_id
FROM quality_before_flags
WHERE NOT finite_numeric

UNION ALL
SELECT
    'UNIT_UNSUPPORTED', 'Deterministic winners W',
    source_object, source_row, record_id, sensor_id
FROM quality_before_flags
WHERE NOT unit_supported

UNION ALL
SELECT
    'VALUE_OUT_OF_RANGE', 'Deterministic winners W',
    source_object, source_row, record_id, sensor_id
FROM quality_before_flags
WHERE finite_numeric
  AND unit_supported
  AND temperature_c_calculated IS NOT NULL
  AND isfinite(temperature_c_calculated)
  AND NOT (temperature_c_calculated BETWEEN min_temperature_c AND max_temperature_c)

UNION ALL
SELECT
    'EVENT_TIME_PARSE', 'Deterministic winners W',
    source_object, source_row, record_id, sensor_id
FROM quality_before_flags
WHERE event_ts IS NULL

UNION ALL
SELECT
    'TIME_ORDER', 'Deterministic winners W',
    source_object, source_row, record_id, sensor_id
FROM quality_before_flags
WHERE event_ts IS NOT NULL
  AND (
      event_ts > ingest_ts
      OR ingest_ts > as_of_utc
  )

UNION ALL
SELECT
    'UNKNOWN_SENSOR', 'Deterministic winners W',
    source_object, source_row, record_id, sensor_id
FROM quality_before_flags
WHERE NOT sensor_known

UNION ALL
SELECT
    'LATE_RETAINED', 'Chronologically valid winners W',
    source_object, source_row, record_id, sensor_id
FROM quality_before_flags
WHERE is_late;

CREATE OR REPLACE TEMP VIEW quality_defect_counts AS
SELECT defect_code, count(*)::BIGINT AS defect_count
FROM quality_defect_rows
GROUP BY defect_code;

CREATE OR REPLACE TEMP VIEW quality_defect_samples AS
WITH ranked_samples AS (
    SELECT
        d.*,
        c.defect_count,
        row_number() OVER (
            PARTITION BY d.defect_code
            ORDER BY d.source_object ASC, d.source_row ASC
        ) AS sample_number
    FROM quality_defect_rows AS d
    JOIN quality_defect_counts AS c USING (defect_code)
)
SELECT
    defect_code,
    population,
    defect_count,
    sample_number,
    source_object,
    source_row,
    record_id,
    sensor_id
FROM ranked_samples
WHERE sample_number <= 5;

-- The runner creates curated_candidate from the T3 Parquet artifact, or as an empty view
-- when only the T2 before-curation checkpoint is being measured.
CREATE OR REPLACE TEMP VIEW quality_after_flags AS
WITH converted AS (
    SELECT
        upper(trim(c.record_id)) AS record_id,
        upper(trim(c.sensor_id)) AS sensor_id,
        try_cast(c.event_time_utc AS TIMESTAMP) AS event_ts,
        try_cast(c.ingest_time_utc AS TIMESTAMP) AS ingest_ts,
        try_cast(c.temperature_c AS DOUBLE) AS temperature_c,
        c.source_object,
        c.source_row,
        c.source_sha256,
        (
            nullif(trim(c.record_id), '') IS NOT NULL
            AND nullif(trim(c.sensor_id), '') IS NOT NULL
            AND c.event_time_utc IS NOT NULL
            AND c.ingest_time_utc IS NOT NULL
            AND c.temperature_c IS NOT NULL
        ) AS required_complete,
        p.as_of_utc,
        p.min_temperature_c,
        p.max_temperature_c,
        p.late_threshold_seconds
    FROM curated_candidate AS c
    CROSS JOIN contract_parameters AS p
), flagged AS (
    SELECT
        c.*,
        coalesce(temperature_c IS NOT NULL AND isfinite(temperature_c), false)
            AS finite_numeric,
        EXISTS (
            SELECT 1
            FROM normalized_registry AS r
            WHERE r.sensor_id = c.sensor_id
        ) AS sensor_known,
        coalesce(
            event_ts IS NOT NULL
            AND ingest_ts IS NOT NULL
            AND event_ts <= ingest_ts
            AND ingest_ts <= as_of_utc,
            false
        ) AS chronology_valid,
        CASE
            WHEN event_ts IS NOT NULL AND ingest_ts IS NOT NULL
                THEN date_diff('second', event_ts, ingest_ts)
            ELSE NULL
        END AS lag_seconds
    FROM converted AS c
)
SELECT
    f.*,
    coalesce(
        finite_numeric
        AND temperature_c BETWEEN min_temperature_c AND max_temperature_c,
        false
    ) AS value_valid,
    coalesce(chronology_valid AND lag_seconds <= late_threshold_seconds, false) AS timely,
    coalesce(chronology_valid AND lag_seconds > late_threshold_seconds, false) AS is_late
FROM flagged AS f;

CREATE OR REPLACE TEMP VIEW quality_reference_matches AS
SELECT
    upper(trim(q.record_id)) AS record_id,
    bool_and(coalesce(abs(c.temperature_c - q.reference_c) <= 0.05, false)) AS agrees
FROM qa_reference AS q
JOIN quality_after_flags AS c
  ON upper(trim(q.record_id)) = c.record_id
GROUP BY upper(trim(q.record_id));

CREATE OR REPLACE TEMP VIEW quality_after_metrics AS
WITH metric_rows AS (
    SELECT
        'Q_REQUIRED_COMPLETENESS' AS rule_id,
        'Required-field completeness' AS metric_name,
        'Curated candidate; source-required fields represented by released columns' AS cohort,
        count(*) FILTER (WHERE required_complete)::BIGINT AS numerator,
        count(*)::BIGINT AS denominator,
        (SELECT required_completeness_threshold FROM contract_parameters)::DOUBLE AS threshold,
        'BLOCK' AS severity,
        'complete_rows' AS measure
    FROM quality_after_flags

    UNION ALL
    SELECT
        'Q_BUSINESS_KEY_UNIQUENESS',
        'Business-key uniqueness after curation',
        'All curated candidate records',
        count(DISTINCT record_id)::BIGINT,
        count(*)::BIGINT,
        (SELECT business_key_uniqueness_threshold FROM contract_parameters)::DOUBLE,
        'BLOCK',
        'distinct_record_ids'
    FROM quality_after_flags

    UNION ALL
    SELECT
        'Q_VALUE_VALIDITY',
        'Finite, in-range Celsius temperature values',
        'All curated candidate records',
        count(*) FILTER (WHERE value_valid)::BIGINT,
        count(*)::BIGINT,
        (SELECT value_validity_threshold FROM contract_parameters)::DOUBLE,
        'BLOCK',
        'valid_values'
    FROM quality_after_flags

    UNION ALL
    SELECT
        'Q_REFERENTIAL_CONSISTENCY',
        'Sensor registry consistency',
        'All curated candidate records',
        count(*) FILTER (WHERE sensor_known)::BIGINT,
        count(*)::BIGINT,
        (SELECT sensor_consistency_threshold FROM contract_parameters)::DOUBLE,
        'BLOCK',
        'known_sensor_rows'
    FROM quality_after_flags

    UNION ALL
    SELECT
        'Q_TEMPORAL_CONSISTENCY',
        'Timestamp parsing and chronology',
        'All curated candidate records',
        count(*) FILTER (WHERE chronology_valid)::BIGINT,
        count(*)::BIGINT,
        (SELECT chronology_threshold FROM contract_parameters)::DOUBLE,
        'BLOCK',
        'chronologically_valid_rows'
    FROM quality_after_flags

    UNION ALL
    SELECT
        'Q_TIMELINESS',
        'Arrival within the contract lag threshold',
        'Chronologically valid curated rows only',
        count(*) FILTER (WHERE timely)::BIGINT,
        count(*)::BIGINT,
        (SELECT timeliness_threshold FROM contract_parameters)::DOUBLE,
        'BLOCK',
        'timely_rows'
    FROM quality_after_flags
    WHERE chronology_valid

    UNION ALL
    SELECT
        'Q_REFERENCE_AGREEMENT',
        'Reference sample agreement within 0.05 C',
        'Matched record IDs in the curated candidate and 100-row QA sample',
        count(*) FILTER (WHERE agrees)::BIGINT,
        count(*)::BIGINT,
        (SELECT qa_agreement_threshold FROM contract_parameters)::DOUBLE,
        'BLOCK',
        'agreeing_matched_ids'
    FROM quality_reference_matches

    UNION ALL
    SELECT
        'Q_REFERENCE_COVERAGE',
        'Reference sample coverage',
        'Matched QA IDs divided by all IDs in qa_reference.csv',
        (SELECT count(*) FROM quality_reference_matches)::BIGINT,
        (SELECT count(DISTINCT upper(trim(record_id))) FROM qa_reference)::BIGINT,
        (SELECT qa_coverage_threshold FROM contract_parameters)::DOUBLE,
        'BLOCK',
        'matched_reference_ids'
    FROM contract_parameters AS p
)
SELECT
    metric_rows.*,
    CASE
        WHEN denominator = 0 THEN NULL
        ELSE numerator::DOUBLE / denominator
    END AS value
FROM metric_rows;
