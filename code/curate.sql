-- ==============================================================================
-- Lab 2 - Task 3: Deterministic Curation SQL Pipeline
-- Role C / Curation
-- Contract Version: research-telemetry-v1
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- Step 1: Pre-key Quarantine (Malformed, Invalid ID, or Unparseable Ingest Time)
-- Applies to all physical raw envelopes BEFORE deduplication.
-- Precedence: PARSE > KEY > INGEST_PARSE
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW prekey_quarantine_classified AS
SELECT 
    *,
    -- Rule: PARSE (Malformed JSON / unparseable raw envelope)
    (parse_ok = false) AS fail_parse,
    
    -- Rule: KEY (Missing, blank, or invalid format not matching pattern R[0-9]{6})
    (record_id IS NULL OR length(trim(record_id)) = 0 OR regexp_full_match(record_id, '^R[0-9]{6}$') = false) AS fail_key,
    
    -- Rule: INGEST_PARSE (Unparseable ingest/arrival timestamp)
    (ingest_ts IS NULL) AS fail_ingest_parse
FROM typed;

CREATE OR REPLACE VIEW prekey_quarantine AS
SELECT 
    source_object,
    source_row,
    source_sha256,
    record_id,
    sensor_id,
    event_time,
    ingest_time,
    reading,
    unit,
    operator_email,
    raw_payload,
    CASE 
        WHEN fail_parse THEN 'PARSE'
        WHEN fail_key THEN 'KEY'
        WHEN fail_ingest_parse THEN 'INGEST_PARSE'
    END AS primary_reason,
    concat_ws(';', 
        CASE WHEN fail_parse THEN 'PARSE' ELSE NULL END,
        CASE WHEN fail_key THEN 'KEY' ELSE NULL END,
        CASE WHEN fail_ingest_parse THEN 'INGEST_PARSE' ELSE NULL END
    ) AS all_reasons
FROM prekey_quarantine_classified
WHERE fail_parse OR fail_key OR fail_ingest_parse;

-- ------------------------------------------------------------------------------
-- Step 2: Deduplication on Key-Eligible Records
-- Select deterministic winner: largest ingest_time_utc DESC, then source_object ASC, source_row ASC
-- Non-selected versions enter duplicate ledger with reason SUPERSEDED.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW key_eligible AS
SELECT *
FROM prekey_quarantine_classified
WHERE NOT (fail_parse OR fail_key OR fail_ingest_parse);

CREATE OR REPLACE VIEW ranked_versions AS
SELECT 
    *,
    ROW_NUMBER() OVER (
        PARTITION BY record_id 
        ORDER BY ingest_ts DESC, source_object ASC, source_row ASC
    ) AS version_rank,
    FIRST_VALUE(source_object) OVER (
        PARTITION BY record_id 
        ORDER BY ingest_ts DESC, source_object ASC, source_row ASC
    ) AS winner_source_object,
    FIRST_VALUE(source_row) OVER (
        PARTITION BY record_id 
        ORDER BY ingest_ts DESC, source_object ASC, source_row ASC
    ) AS winner_source_row,
    FIRST_VALUE(source_sha256) OVER (
        PARTITION BY record_id 
        ORDER BY ingest_ts DESC, source_object ASC, source_row ASC
    ) AS winner_source_sha256
FROM key_eligible;

-- Duplicate ledger (superseded non-winner records)
CREATE OR REPLACE VIEW duplicates AS
SELECT 
    source_object,
    source_row,
    source_sha256,
    record_id,
    sensor_id,
    event_time,
    ingest_time,
    reading,
    unit,
    operator_email,
    winner_source_object,
    winner_source_row,
    winner_source_sha256,
    'SUPERSEDED' AS reason,
    raw_payload
FROM ranked_versions
WHERE version_rank > 1;

-- ------------------------------------------------------------------------------
-- Step 3: Validate Selected Winners (version_rank = 1)
-- Reason Precedence: REQUIRED > NUMERIC > UNIT > RANGE > TIME_PARSE > TIME_ORDER > REFERENCE
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW winners_raw AS
SELECT *
FROM ranked_versions
WHERE version_rank = 1;

CREATE OR REPLACE VIEW winners_classified AS
SELECT 
    w.*,
    
    -- 1. REQUIRED: Any mandatory field missing/empty (operator_email is optional)
    (
        w.record_id IS NULL OR length(trim(w.record_id)) = 0 OR
        w.sensor_id IS NULL OR length(trim(w.sensor_id)) = 0 OR
        w.event_time IS NULL OR length(trim(w.event_time)) = 0 OR
        w.ingest_time IS NULL OR length(trim(w.ingest_time)) = 0 OR
        w.reading IS NULL OR length(trim(w.reading)) = 0 OR
        w.unit IS NULL OR length(trim(w.unit)) = 0
    ) AS fail_required,
    
    -- 2. NUMERIC: Reading must be a finite numeric value
    (w.value_num IS NULL OR NOT isfinite(w.value_num)) AS fail_numeric,
    
    -- 3. UNIT: Only 'C' and 'F' are supported
    (w.unit IS NULL OR w.unit NOT IN ('C', 'F')) AS fail_unit,
    
    -- 4. RANGE: Converted temperature in Celsius must be in [-30, 60] °C
    (
        (w.value_num IS NOT NULL AND isfinite(w.value_num) AND w.unit IN ('C', 'F')) AND
        (
            CASE 
                WHEN w.unit = 'C' THEN w.value_num
                WHEN w.unit = 'F' THEN (w.value_num - 32.0) * 5.0 / 9.0
            END < -30.0
            OR
            CASE 
                WHEN w.unit = 'C' THEN w.value_num
                WHEN w.unit = 'F' THEN (w.value_num - 32.0) * 5.0 / 9.0
            END > 60.0
        )
    ) AS fail_range,
    
    -- 5. TIME_PARSE: event_time must be parseable ISO-8601 UTC
    (w.event_ts IS NULL) AS fail_time_parse,
    
    -- 6. TIME_ORDER: event_ts <= ingest_ts <= AS_OF (2026-02-09T00:00:00Z)
    (
        (w.event_ts IS NOT NULL AND w.ingest_ts IS NOT NULL) AND
        (
            w.event_ts > w.ingest_ts OR 
            w.ingest_ts > TIMESTAMP '2026-02-09 00:00:00' OR
            w.event_ts > TIMESTAMP '2026-02-09 00:00:00'
        )
    ) AS fail_time_order,
    
    -- 7. REFERENCE: sensor_id must exist in registry (sensors.csv)
    (reg.sensor_id IS NULL) AS fail_reference

FROM winners_raw w
LEFT JOIN registry reg ON w.sensor_id = reg.sensor_id;

-- ------------------------------------------------------------------------------
-- Step 4: Invalid Winners to Quarantine
-- If a newer winner is invalid, quarantine it (do not substitute older valid versions)
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW invalid_winners_quarantine AS
SELECT 
    wc.source_object,
    wc.source_row,
    wc.source_sha256,
    wc.record_id,
    wc.sensor_id,
    wc.event_time,
    wc.ingest_time,
    wc.reading,
    wc.unit,
    wc.operator_email,
    wc.raw_payload,
    CASE 
        WHEN fail_required THEN 'REQUIRED'
        WHEN fail_numeric THEN 'NUMERIC'
        WHEN fail_unit THEN 'UNIT'
        WHEN fail_range THEN 'RANGE'
        WHEN fail_time_parse THEN 'TIME_PARSE'
        WHEN fail_time_order THEN 'TIME_ORDER'
        WHEN fail_reference THEN 'REFERENCE'
    END AS primary_reason,
    concat_ws(';',
        CASE WHEN fail_required THEN 'REQUIRED' ELSE NULL END,
        CASE WHEN fail_numeric THEN 'NUMERIC' ELSE NULL END,
        CASE WHEN fail_unit THEN 'UNIT' ELSE NULL END,
        CASE WHEN fail_range THEN 'RANGE' ELSE NULL END,
        CASE WHEN fail_time_parse THEN 'TIME_PARSE' ELSE NULL END,
        CASE WHEN fail_time_order THEN 'TIME_ORDER' ELSE NULL END,
        CASE WHEN fail_reference THEN 'REFERENCE' ELSE NULL END
    ) AS all_reasons
FROM winners_classified wc
WHERE fail_required 
   OR fail_numeric 
   OR fail_unit 
   OR fail_range 
   OR fail_time_parse 
   OR fail_time_order 
   OR fail_reference;

-- Combined quarantine view (Pre-key + Invalid Winners)
CREATE OR REPLACE VIEW quarantine AS
SELECT * FROM prekey_quarantine
UNION ALL
SELECT * FROM invalid_winners_quarantine;

-- ------------------------------------------------------------------------------
-- Step 5: Curated Output (Valid Winners)
-- Join registry to attach site; compute temperature_c and is_late.
-- Explicit allowlist of 10 columns (operator_email and raw_payload excluded).
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW curated AS
SELECT 
    wc.record_id::VARCHAR AS record_id,
    wc.sensor_id::VARCHAR AS sensor_id,
    reg.site::VARCHAR AS site,
    wc.event_ts AS event_time_utc,
    wc.ingest_ts AS ingest_time_utc,
    round(
        CASE 
            WHEN wc.unit = 'C' THEN wc.value_num
            WHEN wc.unit = 'F' THEN (wc.value_num - 32.0) * 5.0 / 9.0
        END, 2
    )::DECIMAL(8,2) AS temperature_c,
    (epoch(wc.ingest_ts) - epoch(wc.event_ts) > 900)::BOOLEAN AS is_late,
    wc.source_object::VARCHAR AS source_object,
    wc.source_row::BIGINT AS source_row,
    wc.source_sha256::VARCHAR AS source_sha256
FROM winners_classified wc
JOIN registry reg ON wc.sensor_id = reg.sensor_id
WHERE NOT (
    fail_required 
    OR fail_numeric 
    OR fail_unit 
    OR fail_range 
    OR fail_time_parse 
    OR fail_time_order 
    OR fail_reference
);
