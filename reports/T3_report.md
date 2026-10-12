# T3 — Deterministic Curation & Reconciliation

## C01 — Conservation & Physical-Key Partition

PASS.

The pipeline reconciles 100% of the physical intake into a pairwise-disjoint set partition across three terminal dispositions without loss or invented records:

- Total raw intake envelopes: **10,205**
- Curated candidate records: **9,610**
- Quarantine records (pre-key + invalid winners): **395**
- Duplicate ledger records (superseded non-winners): **200**
- Sum of terminal outputs: **10,205** (Delta: **0**)
- Physical ID overlap (Curated ∩ Quarantine ∩ Duplicates): **0**
- Curated business key (`record_id`) uniqueness: **100%** (0 duplicates)

### Quarantine Breakdown by Primary Reason:
- `REQUIRED`: 110
- `NUMERIC`: 60
- `UNIT`: 40
- `RANGE`: 40
- `TIME_PARSE`: 40
- `TIME_ORDER`: 30
- `REFERENCE`: 30
- `INGEST_PARSE`: 20
- `KEY`: 20
- `PARSE`: 5

## C02 — Determinism & Replay Equivalence

PASS.

The curation pipeline was executed with randomized row ordering and reversed object enumeration. The resulting logical projections and canonical content digests are identical across executions:

- Curated canonical digest match: **TRUE**
- Quarantine canonical digest match: **TRUE**
- Duplicate ledger canonical digest match: **TRUE**

## C03 — Schema Allowlist & Privacy Minimization

PASS.

The curated Parquet candidate adheres strictly to the 10-column allowlist with verified types. Restricted attributes (`operator_email`, `raw_payload`) are completely excluded from the analyst-facing release:

| Column | Type | Nullable | Description |
| :--- | :--- | :--- | :--- |
| `record_id` | VARCHAR | No | Normalized business identifier (`^R[0-9]{6}$`) |
| `sensor_id` | VARCHAR | No | Normalized sensor identifier |
| `site` | VARCHAR | No | Sensor deployment zone from registry |
| `event_time_utc` | TIMESTAMP | No | Observation event timestamp (UTC) |
| `ingest_time_utc` | TIMESTAMP | No | Ingest/arrival timestamp (UTC) |
| `temperature_c` | DECIMAL(8,2) | No | Calibrated temperature in Celsius |
| `is_late` | BOOLEAN | No | True if `ingest_ts - event_ts > 900s` |
| `source_object` | VARCHAR | No | Provenance source object URI |
| `source_row` | BIGINT | No | Provenance 1-based source row index |
| `source_sha256` | VARCHAR | No | SHA-256 hash of the input source object |

## C04 — Edge Cases & Counterexamples

PASS (4/4 cases).

1. **CE1 (32.0 °F Unit Conversion)**: Successfully converted to `0.00 °C` and retained in curated candidate.
2. **CE2 (Unsupported K Unit / Non-finite Numeric)**: Quarantined with primary reason `UNIT` / `NUMERIC` without silent conversion.
3. **CE3 (Blank `operator_email`)**: Valid observation retained; optional restricted email column omitted from curated candidate.
4. **CE4 (Newer Invalid Winner + Older Valid Version)**: Newer invalid version is quarantined (`RANGE`); older version remains in duplicate ledger (`SUPERSEDED`); no silent substitution of older version.

## C05 — Residual Quality & QA Sample Evaluation

PASS.

- **Required Completeness**: 9,610 / 9,610 (**100.00%**)
- **Business Key Uniqueness**: 9,610 / 9,610 (**100.00%**)
- **Value Validity**: 9,610 / 9,610 (**100.00%**)
- **Referential Consistency**: 9,610 / 9,610 (**100.00%**)
- **Temporal Consistency**: 9,610 / 9,610 (**100.00%**)
- **Timeliness**: 9,410 / 9,610 (**97.92%** >= 95% threshold; 200 retained late observations)
- **QA Reference Coverage**: 100 / 100 (**100.00%** == 100% threshold)
- **QA Reference Agreement**: 95 / 100 (**95.00%** >= 95% threshold within ±0.05 °C)
