# T2 — Quality Measurement

- Measurement run: `5e6b036d-9d0b-47f0-9536-d5e37d8a2ac0`
- Generated at: `2026-10-12T02:17:59.478138+00:00`
- Contract: `research-telemetry-v1`; fixed AS_OF `2026-02-09T00:00:00Z`
- Measurement status: before-curation checkpoint generated; this is not a release-gate decision.

## Before curation

The before population W is the deterministic winner set after parse/key eligibility and deduplication, before row-validity filtering. Completeness and validity ratios therefore do not use all physical intake rows as their denominator.

- Raw observation records: 10205
- Parse failures retained in intake: 5
- Key-rejected parsed records: 40
- Key-eligible records before deduplication: 10160
- Duplicate excess: 200
- Winners in W before validity filtering: 9960
- Late but chronologically valid winners retained: 200
- Required-field failures in parsed intake: 130 / 10200

| Metric | Numerator / denominator | Value | Threshold | Status |
|---|---:|---:|---:|---|
| Business-key uniqueness before deduplication | 9960 / 10160 | 98.0315% | 100% | FAIL |
| Required-field failures in parsed intake | 130 / 10200 | 1.2745% | n/a | OBSERVED |
| Sensor registry consistency | 9930 / 9960 | 99.6988% | 100% | FAIL |
| Required-field completeness | 9850 / 9960 | 98.8956% | 100% | FAIL |
| Timestamp parsing and chronology | 9890 / 9960 | 99.2972% | 100% | FAIL |
| Arrival within the contract lag threshold | 9690 / 9890 | 97.9778% | 95% | PASS |
| Finite, supported, in-range temperature values | 9710 / 9960 | 97.4900% | 100% | FAIL |

A zero denominator is recorded as `NOT_EVALUATED` with a null value. Timeliness excludes chronologically invalid winners from its denominator; the temporal metric reports those rows separately. Diagnostic categories can overlap; category counts are not added to infer a total row count. Duplicate versions and retained late rows are reported separately from invalid rows.

## Defect investigation

The table highlights up to three observed categories by count. `defects.csv` contains up to five stable physical-row examples per category and excludes `operator_email` and `raw_payload`.

| Category | Count | Interpretation | Sample physical reference |
|---|---:|---|---|
| NUMERIC_INVALID | 170 | A winner has a missing, non-numeric, or non-finite reading. | `s3://research-raw/lab2/inputs/batch-01/observations_a.csv#1` |
| REQUIRED_FIELD_MISSING | 110 | A winner has one or more blank required source fields. | `s3://research-raw/lab2/inputs/batch-01/observations_a.csv#1` |
| EVENT_TIME_PARSE | 40 | A winner's observation timestamp could not be parsed. | `s3://research-raw/lab2/inputs/batch-01/observations_a.csv#241` |

## After curation

| Metric | Numerator / denominator | Value | Threshold | Status |
|---|---:|---:|---:|---|
| Business-key uniqueness after curation | 9610 / 9610 | 100.0000% | 100% | PASS |
| Reference sample agreement within 0.05 C | 95 / 100 | 95.0000% | 95% | PASS |
| Reference sample coverage | 100 / 100 | 100.0000% | 100% | PASS |
| Sensor registry consistency | 9610 / 9610 | 100.0000% | 100% | PASS |
| Required-field completeness | 9610 / 9610 | 100.0000% | 100% | PASS |
| Timestamp parsing and chronology | 9610 / 9610 | 100.0000% | 100% | PASS |
| Arrival within the contract lag threshold | 9410 / 9610 | 97.9188% | 95% | PASS |
| Finite, in-range Celsius temperature values | 9610 / 9610 | 100.0000% | 100% | PASS |

- Candidate retained: 9610 / 10205 (94.1695%).
- Quarantined: 395 / 10205 (3.8707%).
- Duplicate versions: 200 / 10205 (1.9598%).
- Physical count partition matches raw input: `true`.
- Remaining late observations retained in candidate: 200 of 9610 chronologically valid rows.
- Before/after chart: `outputs/t2_quality/before_after.svg` (separate dimensions; no composite quality score).

## Interpretation and limitations

Completeness may rise after invalid rows are removed; that does not mean missing source values were recovered. Timeliness uses chronologically valid rows as its denominator, and late observations remain visible. Reference agreement and coverage evaluate only the supplied 100-row sample and do not establish accuracy for every sensor observation.

## Reproducibility

- Trusted input manifest SHA-256: `77dacfdb5a8b4e82fb2316990972de731a51b31f9fc052d739d0dea78e992331`
- Contract SHA-256: `cefd64851030ba3f20e95dc8511dc64a9de2e00d8e892baa066160485649f78a`
- Executed code bundle SHA-256: `a1172f27464d679c0ab1d14cee0b0158239c52702e68f8eb04fa7c4321305e40`
- Before-checkpoint command: `python code/run_quality.py --data-dir input --contract contract/contract.json`.
- SQL metric populations and numerators/denominators are defined in `code/quality.sql`; runtime inputs are loaded by `code/run_quality.py`.
