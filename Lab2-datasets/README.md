# Lab 2 Dataset Package — research-telemetry-v1

This package contains **synthetic teaching data** for Lab 2: *Data Curation, Quality & Metadata Governance*. No real personal, institutional, or research data are included.

## Files

- `input/observations_a.csv` — source A, CSV schema.
- `input/observations_b.jsonl` — source B, JSON Lines schema. Some malformed lines are intentional and must be preserved as failed physical records rather than silently skipped.
- `input/sensors.csv` — 20-sensor registry used for referential checks and to derive `site`.
- `input/qa_reference.csv` — 100 reference measurements used **only** to evaluate agreement/coverage. Do not use it to repair, impute, select, or overwrite observations.
- `manifests/source_manifest.json` — convenience manifest describing byte size, physical record count, and SHA-256. In the governance exercise, this copy is **not authoritative** because it travels with the data.
- `schemas/observation_contract_schema.json` — schema for your team-created `contract.json`.
- `schemas/metadata_catalog_schema.json` — schema for your machine-readable dataset catalog.
- `templates/catalog_template.json` — starter catalog structure with placeholders.
- `templates/openlineage_event_template.json` — starter run-event structure.

## Source-field mapping

| Canonical field | CSV source A | JSON Lines source B |
|---|---|---|
| record_id | record_id | id |
| sensor_id | sensor_id | sensor |
| event_time | event_time | timestamp |
| ingest_time | ingest_time | arrived_at |
| reading | reading | temperature |
| unit | unit | temperature_unit |
| operator_email | operator_email | operator |

## Important handling rules

1. Do **not** modify files under `input/`.
2. Preserve physical provenance as `(source_object, source_row)`.
3. Treat a CSV `source_row` as the 1-based data-record ordinal excluding the header; a JSONL `source_row` is the 1-based line number.
4. Malformed JSON lines are intentional and must remain visible to your reconciliation logic.
5. The instructor provides `trusted_manifest.json` separately (for example, read-only through the LMS or mounted into the lab environment). Verify downloaded input bytes against that independently controlled copy.
6. `qa_reference.csv` is a validation source, not a transformation source.
7. The fixed evaluation cutoff for this fixture is `2026-02-09T00:00:00Z`.

## Quick inventory

The observation sources contain 10,205 physical records in total. The package also includes 20 registry rows and 100 QA-reference rows. Those counts are inventory facts, not the expected curation result.

## Recommended S3 keys

Upload the four files under `input/` to:

`research-raw/lab2/inputs/batch-01/`

Do not place instructor-only expected-result files in a student-accessible bucket.
