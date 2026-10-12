# Task 4 — Metadata Catalog, Lineage, and Retention

## 1. Purpose and scope

This task documents the synthetic research-telemetry pipeline from its raw source objects through the local curation candidates. It provides dataset metadata, processing lineage, proposed retention rules, and evidence for downstream consumers. The data is intended for synthetic teaching and laboratory use only.

## 2. Metadata catalog

`catalog.json` contains seven dataset entries:

- `raw_observations_a`: 5,000 CSV data rows.
- `raw_observations_b`: 5,205 physical JSONL lines. Five lines are malformed, so the physical line count must not be interpreted as the number of valid JSON records.
- `qa_reference`: 100 reference rows.
- `sensor_reference`: 20 sensor rows.
- `curated_candidate`: 9,610 curated rows in Parquet format.
- `quarantine_collection`: 395 rejected or quarantined rows.
- `duplicate_versions`: 200 duplicate-version rows.

The catalog records dataset purpose, ownership, stewardship, classification, allowed use, location, column metadata, schema version, input-manifest hash, contract hash, code hash, quality-report references, retention information, and provenance.

The catalog is schema-validated. Hashes are used to identify the exact manifest, contract, code, and candidate artifacts. A hash establishes file identity; it does not by itself prove that a dataset is safe or correct.

## 3. Curation and quality

The Task 3 reconciliation reports:

| Output | Rows |
|---|---:|
| Curated candidate | 9,610 |
| Quarantine | 395 |
| Duplicate versions | 200 |
| Total output rows | 10,205 |

The reported conservation delta is zero, and the reconciliation status is `CONSERVED`. Task 3's checks C01–C05 reported `ALL_PASS`. These results apply to the recorded checks and thresholds; they do not imply that every possible data-quality risk has been eliminated.

## 4. Lineage

`lineage.jsonl` records a paired `START` and `COMPLETE` event for the curation job, with one shared run ID, four declared source inputs, and three local candidate outputs. `evidence/T4/lineage/lineage_run_metadata.json` records the run metadata and artifact hashes.

The lineage file documents existing artifacts. Generating these records does not rerun the curation pipeline. The declared candidate outputs are local files and must not be treated as published S3 datasets.

## 5. Retention and access boundaries

`retention-plan.json` proposes these TTLs:

- Raw source data: 30 days.
- Curated candidates: 90 days.
- Quarantine and duplicate staging data: 7 days.
- Audit manifests, lineage, and quality evidence: 180 days.

Retention clocks must use the timestamp defined by the relevant policy. Unknown timestamps must not be treated as expired. Legal or administrative holds and active downstream dependencies override normal expiry. The plan is `DRY_RUN_ONLY`: it does not delete local files or S3 objects. Any future deletion requires a separate review and authorization.

Raw and restricted staging data should remain accessible only to authorized laboratory roles. The catalog's allowed-use statement limits this dataset to synthetic teaching and laboratory activities. Do not use it to make real-world decisions about individuals.

## 6. Publication status and reproduction

The current publication status is `LOCAL_CANDIDATE_NOT_PUBLISHED`. The curated Parquet file, quarantine CSV, and duplicates CSV are local Task 3 artifacts. No claim is made that these outputs have been released to S3.

From the `lab2-work` directory, inspect:

- `submission/T4/catalog.json`
- `submission/T4/lineage.jsonl`
- `submission/T4/retention-plan.json`
- `submission/T3/candidate_manifest.json`
- `submission/T3/reconciliation-summary.json`
- `submission/T3/t3-summary.json`
- `evidence/T4/lineage/lineage_run_metadata.json`

Verify candidate file hashes against `candidate_manifest.json` before consuming them. Recheck catalog schema validity, lineage event pairing, and retention scenarios after any artifact changes.
