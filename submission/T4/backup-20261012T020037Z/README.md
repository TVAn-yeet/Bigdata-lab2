# Lab 2 - Task 4: Metadata Catalog, Lineage, and Retention

## 1. Data Source & Structure
- **Source Type**: Multi-file batch containing environmental sensor streams (`observations_a.csv` and `observations_b.jsonl`).
- **Storage Locations**: 
  - Raw inputs reside at `s3://research-raw/lab2/inputs/batch-01/`
  - Curated outputs reside at `s3://research-release/lab2/curated/batch-01/`

## 2. Timestamps, Units & Deduplication
- **Timestamps**: Formatted strictly in ISO8601 UTC standard.
- **Units**: Temperature readings are standardized to Celsius ($[-30, 60]^\circ\text{C}$).
- **Deduplication**: Applied using deterministic SQL-based window functions (`ROW_NUMBER()`) prioritizing business keys (`record_id`).

## 3. Quality Cohorts & Permitted Use
- **Quality Cohorts**: Separated into raw ingestion streams, verified curated candidates, and an isolated quarantine collection for records violating the contract.
- **Permitted Use**: Strictly restricted to analytics, educational research, and downstream machine learning modeling.
- **Access Boundary**: Internal research team access only; PII (such as `operator_email`) is filtered out in the release schema.

## 4. Reproducibility & Software Versions
- **Software Versions**: Python 3.10+, OpenLineage 2.0.2 specification.
- **Reproducibility Guide**: 
  1. Verify contract constraints using `contract/contract.json`.
  2. Execute ingestion pipelines and validate inputs against the manifest hashes.
  3. Run deterministic curation queries and review generated metadata catalog and lineage logs.