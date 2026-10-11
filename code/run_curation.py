"""
Lab 2 - Task 3: Deterministic Curation Runner
Role C / Curation
Executes typed.sql and curate.sql, exports curated Parquet, quarantine CSV, and duplicate ledger CSV.
Generates candidate manifest and reconciliation metrics.
"""

import hashlib
import json
import os
import shutil
from pathlib import Path
import duckdb

def sha256_file(filepath: Path) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()

def run_pipeline():
    workspace_root = Path(__file__).resolve().parent.parent
    code_dir = workspace_root / "code"
    
    # Locate inputs
    envelopes_path = workspace_root / "input" / "envelopes.csv"
    if not envelopes_path.exists():
        envelopes_path = workspace_root / "envelopes.csv"
        
    sensors_path = workspace_root / "input" / "snapshot" / "sensors.csv"
    if not sensors_path.exists():
        sensors_path = workspace_root / "snapshot" / "sensors.csv"
        
    qa_path = workspace_root / "input" / "snapshot" / "qa_reference.csv"
    if not qa_path.exists():
        qa_path = workspace_root / "snapshot" / "qa_reference.csv"
        
    contract_path = workspace_root / "contract" / "contract.json"
    if not contract_path.exists():
        contract_path = workspace_root / "submission" / "T1" / "contract.json"

    # Setup output directories
    candidate_dir = workspace_root / "candidate"
    candidate_dir.mkdir(parents=True, exist_ok=True)
    
    submission_t3_dir = workspace_root / "submission" / "T3"
    submission_t3_dir.mkdir(parents=True, exist_ok=True)

    print(f"[*] Starting DuckDB curation pipeline...")
    con = duckdb.connect()

    # Configure session
    con.execute("SET TimeZone='UTC';")
    con.execute("SET threads=2;")
    con.execute("SET memory_limit='512MB';")

    # Load base tables
    env_str = str(envelopes_path).replace("\\", "/")
    sensors_str = str(sensors_path).replace("\\", "/")
    qa_str = str(qa_path).replace("\\", "/")

    con.execute(f"""
    CREATE OR REPLACE TABLE raw_envelopes AS
    SELECT * FROM read_csv('{env_str}', header=true, all_varchar=true);

    CREATE OR REPLACE TABLE registry AS
    SELECT * FROM read_csv('{sensors_str}', all_varchar=true);

    CREATE OR REPLACE TABLE qa_reference AS
    SELECT record_id, cast(reference_c AS DECIMAL(8,2)) AS reference_c
    FROM read_csv('{qa_str}', all_varchar=true);
    """)

    # Execute typed view logic
    con.execute("""
    CREATE OR REPLACE VIEW typed AS
    WITH normalized AS (
        SELECT * EXCLUDE(record_id,sensor_id,unit,source_row,parse_ok),
            upper(trim(record_id)) AS record_id,
            upper(trim(sensor_id)) AS sensor_id,
            upper(trim(unit)) AS unit,
            cast(source_row AS BIGINT) AS source_row,
            cast(parse_ok AS BOOLEAN) AS parse_ok
        FROM raw_envelopes
    )
    SELECT *,
        try_cast(reading AS DOUBLE) AS value_num,
        try_strptime(event_time,'%Y-%m-%dT%H:%M:%SZ') AS event_ts,
        try_strptime(ingest_time,'%Y-%m-%dT%H:%M:%SZ') AS ingest_ts
    FROM normalized;
    """)

    # Execute curate.sql
    curate_sql = (code_dir / "curate.sql").read_text(encoding="utf-8")
    con.execute(curate_sql)

    # Output file paths
    curated_parquet = candidate_dir / "curated.parquet"
    quarantine_csv = candidate_dir / "quarantine.csv"
    duplicates_csv = candidate_dir / "duplicates.csv"

    # Export to files
    curated_str = str(curated_parquet).replace("\\", "/")
    quarantine_str = str(quarantine_csv).replace("\\", "/")
    duplicates_str = str(duplicates_csv).replace("\\", "/")

    print(f"[*] Exporting candidate/curated.parquet...")
    con.execute(f"COPY (SELECT * FROM curated ORDER BY record_id) TO '{curated_str}' (FORMAT PARQUET);")

    print(f"[*] Exporting candidate/quarantine.csv...")
    con.execute(f"COPY (SELECT * FROM quarantine ORDER BY source_object, source_row) TO '{quarantine_str}' (HEADER, DELIMITER ',');")

    print(f"[*] Exporting candidate/duplicates.csv...")
    con.execute(f"COPY (SELECT * FROM duplicates ORDER BY source_object, source_row) TO '{duplicates_str}' (HEADER, DELIMITER ',');")

    # Counts
    n_raw = con.execute("SELECT count(*) FROM raw_envelopes").fetchone()[0]
    n_curated = con.execute("SELECT count(*) FROM curated").fetchone()[0]
    n_quarantine = con.execute("SELECT count(*) FROM quarantine").fetchone()[0]
    n_duplicates = con.execute("SELECT count(*) FROM duplicates").fetchone()[0]

    # Copy artifacts to submission/T3
    shutil.copy2(curated_parquet, submission_t3_dir / "curated.parquet")
    shutil.copy2(quarantine_csv, submission_t3_dir / "quarantine.csv")
    shutil.copy2(duplicates_csv, submission_t3_dir / "duplicates.csv")

    # Compute hashes
    curate_sql_path = code_dir / "curate.sql"
    code_hash = sha256_file(curate_sql_path) if curate_sql_path.exists() else None
    contract_hash = sha256_file(contract_path) if contract_path.exists() else None

    # Manifest and reconciliation record
    artifacts_manifest = {
        "contract_sha256": contract_hash,
        "code_sha256": code_hash,
        "artifacts": [
            {
                "name": "curated.parquet",
                "format": "parquet",
                "records": n_curated,
                "bytes": curated_parquet.stat().st_size,
                "sha256": sha256_file(curated_parquet),
                "classification": "analyst-release"
            },
            {
                "name": "quarantine.csv",
                "format": "csv",
                "records": n_quarantine,
                "bytes": quarantine_csv.stat().st_size,
                "sha256": sha256_file(quarantine_csv),
                "classification": "restricted-staging"
            },
            {
                "name": "duplicates.csv",
                "format": "csv",
                "records": n_duplicates,
                "bytes": duplicates_csv.stat().st_size,
                "sha256": sha256_file(duplicates_csv),
                "classification": "restricted-staging"
            }
        ],
        "reconciliation": {
            "n_raw": n_raw,
            "n_curated": n_curated,
            "n_quarantine": n_quarantine,
            "n_duplicates": n_duplicates,
            "sum_outputs": n_curated + n_quarantine + n_duplicates,
            "conservation_delta": n_raw - (n_curated + n_quarantine + n_duplicates),
            "status": "CONSERVED" if n_raw == (n_curated + n_quarantine + n_duplicates) else "MISMATCH"
        }
    }

    manifest_json_path = submission_t3_dir / "candidate_manifest.json"
    manifest_json_path.write_text(json.dumps(artifacts_manifest, indent=2), encoding="utf-8")
    
    reconcil_json_path = submission_t3_dir / "reconciliation-summary.json"
    reconcil_json_path.write_text(json.dumps(artifacts_manifest["reconciliation"], indent=2), encoding="utf-8")

    print(f"\n[+] Curation Pipeline Completed Successfully!")
    print(f"    Raw Input Records: {n_raw}")
    print(f"    Curated Records:   {n_curated}")
    print(f"    Quarantine:        {n_quarantine}")
    print(f"    Duplicates:        {n_duplicates}")
    print(f"    Sum of Outputs:    {n_curated + n_quarantine + n_duplicates}")
    print(f"    Conservation:      {artifacts_manifest['reconciliation']['status']}")

if __name__ == "__main__":
    run_pipeline()
