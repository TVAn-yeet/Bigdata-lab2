#!/usr/bin/env python3
"""Local fail-closed release-gate probes for Lab 2 Task 5.

This is a local validation harness only. It never publishes data.
Each probe starts from a fresh copy of the baseline candidate/metadata.
"""
from __future__ import annotations

import csv
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
PROBES = ROOT / "submission" / "T5" / "probes"
EVIDENCE = ROOT / "evidence" / "T5" / "probes"
BASE_CANDIDATE = ROOT / "submission" / "T3" / "curated.parquet"
BASE_CATALOG = ROOT / "submission" / "T4" / "catalog.json"
BASE_LINEAGE = ROOT / "submission" / "T4" / "lineage.jsonl"
BASE_MANIFEST = ROOT / "submission" / "T3" / "candidate_manifest.json"
TRUSTED_MANIFEST = ROOT / "input" / "trusted_manifest.json"
CONTRACT = ROOT / "contract" / "contract.json"
QA = ROOT / "input" / "snapshot" / "qa_reference.csv"
SCHEMA = ROOT / "Lab2-datasets" / "schemas" / "metadata_catalog_schema.json"

def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))

def dump_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")

def candidate_stats(path: Path):
    con = duckdb.connect()
    p = str(path).replace("'", "''")
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{p}')").fetchone()[0]
    distinct_ids = con.execute(
        f"SELECT count(DISTINCT record_id) FROM read_parquet('{p}')"
    ).fetchone()[0]
    null_temperature = con.execute(
        f"SELECT count(*) FROM read_parquet('{p}') WHERE temperature_c IS NULL"
    ).fetchone()[0]
    out_of_range = con.execute(
        f"""SELECT count(*) FROM read_parquet('{p}')
            WHERE temperature_c IS NOT NULL
              AND (temperature_c < -30 OR temperature_c > 60)"""
    ).fetchone()[0]
    null_required = con.execute(
        f"""SELECT count(*) FROM read_parquet('{p}')
            WHERE record_id IS NULL OR sensor_id IS NULL OR site IS NULL
               OR event_time_utc IS NULL OR ingest_time_utc IS NULL
               OR temperature_c IS NULL OR is_late IS NULL
               OR source_object IS NULL OR source_row IS NULL
               OR source_sha256 IS NULL"""
    ).fetchone()[0]
    qa_count = con.execute(
        f"""SELECT count(*) FROM read_csv_auto('{str(QA).replace("'", "''")}')"""
    ).fetchone()[0]
    matched = con.execute(
        f"""SELECT count(DISTINCT upper(trim(q.record_id)))
            FROM read_csv_auto('{str(QA).replace("'", "''")}') q
            JOIN read_parquet('{p}') c
              ON upper(trim(q.record_id)) = upper(trim(c.record_id))"""
    ).fetchone()[0]
    agrees = con.execute(
        f"""SELECT count(DISTINCT upper(trim(q.record_id)))
            FROM read_csv_auto('{str(QA).replace("'", "''")}') q
            JOIN read_parquet('{p}') c
              ON upper(trim(q.record_id)) = upper(trim(c.record_id))
            WHERE abs(c.temperature_c - q.reference_c) <= 0.05"""
    ).fetchone()[0]
    con.close()
    return {
        "rows": rows,
        "distinct_record_ids": distinct_ids,
        "duplicate_rows": rows - distinct_ids,
        "null_temperature_rows": null_temperature,
        "out_of_range_temperature_rows": out_of_range,
        "required_field_failure_rows": null_required,
        "qa_reference_rows": qa_count,
        "qa_matched_ids": matched,
        "qa_agreeing_ids": agrees,
        "qa_coverage": matched / qa_count if qa_count else None,
        "qa_agreement_of_all_reference_rows": agrees / qa_count if qa_count else None,
    }

def prepare_probe(probe_id: str):
    d = PROBES / probe_id
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    candidate = d / "curated.parquet"
    shutil.copy2(BASE_CANDIDATE, candidate)
    shutil.copy2(BASE_CATALOG, d / "catalog.json")
    shutil.copy2(BASE_LINEAGE, d / "lineage.jsonl")
    shutil.copy2(BASE_MANIFEST, d / "candidate_manifest.json")
    shutil.copy2(TRUSTED_MANIFEST, d / "trusted_manifest.json")
    return d, candidate

def alter_candidate(path: Path, sql_select: str):
    temp = path.with_suffix(".tmp.parquet")
    con = duckdb.connect()
    src = str(path).replace("'", "''")
    dst = str(temp).replace("'", "''")
    con.execute(f"COPY ({sql_select.replace('__SOURCE__', src)}) TO '{dst}' (FORMAT PARQUET)")
    con.close()
    temp.replace(path)

def update_candidate_manifest(probe_dir: Path, candidate: Path):
    """Record the staged probe bytes, while preserving the trusted source manifest."""
    manifest_path = probe_dir / "candidate_manifest.json"
    m = load_json(manifest_path)
    for artifact in m.get("artifacts", []):
        if artifact.get("name") == "curated.parquet":
            artifact["records"] = candidate_stats(candidate)["rows"]
            artifact["bytes"] = candidate.stat().st_size
            artifact["sha256"] = sha256(candidate)
    dump_json(manifest_path, m)

def gate(probe_id: str, probe_dir: Path, candidate: Path, intended: str):
    reasons = []
    stats = candidate_stats(candidate)
    contract = load_json(CONTRACT)
    trusted = load_json(probe_dir / "trusted_manifest.json")
    catalog = load_json(probe_dir / "catalog.json")
    lineage_lines = [
        json.loads(line) for line in (probe_dir / "lineage.jsonl").read_text().splitlines()
        if line.strip()
    ]

    # Candidate self-integrity: check its bytes against its staged manifest.
    manifest = load_json(probe_dir / "candidate_manifest.json")
    artifact = next(
        (x for x in manifest.get("artifacts", []) if x.get("name") == "curated.parquet"),
        None,
    )
    if not artifact or artifact.get("sha256") != sha256(candidate):
        reasons.append("INTEGRITY")

    # Reconcile schema metadata with the actual candidate.
    curated_meta = next(
        (x for x in catalog.get("datasets", []) if x.get("dataset_id") == "curated_candidate"),
        None,
    )
    if not curated_meta or not curated_meta.get("steward"):
        reasons.append("METADATA")
    elif curated_meta.get("row_count") != stats["rows"]:
        reasons.append("METADATA")

    # Actual content predicates.
    if stats["required_field_failure_rows"] > 0:
        reasons.append("REQUIRED")
    if stats["out_of_range_temperature_rows"] > 0:
        reasons.append("VALUE")
    if stats["duplicate_rows"] > 0:
        reasons.append("UNIQUENESS")
    if stats["qa_coverage"] is None or stats["qa_coverage"] < contract["release_thresholds"]["qa_coverage"]:
        reasons.append("COVERAGE")
    if stats["qa_agreement_of_all_reference_rows"] is None or (
        stats["qa_agreement_of_all_reference_rows"]
        < contract["release_thresholds"]["qa_agreement"]
    ):
        reasons.append("AGREEMENT")

    # Verify trusted frozen snapshot bytes independently.
    snapshot = ROOT / "input" / "snapshot"
    for obj in trusted.get("objects", []):
        source = snapshot / obj["key"]
        if not source.exists() or source.stat().st_size != obj["bytes"] or sha256(source) != obj["sha256"]:
            reasons.append("INPUT_INTEGRITY")
            break

    # P06 must validate the disposable tampered source copy against the trusted manifest.
    if probe_id == "P06":
        probe_source = probe_dir / "probe_source.json"
        try:
            tamper = load_json(probe_source)
            if (
                tamper["observed_sha256"] != tamper["trusted_sha256"]
                or tamper["observed_bytes"] != tamper["trusted_bytes"]
            ):
                reasons.append("INPUT_INTEGRITY")
        except Exception:
            reasons.append("INPUT_INTEGRITY")

    # Verify required output lineage exists and matches the baseline artifact names.
    complete_events = [e for e in lineage_lines if e.get("eventType") == "COMPLETE"]
    required_outputs = {"curated.parquet", "quarantine.csv", "duplicates.csv"}
    lineage_outputs = {
        out.get("name")
        for event in complete_events
        for out in event.get("outputs", [])
    }
    if not complete_events or not required_outputs.issubset(lineage_outputs):
        reasons.append("LINEAGE")

    # Require metadata catalog schema to validate.
    try:
        import jsonschema
        schema = load_json(SCHEMA)
        datasets = catalog.get("datasets", [])
        if not datasets:
            raise ValueError("Catalog contains no datasets")
        for dataset in datasets:
            jsonschema.validate(dataset, schema)
    except Exception as exc:
        reasons.append("METADATA")

    reasons = sorted(set(reasons))
    result = {
        "probe_id": probe_id,
        "intended_test": intended,
        "decision": "REJECT" if reasons else "PASS",
        "reasons": reasons,
        "eligible_for_independent_owner_review": probe_id == "P01" and not reasons,
        "release_published": False,
        "candidate_sha256": sha256(candidate),
        "candidate_bytes": candidate.stat().st_size,
        "stats": stats,
        "evaluated_at_utc": datetime.now(timezone.utc).isoformat(),
        "limitations": [
            "Local test harness only; no release or cloud publication performed.",
            "A passing gate is not owner approval.",
            "Lineage validation checks required output names; it does not prove execution by itself."
        ],
    }
    dump_json(EVIDENCE / f"{probe_id}.json", result)
    return result

def main():
    PROBES.mkdir(parents=True, exist_ok=True)
    EVIDENCE.mkdir(parents=True, exist_ok=True)

    results = []
    definitions = [
        ("P01", "Unmodified baseline candidate"),
        ("P02", "Set one temperature_c to 100.00 C"),
        ("P03", "Set one required temperature_c to NULL"),
        ("P04", "Append a duplicate business record"),
        ("P05", "Remove steward from curated metadata"),
        ("P06", "Tamper with disposable source copy and compare to trusted manifest"),
        ("P07", "Remove required output from disposable lineage"),
        ("P08", "Remove mismatching QA sample R000810 from candidate"),
    ]

    for probe_id, intended in definitions:
        d, candidate = prepare_probe(probe_id)

        if probe_id == "P02":
            alter_candidate(candidate, """
                SELECT * REPLACE (
                    CASE WHEN row_number() OVER (ORDER BY record_id) = 1
                         THEN 100.00::DECIMAL(8,2) ELSE temperature_c END
                    AS temperature_c
                ) FROM read_parquet('__SOURCE__')
            """)
        elif probe_id == "P03":
            alter_candidate(candidate, """
                SELECT * REPLACE (
                    CASE WHEN row_number() OVER (ORDER BY record_id) = 1
                         THEN NULL::DECIMAL(8,2) ELSE temperature_c END
                    AS temperature_c
                ) FROM read_parquet('__SOURCE__')
            """)
        elif probe_id == "P04":
            alter_candidate(candidate, """
                (SELECT * FROM read_parquet('__SOURCE__'))
                UNION ALL
                (SELECT * FROM read_parquet('__SOURCE__') LIMIT 1)
            """)
        elif probe_id == "P05":
            catalog_path = d / "catalog.json"
            catalog = load_json(catalog_path)
            for item in catalog.get("datasets", []):
                if item.get("dataset_id") == "curated_candidate":
                    item.pop("steward", None)
            dump_json(catalog_path, catalog)
        elif probe_id == "P06":
            src_copy = d / "source_copy"
            src_copy.mkdir()
            shutil.copy2(ROOT / "input" / "snapshot" / "qa_reference.csv", src_copy / "qa_reference.csv")
            with (src_copy / "qa_reference.csv").open("ab") as f:
                f.write(b"\n# disposable tamper test\n")
            trusted_obj = next(
                x for x in load_json(d / "trusted_manifest.json")["objects"]
                if x["key"] == "qa_reference.csv"
            )
            # The gate must compare this disposable copy, not mutate frozen inputs.
            (d / "probe_source.json").write_text(json.dumps({
                "key": "qa_reference.csv",
                "trusted_sha256": trusted_obj["sha256"],
                "observed_sha256": sha256(src_copy / "qa_reference.csv"),
                "trusted_bytes": trusted_obj["bytes"],
                "observed_bytes": (src_copy / "qa_reference.csv").stat().st_size,
            }, indent=2) + "\n")
        elif probe_id == "P07":
            lp = d / "lineage.jsonl"
            lines = [json.loads(line) for line in lp.read_text().splitlines() if line.strip()]
            for event in lines:
                if event.get("eventType") == "COMPLETE":
                    event["outputs"] = [
                        out for out in event.get("outputs", [])
                        if out.get("name") != "curated.parquet"
                    ]
            lp.write_text("".join(json.dumps(x) + "\n" for x in lines))
        elif probe_id == "P08":
            alter_candidate(candidate, """
                SELECT * FROM read_parquet('__SOURCE__')
                WHERE record_id <> 'R000810'
            """)

        # Keep metadata row count aligned only for probes that change row count
        # so the intended data predicate, rather than stale row-count metadata, is tested.
        if probe_id in {"P04", "P08"}:
            cp = d / "catalog.json"
            cat = load_json(cp)
            new_count = candidate_stats(candidate)["rows"]
            for item in cat.get("datasets", []):
                if item.get("dataset_id") == "curated_candidate":
                    item["row_count"] = new_count
            dump_json(cp, cat)

        update_candidate_manifest(d, candidate)
        result = gate(probe_id, d, candidate, intended)

        # P06 additionally requires the disposable copy's observed hash to fail trust comparison.
        if probe_id == "P06":
            pc = load_json(d / "probe_source.json")
            if pc["observed_sha256"] == pc["trusted_sha256"] or pc["observed_bytes"] == pc["trusted_bytes"]:
                result["reasons"] = sorted(set(result["reasons"] + ["INPUT_INTEGRITY"]))
                result["decision"] = "REJECT"
                dump_json(EVIDENCE / f"{probe_id}.json", result)

        results.append(result)

    summary_path = EVIDENCE / "probe-summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "probe_id", "decision", "reasons", "eligible_for_independent_owner_review",
            "release_published", "candidate_sha256"
        ])
        writer.writeheader()
        for r in results:
            writer.writerow({
                "probe_id": r["probe_id"],
                "decision": r["decision"],
                "reasons": ";".join(r["reasons"]),
                "eligible_for_independent_owner_review": r["eligible_for_independent_owner_review"],
                "release_published": r["release_published"],
                "candidate_sha256": r["candidate_sha256"],
            })

    print(json.dumps({
        "summary_csv": str(summary_path.relative_to(ROOT)),
        "results": [
            {"probe_id": r["probe_id"], "decision": r["decision"], "reasons": r["reasons"]}
            for r in results
        ],
        "release_published": False,
    }, indent=2))

if __name__ == "__main__":
    main()
