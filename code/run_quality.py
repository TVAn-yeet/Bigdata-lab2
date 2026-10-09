"""Run the Lab 2 T2 quality measurement from the frozen T1 snapshot.

Run before curation:
    python code/run_quality.py

Run the after-curation checkpoint after T3 exports its candidate and ledgers:
    python code/run_quality.py \
      --candidate outputs/t3_curation/curated.parquet \
      --quarantine outputs/t3_curation/quarantine.csv \
      --duplicates outputs/t3_curation/duplicates.csv
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import math
import os
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator


METRIC_COLUMNS = [
    "rule_id",
    "metric_name",
    "cohort",
    "numerator",
    "denominator",
    "value",
    "threshold",
    "severity",
    "status",
    "measure",
    "dataset_id",
    "dataset_version",
    "run_id",
]

COMPARABLE_METRICS = [
    "Q_REQUIRED_COMPLETENESS",
    "Q_BUSINESS_KEY_UNIQUENESS",
    "Q_VALUE_VALIDITY",
    "Q_REFERENTIAL_CONSISTENCY",
    "Q_TEMPORAL_CONSISTENCY",
    "Q_TIMELINESS",
]

DEFECT_EXPLANATIONS = {
    "PARSE_FAILURE": "Malformed JSONL records preserved in intake.",
    "INVALID_RECORD_ID": "Missing or invalid business key before deduplication.",
    "INGEST_TIME_PARSE": "Arrival timestamp could not be parsed before deduplication.",
    "DUPLICATE_VERSION": "A non-winning physical version for a repeated business key.",
    "REQUIRED_FIELD_MISSING": "A winner has one or more blank required source fields.",
    "NUMERIC_INVALID": "A winner has a missing, non-numeric, or non-finite reading.",
    "UNIT_UNSUPPORTED": "A winner uses a unit outside the frozen contract.",
    "VALUE_OUT_OF_RANGE": "A converted Celsius value falls outside the contract range.",
    "EVENT_TIME_PARSE": "A winner's observation timestamp could not be parsed.",
    "TIME_ORDER": "A winner violates event <= ingest <= AS_OF.",
    "UNKNOWN_SENSOR": "A winner references a sensor absent from the registry.",
    "LATE_RETAINED": "A chronologically valid late winner retained for transparency.",
}

INVALID_DEFECT_CODES = {
    "PARSE_FAILURE",
    "INVALID_RECORD_ID",
    "INGEST_TIME_PARSE",
    "REQUIRED_FIELD_MISSING",
    "NUMERIC_INVALID",
    "UNIT_UNSUPPORTED",
    "VALUE_OUT_OF_RANGE",
    "EVENT_TIME_PARSE",
    "TIME_ORDER",
    "UNKNOWN_SENSOR",
}


def metric_value_status(
    numerator: int,
    denominator: int,
    threshold: float | None,
) -> tuple[float | None, str]:
    """Return a ratio and observed status without treating an empty cohort as 100%."""
    if numerator < 0 or denominator < 0 or numerator > denominator:
        raise ValueError("Metric counts must satisfy 0 <= numerator <= denominator")
    if denominator == 0:
        return None, "NOT_EVALUATED"
    value = numerator / denominator
    if threshold is None:
        return value, "OBSERVED"
    return value, "PASS" if value >= threshold else "FAIL"


def build_metric_record(
    row: tuple[Any, ...], *, dataset_id: str, dataset_version: str, run_id: str
) -> dict[str, Any]:
    (
        rule_id,
        metric_name,
        cohort,
        numerator,
        denominator,
        threshold,
        severity,
        measure,
        sql_value,
    ) = row
    numerator = int(numerator)
    denominator = int(denominator)
    threshold = None if threshold is None else float(threshold)
    value, status = metric_value_status(numerator, denominator, threshold)
    if sql_value is not None and value is not None and not math.isclose(
        float(sql_value), value, rel_tol=1e-12, abs_tol=1e-12
    ):
        raise ValueError(f"SQL/Python ratio mismatch for {rule_id}")
    return {
        "rule_id": rule_id,
        "metric_name": metric_name,
        "cohort": cohort,
        "numerator": numerator,
        "denominator": denominator,
        "value": value,
        "threshold": threshold,
        "severity": severity,
        "status": status,
        "measure": measure,
        "dataset_id": dataset_id,
        "dataset_version": dataset_version,
        "run_id": run_id,
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def code_bundle_digest(repo_root: Path) -> tuple[str, list[dict[str, str]]]:
    paths = ["code/typed.sql", "code/quality.sql", "code/run_quality.py"]
    entries = [
        {"path": rel_path, "sha256": sha256_file(repo_root / rel_path)}
        for rel_path in sorted(paths)
    ]
    canonical = json.dumps(entries, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(canonical).hexdigest(), entries


def resolve_manifest_path(repo_root: Path, data_dir: Path) -> Path:
    candidates = [
        data_dir / "trusted_manifest.json",
        data_dir.parent / "trusted_manifest.json",
        repo_root / "input" / "trusted_manifest.json",
        repo_root / "trusted_manifest.json",
    ]
    for path in candidates:
        if path.is_file():
            return path.resolve()
    raise FileNotFoundError("Could not locate the T1 trusted_manifest.json")


def resolve_inventory_path(repo_root: Path, data_dir: Path) -> Path | None:
    candidates = [
        data_dir / "input_inventory.json",
        data_dir.parent / "input_inventory.json",
        repo_root / "input" / "input_inventory.json",
    ]
    return next((path.resolve() for path in candidates if path.is_file()), None)


def validate_snapshot_files(data_dir: Path, manifest: dict[str, Any]) -> None:
    objects = manifest.get("objects", [])
    if not objects:
        raise ValueError("Trusted manifest has no objects")
    seen = set()
    for obj in objects:
        name = obj["key"]
        if name in seen:
            raise ValueError(f"Trusted manifest repeats object key: {name}")
        seen.add(name)
        path = data_dir / "snapshot" / name
        if not path.is_file():
            raise FileNotFoundError(f"Snapshot object is missing: {path}")
        data_size = path.stat().st_size
        digest = sha256_file(path)
        if data_size != int(obj["bytes"]) or digest != obj["sha256"]:
            raise ValueError(f"INPUT_INTEGRITY: staged snapshot does not match trusted manifest: {name}")
        if name.endswith(".jsonl"):
            with path.open("r", encoding="utf-8") as stream:
                record_count = sum(1 for _ in stream)
        else:
            with path.open("r", newline="", encoding="utf-8") as stream:
                record_count = sum(1 for _ in csv.DictReader(stream))
        if record_count != int(obj["records"]):
            raise ValueError(f"ROW_COUNT: staged snapshot does not match trusted manifest: {name}")


@contextmanager
def working_directory(path: Path) -> Iterator[None]:
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def display_path(path: Path, repo_root: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def make_contract_parameters(connection: Any, contract: dict[str, Any]) -> None:
    units = contract.get("supported_units", [])
    if set(units) != {"C", "F"}:
        raise ValueError("T2 requires the contract's supported units to be C and F")
    thresholds = contract["release_thresholds"]
    temperature_range = contract["temperature_range_c"]
    values = [
        contract["as_of"],
        temperature_range["min"],
        temperature_range["max"],
        contract["late_threshold_seconds"],
        contract["record_id_pattern"],
        "C",
        "F",
        thresholds["required_completeness"],
        thresholds["business_key_uniqueness"],
        thresholds["value_validity"],
        thresholds["sensor_consistency"],
        thresholds["chronology"],
        thresholds["timeliness"],
        thresholds["qa_coverage"],
        thresholds["qa_agreement"],
    ]
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE contract_parameters AS
        SELECT
            ?::TIMESTAMP AS as_of_utc,
            ?::DOUBLE AS min_temperature_c,
            ?::DOUBLE AS max_temperature_c,
            ?::BIGINT AS late_threshold_seconds,
            ?::VARCHAR AS record_id_pattern,
            ?::VARCHAR AS celsius_unit,
            ?::VARCHAR AS fahrenheit_unit,
            ?::DOUBLE AS required_completeness_threshold,
            ?::DOUBLE AS business_key_uniqueness_threshold,
            ?::DOUBLE AS value_validity_threshold,
            ?::DOUBLE AS sensor_consistency_threshold,
            ?::DOUBLE AS chronology_threshold,
            ?::DOUBLE AS timeliness_threshold,
            ?::DOUBLE AS qa_coverage_threshold,
            ?::DOUBLE AS qa_agreement_threshold
        """,
        values,
    )


def create_candidate_view(connection: Any, candidate: Path | None) -> None:
    if candidate is None:
        connection.execute(
            """
            CREATE OR REPLACE TEMP VIEW curated_candidate AS
            SELECT
                NULL::VARCHAR AS record_id,
                NULL::VARCHAR AS sensor_id,
                NULL::VARCHAR AS site,
                NULL::TIMESTAMP AS event_time_utc,
                NULL::TIMESTAMP AS ingest_time_utc,
                NULL::DECIMAL(8, 2) AS temperature_c,
                NULL::BOOLEAN AS is_late,
                NULL::VARCHAR AS source_object,
                NULL::BIGINT AS source_row,
                NULL::VARCHAR AS source_sha256
            WHERE false
            """
        )
        return
    connection.execute(
        "CREATE OR REPLACE TEMP VIEW curated_candidate AS "
        f"SELECT * FROM read_parquet({sql_string(candidate.as_posix())})"
    )


def query_metric_records(
    connection: Any,
    view_name: str,
    *,
    dataset_id: str,
    dataset_version: str,
    run_id: str,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        f"""
        SELECT
            rule_id, metric_name, cohort, numerator, denominator,
            threshold, severity, measure, value
        FROM {view_name}
        ORDER BY rule_id
        """
    ).fetchall()
    return [
        build_metric_record(
            row,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            run_id=run_id,
        )
        for row in rows
    ]


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_metrics_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    write_csv(path, rows, METRIC_COLUMNS)


def metric_by_id(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {row["rule_id"]: row for row in rows}


def format_metric_value(record: dict[str, Any]) -> str:
    value = record["value"]
    if value is None:
        return "NOT_EVALUATED"
    return f"{value:.4%}"


def before_after_rows(
    before: list[dict[str, Any]], after: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    before_by_id = metric_by_id(before)
    after_by_id = metric_by_id(after)
    output = []
    for rule_id in COMPARABLE_METRICS:
        left = before_by_id[rule_id]
        right = after_by_id[rule_id]
        output.append(
            {
                "rule_id": rule_id,
                "metric_name": left["metric_name"],
                "before_cohort": left["cohort"],
                "before_numerator": left["numerator"],
                "before_denominator": left["denominator"],
                "before_value": left["value"],
                "after_cohort": right["cohort"],
                "after_numerator": right["numerator"],
                "after_denominator": right["denominator"],
                "after_value": right["value"],
            }
        )
    return output


def render_before_after_svg(rows: list[dict[str, Any]]) -> str:
    """Render a dependency-free chart from the metric records and their n/d counts."""
    width = 1180
    row_height = 72
    top = 106
    height = top + row_height * len(rows) + 24
    bar_x = 405
    bar_width = 260
    value_x = bar_x + bar_width + 14
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">',
        '<title id="title">T2 quality metrics before and after curation</title>',
        '<desc id="desc">Separate metric ratios with numerator and denominator labels. No aggregate quality score.</desc>',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<text x="18" y="34" font-family="Arial, sans-serif" font-size="22" font-weight="700" fill="#172033">T2 quality metrics by population</text>',
        '<text x="18" y="58" font-family="Arial, sans-serif" font-size="13" fill="#4b5563">Each row shows the measured numerator / denominator; dimensions are not averaged.</text>',
        '<rect x="18" y="74" width="13" height="13" rx="2" fill="#2563eb"/>',
        '<text x="39" y="85" font-family="Arial, sans-serif" font-size="13" fill="#374151">Before: deterministic winners W</text>',
        '<rect x="257" y="74" width="13" height="13" rx="2" fill="#0f766e"/>',
        '<text x="278" y="85" font-family="Arial, sans-serif" font-size="13" fill="#374151">After: curated candidate</text>',
    ]
    for pct, x in [(0, bar_x), (50, bar_x + bar_width / 2), (100, bar_x + bar_width)]:
        lines.append(
            f'<text x="{x}" y="101" text-anchor="middle" font-family="Arial, sans-serif" font-size="11" fill="#6b7280">{pct}%</text>'
        )
    for index, row in enumerate(rows):
        y = top + index * row_height
        label = html.escape(row["metric_name"])
        lines.append(f'<line x1="18" y1="{y + row_height - 3}" x2="{width - 18}" y2="{y + row_height - 3}" stroke="#e5e7eb"/>')
        lines.append(f'<text x="18" y="{y + 23}" font-family="Arial, sans-serif" font-size="13" font-weight="600" fill="#1f2937">{label}</text>')
        for group, top_offset, color, prefix in [
            ("before", 31, "#2563eb", "Before"),
            ("after", 51, "#0f766e", "After"),
        ]:
            value = row[f"{group}_value"]
            numerator = row[f"{group}_numerator"]
            denominator = row[f"{group}_denominator"]
            y_bar = y + top_offset
            lines.append(f'<rect x="{bar_x}" y="{y_bar}" width="{bar_width}" height="11" rx="5" fill="#eef2f7"/>')
            if value is not None:
                fill_width = max(0.0, min(1.0, float(value))) * bar_width
                lines.append(f'<rect x="{bar_x}" y="{y_bar}" width="{fill_width:.2f}" height="11" rx="5" fill="{color}"/>')
                text = f"{float(value):.1%} ({numerator}/{denominator})"
            else:
                text = f"NOT_EVALUATED ({numerator}/{denominator})"
            lines.append(f'<text x="{value_x}" y="{y_bar + 10}" font-family="Arial, sans-serif" font-size="11" fill="#374151">{prefix}: {html.escape(text)}</text>')
    lines.append("</svg>")
    return "\n".join(lines) + "\n"


def render_report(
    *,
    run_id: str,
    generated_at_utc: str,
    before: list[dict[str, Any]],
    counts: dict[str, int],
    defect_counts: list[dict[str, Any]],
    defect_samples: list[dict[str, Any]],
    contract: dict[str, Any],
    hashes: dict[str, str],
    after: dict[str, Any] | None,
) -> str:
    before_table = [
        "| Metric | Numerator / denominator | Value | Threshold | Status |",
        "|---|---:|---:|---:|---|",
    ]
    for row in before:
        threshold = "n/a" if row["threshold"] is None else f"{row['threshold']:.0%}"
        before_table.append(
            f"| {row['metric_name']} | {row['numerator']} / {row['denominator']} | {format_metric_value(row)} | {threshold} | {row['status']} |"
        )

    samples_by_code: dict[str, list[dict[str, Any]]] = {}
    for sample in defect_samples:
        samples_by_code.setdefault(sample["defect_code"], []).append(sample)
    highlighted = sorted(
        (
            row
            for row in defect_counts
            if int(row["defect_count"]) > 0 and row["defect_code"] in INVALID_DEFECT_CODES
        ),
        key=lambda row: (-int(row["defect_count"]), row["defect_code"]),
    )[:3]
    defect_section = [
        "| Category | Count | Interpretation | Sample physical reference |",
        "|---|---:|---|---|",
    ]
    for category in highlighted:
        code = category["defect_code"]
        sample = (samples_by_code.get(code) or [{}])[0]
        source_ref = "n/a"
        if sample:
            source_ref = f"`{sample.get('source_object', '')}#{sample.get('source_row', '')}`"
        defect_section.append(
            f"| {code} | {int(category['defect_count'])} | {DEFECT_EXPLANATIONS.get(code, 'Observed diagnostic category.')} | {source_ref} |"
        )
    if not highlighted:
        defect_section.append("| No observed defect rows | 0 | No diagnostic category had a positive count. | n/a |")

    lines = [
        "# T2 — Quality Measurement",
        "",
        f"- Measurement run: `{run_id}`",
        f"- Generated at: `{generated_at_utc}`",
        f"- Contract: `{contract['schema_version']}`; fixed AS_OF `{contract['as_of']}`",
        "- Measurement status: before-curation checkpoint generated; this is not a release-gate decision.",
        "",
        "## Before curation",
        "",
        "The before population W is the deterministic winner set after parse/key eligibility and deduplication, before row-validity filtering. Completeness and validity ratios therefore do not use all physical intake rows as their denominator.",
        "",
        f"- Raw observation records: {counts['raw_records']}",
        f"- Parse failures retained in intake: {counts['parse_failures']}",
        f"- Key-rejected parsed records: {counts['key_rejected_records']}",
        f"- Key-eligible records before deduplication: {counts['key_eligible_records']}",
        f"- Duplicate excess: {counts['duplicate_excess']}",
        f"- Winners in W before validity filtering: {counts['winners_before_validity']}",
        f"- Late but chronologically valid winners retained: {counts['late_winners_before_curation']}",
        f"- Required-field failures in parsed intake: {counts['parsed_intake_required_failures']} / {counts['parsed_intake_records']}",
        "",
        *before_table,
        "",
        "A zero denominator is recorded as `NOT_EVALUATED` with a null value. Timeliness excludes chronologically invalid winners from its denominator; the temporal metric reports those rows separately. Diagnostic categories can overlap; category counts are not added to infer a total row count. Duplicate versions and retained late rows are reported separately from invalid rows.",
        "",
        "## Defect investigation",
        "",
        "The table highlights up to three observed categories by count. `defects.csv` contains up to five stable physical-row examples per category and excludes `operator_email` and `raw_payload`.",
        "",
        *defect_section,
        "",
    ]

    if after is None:
        lines.extend(
            [
                "## After curation",
                "",
                "Pending the T3 curated Parquet candidate and its quarantine/duplicate ledgers. No after metric, retention fraction, rejection fraction, or before/after chart is fabricated. Rerun this command with `--candidate`, `--quarantine`, and `--duplicates` after T3 exports those artifacts.",
                "",
            ]
        )
    else:
        after_records = after["metrics"]
        after_table = [
            "| Metric | Numerator / denominator | Value | Threshold | Status |",
            "|---|---:|---:|---:|---|",
        ]
        for row in after_records:
            threshold = "n/a" if row["threshold"] is None else f"{row['threshold']:.0%}"
            after_table.append(
                f"| {row['metric_name']} | {row['numerator']} / {row['denominator']} | {format_metric_value(row)} | {threshold} | {row['status']} |"
            )
        dispositions = after["dispositions"]
        late = after["remaining_late_observations"]
        lines.extend(
            [
                "## After curation",
                "",
                *after_table,
                "",
                f"- Candidate retained: {dispositions['curated_records']} / {dispositions['raw_records']} ({format_metric_value({'value': dispositions['retained_fraction']})}).",
                f"- Quarantined: {dispositions['quarantined_records']} / {dispositions['raw_records']} ({format_metric_value({'value': dispositions['rejected_fraction']})}).",
                f"- Duplicate versions: {dispositions['duplicate_versions']} / {dispositions['raw_records']} ({format_metric_value({'value': dispositions['duplicate_fraction']})}).",
                f"- Physical count partition matches raw input: `{str(dispositions['partition_matches_raw']).lower()}`.",
                f"- Remaining late observations retained in candidate: {late['count']} of {late['chronologically_valid_count']} chronologically valid rows.",
                "- Before/after chart: `outputs/t2_quality/before_after.svg` (separate dimensions; no composite quality score).",
                "",
            ]
        )

    lines.extend(
        [
            "## Interpretation and limitations",
            "",
            "Completeness may rise after invalid rows are removed; that does not mean missing source values were recovered. Timeliness uses chronologically valid rows as its denominator, and late observations remain visible. Reference agreement and coverage evaluate only the supplied 100-row sample and do not establish accuracy for every sensor observation.",
            "",
            "## Reproducibility",
            "",
            f"- Trusted input manifest SHA-256: `{hashes['input_manifest_sha256']}`",
            f"- Contract SHA-256: `{hashes['contract_sha256']}`",
            f"- Executed code bundle SHA-256: `{hashes['code_sha256']}`",
            "- Before-checkpoint command: `python code/run_quality.py --data-dir input --contract contract/contract.json`.",
            "- SQL metric populations and numerators/denominators are defined in `code/quality.sql`; runtime inputs are loaded by `code/run_quality.py`.",
            "",
        ]
    )
    return "\n".join(lines)


def count_csv_records(path: Path) -> int:
    with path.open("r", newline="", encoding="utf-8") as stream:
        reader = csv.reader(stream)
        next(reader, None)
        return sum(1 for _ in reader)


def write_after_comparison(
    output_dir: Path,
    before: list[dict[str, Any]],
    after: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = before_after_rows(before, after)
    columns = [
        "rule_id",
        "metric_name",
        "before_cohort",
        "before_numerator",
        "before_denominator",
        "before_value",
        "after_cohort",
        "after_numerator",
        "after_denominator",
        "after_value",
    ]
    write_csv(output_dir / "before_after.csv", rows, columns)
    (output_dir / "before_after.svg").write_text(
        render_before_after_svg(rows), encoding="utf-8"
    )
    return rows


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    default_data_dir = repo_root / "input"
    if not (default_data_dir / "envelopes.csv").is_file():
        default_data_dir = Path.cwd()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=default_data_dir)
    parser.add_argument("--contract", type=Path, default=repo_root / "contract" / "contract.json")
    parser.add_argument("--output-dir", type=Path, default=repo_root / "outputs" / "t2_quality")
    parser.add_argument("--report", type=Path, default=repo_root / "reports" / "T2_report.md")
    parser.add_argument("--evidence-dir", type=Path, default=repo_root / "evidence" / "T2" / "before")
    parser.add_argument("--candidate", type=Path, help="T3 curated Parquet candidate")
    parser.add_argument("--quarantine", type=Path, help="T3 quarantine.csv; required with --candidate")
    parser.add_argument("--duplicates", type=Path, help="T3 duplicates.csv; required with --candidate")
    parser.add_argument("--run-id", help="Optional existing run UUID; generated when omitted")
    args = parser.parse_args(argv)
    if args.candidate and not (args.quarantine and args.duplicates):
        parser.error("--candidate requires both --quarantine and --duplicates for disposition fractions")
    if not args.candidate and (args.quarantine or args.duplicates):
        parser.error("--quarantine and --duplicates are only valid with --candidate")
    return args


def run(argv: list[str] | None = None) -> dict[str, Any]:
    args = parse_args(argv)
    repo_root = Path(__file__).resolve().parents[1]
    data_dir = args.data_dir.resolve()
    contract_path = args.contract.resolve()
    output_dir = args.output_dir.resolve()
    report_path = args.report.resolve()
    evidence_dir = args.evidence_dir.resolve()
    candidate = args.candidate.resolve() if args.candidate else None
    quarantine = args.quarantine.resolve() if args.quarantine else None
    duplicates = args.duplicates.resolve() if args.duplicates else None

    if args.run_id:
        run_id = str(uuid.UUID(args.run_id))
    else:
        run_id = str(uuid.uuid4())
    generated_at_utc = datetime.now(timezone.utc).isoformat()

    for required_path in [
        data_dir / "envelopes.csv",
        data_dir / "snapshot" / "sensors.csv",
        data_dir / "snapshot" / "qa_reference.csv",
        contract_path,
        repo_root / "code" / "typed.sql",
        repo_root / "code" / "quality.sql",
    ]:
        if not required_path.is_file():
            raise FileNotFoundError(f"Required T2 input is missing: {required_path}")
    if candidate is not None:
        for required_path in [candidate, quarantine, duplicates]:
            if not required_path.is_file():
                raise FileNotFoundError(f"Required T3 after-curation artifact is missing: {required_path}")

    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    manifest_path = resolve_manifest_path(repo_root, data_dir)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    validate_snapshot_files(data_dir, manifest)
    inventory_path = resolve_inventory_path(repo_root, data_dir)
    inventory = json.loads(inventory_path.read_text(encoding="utf-8")) if inventory_path else []
    store_id = inventory[0].get("store_id") if inventory else None

    contract_hash = sha256_file(contract_path)
    checksum_path = contract_path.with_name("contract.sha256")
    if checksum_path.is_file():
        claimed_hash = checksum_path.read_text(encoding="utf-8").split()[0]
        if claimed_hash != contract_hash:
            raise ValueError("contract.sha256 does not match contract.json; stop before measuring")
    code_hash, code_files = code_bundle_digest(repo_root)
    hashes = {
        "input_manifest_sha256": sha256_file(manifest_path),
        "contract_sha256": contract_hash,
        "code_sha256": code_hash,
    }

    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError(
            "DuckDB is required; use the pinned duckdb==1.4.1 from curator-image/Dockerfile"
        ) from exc

    connection = duckdb.connect(":memory:")
    try:
        with working_directory(data_dir):
            connection.execute((repo_root / "code" / "typed.sql").read_text(encoding="utf-8"))
            make_contract_parameters(connection, contract)
            create_candidate_view(connection, candidate)
            connection.execute((repo_root / "code" / "quality.sql").read_text(encoding="utf-8"))

        before_metrics = query_metric_records(
            connection,
            "quality_before_metrics",
            dataset_id="research-telemetry-v1-before",
            dataset_version=contract["schema_version"],
            run_id=run_id,
        )
        counts_row = connection.execute("SELECT * FROM quality_before_counts").fetchone()
        count_columns = [item[0] for item in connection.description]
        before_counts = {name: int(value) for name, value in zip(count_columns, counts_row)}

        defect_counts = [
            {"defect_code": row[0], "defect_count": int(row[1])}
            for row in connection.execute(
                "SELECT defect_code, defect_count FROM quality_defect_counts ORDER BY defect_code"
            ).fetchall()
        ]
        defect_columns = [
            "defect_code",
            "population",
            "defect_count",
            "sample_number",
            "source_object",
            "source_row",
            "record_id",
            "sensor_id",
        ]
        defect_samples = [
            dict(zip(defect_columns, row))
            for row in connection.execute(
                "SELECT " + ", ".join(defect_columns) + " FROM quality_defect_samples ORDER BY defect_code, sample_number"
            ).fetchall()
        ]

        output_dir.mkdir(parents=True, exist_ok=True)
        before_document = {
            "measurement_status": "MEASURED",
            "dataset_id": "research-telemetry-v1-before",
            "dataset_version": contract["schema_version"],
            "run_id": run_id,
            "generated_at_utc": generated_at_utc,
            "as_of": contract["as_of"],
            "population": "Deterministic winners before row-validity filtering",
            "counts": before_counts,
            "metrics": before_metrics,
            "defect_category_counts": defect_counts,
            "hashes": hashes,
        }
        write_json(output_dir / "quality_before.json", before_document)
        write_csv(
            output_dir / "defects.csv",
            defect_samples,
            defect_columns,
        )

        after_document = None
        all_metrics = list(before_metrics)
        if candidate is not None:
            after_metrics = query_metric_records(
                connection,
                "quality_after_metrics",
                dataset_id="research-telemetry-v1-curated-candidate",
                dataset_version=contract["schema_version"],
                run_id=run_id,
            )
            after_counts_row = connection.execute(
                """
                SELECT
                    count(*)::BIGINT AS curated_records,
                    count(*) FILTER (WHERE is_late)::BIGINT AS remaining_late_observations,
                    count(*) FILTER (WHERE chronology_valid)::BIGINT AS chronologically_valid_records
                FROM quality_after_flags
                """
            ).fetchone()
            curated_count, late_count, chronological_count = map(int, after_counts_row)
            quarantine_count = count_csv_records(quarantine)
            duplicate_count = count_csv_records(duplicates)
            raw_count = before_counts["raw_records"]
            partition_count = curated_count + quarantine_count + duplicate_count
            dispositions = {
                "raw_records": raw_count,
                "curated_records": curated_count,
                "quarantined_records": quarantine_count,
                "duplicate_versions": duplicate_count,
                "retained_fraction": curated_count / raw_count if raw_count else None,
                "rejected_fraction": quarantine_count / raw_count if raw_count else None,
                "duplicate_fraction": duplicate_count / raw_count if raw_count else None,
                "not_in_candidate_fraction": (quarantine_count + duplicate_count) / raw_count if raw_count else None,
                "physical_partition_count": partition_count,
                "partition_matches_raw": partition_count == raw_count,
            }
            after_document = {
                "measurement_status": "MEASURED",
                "dataset_id": "research-telemetry-v1-curated-candidate",
                "dataset_version": contract["schema_version"],
                "run_id": run_id,
                "generated_at_utc": generated_at_utc,
                "as_of": contract["as_of"],
                "population": "Rows in the T3 curated candidate Parquet",
                "candidate_path": display_path(candidate, repo_root),
                "counts": {
                    "curated_records": curated_count,
                    "remaining_late_observations": late_count,
                    "chronologically_valid_records": chronological_count,
                },
                "dispositions": dispositions,
                "remaining_late_observations": {
                    "count": late_count,
                    "chronologically_valid_count": chronological_count,
                    "fraction_of_chronologically_valid": late_count / chronological_count if chronological_count else None,
                },
                "metrics": after_metrics,
                "hashes": hashes,
            }
            write_json(output_dir / "quality_after.json", after_document)
            write_after_comparison(output_dir, before_metrics, after_metrics)
            all_metrics.extend(after_metrics)

        write_metrics_csv(output_dir / "metrics.csv", all_metrics)

        report = render_report(
            run_id=run_id,
            generated_at_utc=generated_at_utc,
            before=before_metrics,
            counts=before_counts,
            defect_counts=defect_counts,
            defect_samples=defect_samples,
            contract=contract,
            hashes=hashes,
            after=after_document,
        )
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report, encoding="utf-8")

        run_record = {
            "run_id": run_id,
            "generated_at_utc": generated_at_utc,
            "as_of": contract["as_of"],
            "store_id": store_id,
            "operator_role": "B / Quality",
            "operator_name": None,
            "reviewer_role": "E / Release (independent QA agreement and coverage check after T3)",
            "reviewer_name": None,
            "review_status": "PENDING_PEER_REVIEW",
            "input_manifest_path": display_path(manifest_path, repo_root),
            "source_objects": manifest.get("objects", []),
            "contract_version": contract["schema_version"],
            "hashes": hashes,
            "code_bundle_files": code_files,
            "command": sys.argv,
            "measurement_status": "MEASURED",
            "counts": before_counts,
            "outputs": [
                display_path(output_path, repo_root)
                for output_path in (
                    [
                        output_dir / "quality_before.json",
                        output_dir / "metrics.csv",
                        output_dir / "defects.csv",
                        report_path,
                    ]
                    + (
                        [
                            output_dir / "quality_after.json",
                            output_dir / "before_after.csv",
                            output_dir / "before_after.svg",
                        ]
                        if after_document is not None
                        else []
                    )
                )
            ],
        }
        run_record["output_artifacts"] = [
            {
                "path": display_path(output_path, repo_root),
                "sha256": sha256_file(output_path),
            }
            for output_path in (
                [
                    output_dir / "quality_before.json",
                    output_dir / "metrics.csv",
                    output_dir / "defects.csv",
                    report_path,
                ]
                + (
                    [
                        output_dir / "quality_after.json",
                        output_dir / "before_after.csv",
                        output_dir / "before_after.svg",
                    ]
                    if after_document is not None
                    else []
                )
            )
        ]
        write_json(evidence_dir / "run-record.json", run_record)
        defect_evidence_path = evidence_dir.parent / "defects" / "defect-investigation.json"
        write_json(
            defect_evidence_path,
            {
                "run_id": run_id,
                "dataset_version": contract["schema_version"],
                "operator_role": "B / Quality",
                "reviewer_role": "E / Release (independent QA agreement and coverage check after T3)",
                "review_status": "PENDING_PEER_REVIEW",
                "counts_overlap": True,
                "category_counts": defect_counts,
                "sample_policy": "Up to five deterministic physical-row references per category.",
                "sample_file": display_path(output_dir / "defects.csv", repo_root),
                "sample_file_sha256": sha256_file(output_dir / "defects.csv"),
                "restricted_fields_excluded": ["operator_email", "raw_payload"],
            },
        )
        if after_document is not None:
            after_output_paths = [
                output_dir / "quality_after.json",
                output_dir / "before_after.csv",
                output_dir / "before_after.svg",
                output_dir / "metrics.csv",
            ]
            after_record = {
                "run_id": run_id,
                "generated_at_utc": generated_at_utc,
                "dataset_version": contract["schema_version"],
                "operator_role": "B / Quality",
                "operator_name": None,
                "reviewer_role": "E / Release (independent QA agreement and coverage check)",
                "reviewer_name": None,
                "review_status": "PENDING_PEER_REVIEW",
                "candidate_path": display_path(candidate, repo_root),
                "candidate_sha256": sha256_file(candidate),
                "quarantine_path": display_path(quarantine, repo_root),
                "quarantine_sha256": sha256_file(quarantine),
                "duplicates_path": display_path(duplicates, repo_root),
                "duplicates_sha256": sha256_file(duplicates),
                "counts": after_document["counts"],
                "dispositions": after_document["dispositions"],
                "outputs": [display_path(path, repo_root) for path in after_output_paths],
                "output_artifacts": [
                    {"path": display_path(path, repo_root), "sha256": sha256_file(path)}
                    for path in after_output_paths
                ],
                "hashes": hashes,
            }
            write_json(evidence_dir.parent / "after" / "run-record.json", after_record)
        return {
            "run_id": run_id,
            "quality_before": str(output_dir / "quality_before.json"),
            "metrics": str(output_dir / "metrics.csv"),
            "defects": str(output_dir / "defects.csv"),
            "defect_evidence": str(defect_evidence_path),
            "quality_after": str(output_dir / "quality_after.json") if after_document else None,
            "defect_categories": len([row for row in defect_counts if row["defect_count"] > 0]),
        }
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    try:
        result = run(argv)
    except Exception as exc:
        print(f"T2_MEASUREMENT_ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
