import csv
import importlib.util
import json
from pathlib import Path
import unittest


RUNNER_PATH = Path(__file__).with_name("run_quality.py")
SPEC = importlib.util.spec_from_file_location("run_quality", RUNNER_PATH)
run_quality = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(run_quality)

REPO_ROOT = Path(__file__).resolve().parents[1]


class QualityMetricUnitTests(unittest.TestCase):
    def test_zero_denominator_is_not_evaluated_as_perfect_quality(self):
        value, status = run_quality.metric_value_status(0, 0, 1.0)

        self.assertIsNone(value)
        self.assertEqual(status, "NOT_EVALUATED")

    def test_metric_status_uses_threshold_and_preserves_ratio(self):
        passed_value, passed_status = run_quality.metric_value_status(19, 20, 0.95)
        failed_value, failed_status = run_quality.metric_value_status(18, 20, 0.95)

        self.assertEqual(passed_value, 0.95)
        self.assertEqual(passed_status, "PASS")
        self.assertEqual(failed_value, 0.9)
        self.assertEqual(failed_status, "FAIL")

    def test_metric_rejects_invalid_counts(self):
        with self.assertRaisesRegex(ValueError, "Metric counts"):
            run_quality.metric_value_status(2, 1, 1.0)

    def test_chart_uses_metric_counts_and_distinct_population_labels(self):
        rows = [
            {
                "rule_id": "Q_REQUIRED_COMPLETENESS",
                "metric_name": "Required-field completeness",
                "before_numerator": 8,
                "before_denominator": 10,
                "before_value": 0.8,
                "after_numerator": 9,
                "after_denominator": 9,
                "after_value": 1.0,
            }
        ]
        svg = run_quality.render_before_after_svg(rows)

        self.assertIn("Before: deterministic winners W", svg)
        self.assertIn("After: curated candidate", svg)
        self.assertIn("80.0% (8/10)", svg)
        self.assertIn("100.0% (9/9)", svg)
        self.assertIn("No aggregate quality score", svg)


@unittest.skipUnless(importlib.util.find_spec("duckdb"), "DuckDB integration runtime is unavailable")
class QualitySqlIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir_context = __import__("tempfile").TemporaryDirectory()
        self.tmp_path = Path(self.temp_dir_context.name)

    def tearDown(self):
        self.temp_dir_context.cleanup()

    def test_t2_before_measurement_writes_sql_derived_artifacts(self):
        output_dir = self.tmp_path / "outputs" / "t2_quality"
        evidence_dir = self.tmp_path / "evidence" / "T2" / "before"
        report_path = self.tmp_path / "reports" / "T2_report.md"
        run_quality.run(
            [
                "--data-dir",
                str(REPO_ROOT / "input"),
                "--contract",
                str(REPO_ROOT / "contract" / "contract.json"),
                "--output-dir",
                str(output_dir),
                "--evidence-dir",
                str(evidence_dir),
                "--report",
                str(report_path),
                "--run-id",
                "532555de-fef9-4e72-9cb0-65431cc7642f",
            ]
        )

        report = json.loads((output_dir / "quality_before.json").read_text())
        self.assertEqual(report["counts"]["raw_records"], 10205)
        self.assertEqual(report["counts"]["parse_failures"], 5)
        self.assertEqual(
            report["counts"]["duplicate_excess"],
            report["counts"]["key_eligible_records"]
            - report["counts"]["winners_before_validity"],
        )
        self.assertGreaterEqual(
            {item["rule_id"] for item in report["metrics"]},
            {
                "Q_REQUIRED_COMPLETENESS",
                "Q_BUSINESS_KEY_UNIQUENESS",
                "Q_VALUE_VALIDITY",
                "Q_REFERENTIAL_CONSISTENCY",
                "Q_TEMPORAL_CONSISTENCY",
                "Q_TIMELINESS",
            },
        )
        self.assertTrue(
            all(
                item["status"] == "NOT_EVALUATED"
                if item["denominator"] == 0
                else item["value"] is not None
                for item in report["metrics"]
            )
        )

        with (output_dir / "defects.csv").open(newline="", encoding="utf-8") as stream:
            defect_rows = list(csv.DictReader(stream))
        defect_codes = {row["defect_code"] for row in defect_rows}
        invalid_codes = {
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
        self.assertGreaterEqual(len(defect_codes & invalid_codes), 3)
        self.assertFalse(any("@" in value for row in defect_rows for value in row.values()))
        self.assertFalse(any("raw_payload" in key or "operator_email" in key for key in defect_rows[0]))
        self.assertTrue(report_path.is_file())
        self.assertTrue((evidence_dir / "run-record.json").is_file())
        self.assertTrue((evidence_dir.parent / "defects" / "defect-investigation.json").is_file())

    def test_winner_view_obeys_latest_ingest_then_physical_key_order(self):
        import duckdb

        contract = json.loads((REPO_ROOT / "contract" / "contract.json").read_text())
        with duckdb.connect(":memory:") as connection:
            with run_quality.working_directory(REPO_ROOT / "input"):
                connection.execute((REPO_ROOT / "code" / "typed.sql").read_text())
                run_quality.make_contract_parameters(connection, contract)
                run_quality.create_candidate_view(connection, None)
                connection.execute((REPO_ROOT / "code" / "quality.sql").read_text())
            violations = connection.execute(
                """
                SELECT count(*)
                FROM ranked_key_eligible AS winner
                JOIN ranked_key_eligible AS loser USING (record_id)
                WHERE winner.winner_rank = 1
                  AND loser.winner_rank > 1
                  AND (
                      winner.ingest_ts < loser.ingest_ts
                      OR (
                          winner.ingest_ts = loser.ingest_ts
                          AND (
                              winner.source_object > loser.source_object
                              OR (
                                  winner.source_object = loser.source_object
                                  AND winner.source_row > loser.source_row
                              )
                          )
                      )
                  )
                """
            ).fetchone()[0]
        self.assertEqual(violations, 0)


    def test_after_measurement_includes_reference_coverage_and_chart(self):
        import duckdb

        candidate = self.tmp_path / "curated.parquet"
        with duckdb.connect(":memory:") as connection:
            connection.execute(
                f"""
                COPY (
                    SELECT
                        'R000801'::VARCHAR AS record_id,
                        'S01'::VARCHAR AS sensor_id,
                        'zone-1'::VARCHAR AS site,
                        TIMESTAMP '2026-02-02 00:00:00' AS event_time_utc,
                        TIMESTAMP '2026-02-02 00:01:00' AS ingest_time_utc,
                        18.10::DECIMAL(8, 2) AS temperature_c,
                        false AS is_late,
                        's3://research-raw/lab2/inputs/batch-01/observations_a.csv'::VARCHAR AS source_object,
                        801::BIGINT AS source_row,
                        repeat('a', 64)::VARCHAR AS source_sha256
                ) TO '{candidate.as_posix()}' (FORMAT PARQUET)
                """
            )

        quarantine = self.tmp_path / "quarantine.csv"
        quarantine.write_text("source_object,source_row,primary_reason\n", encoding="utf-8")
        duplicates = self.tmp_path / "duplicates.csv"
        duplicates.write_text(
            "source_object,source_row,winner_source_object,winner_source_row,reason\n",
            encoding="utf-8",
        )
        output_dir = self.tmp_path / "outputs" / "t2_quality"

        run_quality.run(
            [
                "--data-dir",
                str(REPO_ROOT / "input"),
                "--contract",
                str(REPO_ROOT / "contract" / "contract.json"),
                "--output-dir",
                str(output_dir),
                "--evidence-dir",
                str(self.tmp_path / "evidence" / "T2" / "before"),
                "--report",
                str(self.tmp_path / "reports" / "T2_report.md"),
                "--candidate",
                str(candidate),
                "--quarantine",
                str(quarantine),
                "--duplicates",
                str(duplicates),
                "--run-id",
                "532555de-fef9-4e72-9cb0-65431cc7642f",
            ]
        )

        after = json.loads((output_dir / "quality_after.json").read_text())
        metrics = {row["rule_id"]: row for row in after["metrics"]}
        self.assertEqual(metrics["Q_REFERENCE_COVERAGE"]["numerator"], 1)
        self.assertEqual(metrics["Q_REFERENCE_COVERAGE"]["denominator"], 100)
        self.assertEqual(metrics["Q_REFERENCE_AGREEMENT"]["numerator"], 1)
        self.assertEqual(metrics["Q_REFERENCE_AGREEMENT"]["denominator"], 1)
        self.assertEqual(after["remaining_late_observations"]["count"], 0)
        self.assertTrue((output_dir / "before_after.csv").is_file())
        self.assertTrue((output_dir / "before_after.svg").is_file())
        self.assertTrue((self.tmp_path / "evidence" / "T2" / "after" / "run-record.json").is_file())
