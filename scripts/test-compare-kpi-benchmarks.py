"""Synthetic regression tests for benchmark report integrity."""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SPEC = importlib.util.spec_from_file_location("compare_kpis", Path(__file__).with_name("compare-kpi-benchmarks.py"))
REPORT = importlib.util.module_from_spec(SPEC)
sys.dont_write_bytecode = True
SPEC.loader.exec_module(REPORT)


class BenchmarkComparisonTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.counter = 0

    def log(self, records, *, complete=True, failed=False, prefix=""):
        self.counter += 1
        path = Path(self.directory.name) / f"input-{self.counter}.log"
        lines = [prefix] + ["RUNE_BENCHMARK " + json.dumps(record) for record in records]
        if failed:
            lines.append("Test Case '-[SyntheticTests testWorkload]' failed (0.001 seconds).")
        if complete:
            lines.extend([
                "Test Suite 'Selected tests' passed at 2000-01-01 00:00:00.000.",
                "Executed 1 test, with 0 failures (0 unexpected) in 0.001 (0.001) seconds",
            ])
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def record(self, **changes):
        result = {"name": "synthetic.workload", "samples_seconds": [0.1, 0.2, 0.3], "iterations": 100, "configuration": "release"}
        result.update(changes)
        return result

    def compare(self, before, after):
        return REPORT.compare(before, after, "baseline", "candidate")

    def test_aggregates_repeated_runs_and_uses_nearest_rank_p95(self):
        before = [self.log([self.record(samples_seconds=[1, 2])]), self.log([self.record(samples_seconds=[3, 4])])]
        after = [self.log([self.record(samples_seconds=[0.5, 1, 1.5, 2])])]
        result = self.compare(before, after)
        metric = result["metrics"][0]
        self.assertEqual(result["status"], "complete")
        self.assertEqual(metric["before"]["sample_count"], 4)
        self.assertEqual(metric["before"]["record_count"], 2)
        self.assertEqual(metric["before"]["median_seconds"], 2.5)
        self.assertEqual(metric["before"]["p95_seconds"], 4)
        self.assertEqual(metric["delta_percent"]["median_seconds"], -50)
        self.assertEqual(metric["delta_percent"]["median_iterations_per_second"], 100)

    def test_p95_uses_the_nineteenth_of_twenty_ordered_samples(self):
        log = self.log([self.record(samples_seconds=list(range(20, 0, -1)))])
        result = self.compare([log], [log])
        self.assertEqual(result["metrics"][0]["before"]["p95_seconds"], 19)

    def test_failures_remain_visible_even_with_complete_valid_measurements(self):
        result = self.compare([self.log([self.record()], failed=True)], [self.log([self.record()])])
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["before"]["issues"][0]["code"], "test_failure")
        self.assertEqual(result["metrics"][0]["status"], "comparable")

    def test_completed_failed_suites_are_distinct_from_truncated_runs(self):
        for suite in ("Selected tests", "All tests"):
            with self.subTest(suite=suite):
                path = self.log([self.record()], failed=True)
                path.write_text(path.read_text().replace(
                    "Test Suite 'Selected tests' passed at",
                    f"Test Suite '{suite}' failed at",
                ).replace("with 0 failures", "with 1 failure"))
                result = self.compare([path], [self.log([self.record()])])
                self.assertEqual(result["status"], "incomplete")
                self.assertEqual(result["before"]["issues"], [{"code": "test_failure", "log": 1}])

        result = self.compare([self.log([self.record()], failed=True, complete=False)], [self.log([self.record()])])
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(
            [issue["code"] for issue in result["before"]["issues"]],
            ["test_failure", "incomplete_test_run"],
        )

    def test_summary_failure_count_is_preserved(self):
        path = self.log([self.record()])
        path.write_text(path.read_text().replace("with 0 failures", "with 1 failure"))
        result = self.compare([path], [self.log([self.record()])])
        self.assertEqual(result["before"]["issues"][0]["code"], "test_failure")

    def test_skipped_tests_are_preserved_without_summing_suite_summaries(self):
        path = self.log([self.record()])
        path.write_text(path.read_text() + "\n".join([
            "Executed 2 tests, with 1 test skipped and 0 failures (0 unexpected) in 0.001 (0.001) seconds",
            "Executed 4 tests, with 2 tests skipped and 0 failures (0 unexpected) in 0.001 (0.001) seconds",
            "Executed 4 tests, with 2 tests skipped and 0 failures (0 unexpected) in 0.001 (0.001) seconds",
        ]))
        result = self.compare([path], [self.log([self.record()])])
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["before"]["issues"], [{"code": "skipped_tests", "log": 1, "count": 2}])

    def test_failure_and_all_skipped_summaries_are_not_success(self):
        for summary, expected in [
            ("Executed 2 tests, with 1 test skipped and 1 failure", "test_failure"),
            ("Executed 1 test, with 1 test skipped and 0 failures", "incomplete_test_run"),
        ]:
            with self.subTest(summary=summary):
                path = self.log([self.record()])
                path.write_text(path.read_text().replace("Executed 1 test, with 0 failures", summary))
                result = self.compare([path], [self.log([self.record()])])
                self.assertEqual(result["status"], "incomplete")
                self.assertIn(expected, [issue["code"] for issue in result["before"]["issues"]])

    def test_truncated_and_empty_runs_are_incomplete(self):
        for records, complete, expected in [([self.record()], False, "incomplete_test_run"), ([], True, "no_benchmark_records")]:
            with self.subTest(expected=expected):
                result = self.compare([self.log(records, complete=complete)], [self.log([self.record()])])
                self.assertEqual(result["status"], "incomplete")
                self.assertIn(expected, [issue["code"] for issue in result["before"]["issues"]])

    def test_incompatible_configuration_iterations_and_mixed_runs(self):
        for changes in [{"configuration": "debug"}, {"iterations": 200}]:
            with self.subTest(changes=changes):
                old = self.log([self.record()])
                new = self.log([self.record(**changes)])
                for before, after in [([old], [new]), ([old, new], [old]), ([old], [old, new])]:
                    result = self.compare(before, after)
                    self.assertEqual(result["status"], "incomplete")
                    self.assertEqual(result["metrics"][0]["status"], "incompatible_workload")
                    self.assertNotIn("delta_percent", result["metrics"][0])

    def test_missing_metrics_are_preserved_in_both_directions(self):
        result = self.compare([self.log([self.record(name="only.before")])], [self.log([self.record(name="only.after")])])
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual({metric["status"] for metric in result["metrics"]}, {"missing_before", "missing_after"})

    def test_invalid_samples_and_workloads_are_rejected(self):
        cases = [
            {"samples_seconds": samples} for samples in [[], [0], [-1], [float("nan")], [float("inf")], [True], ["0.1"], "0.1"]
        ] + [{"iterations": value} for value in [0, -1, True, 1.5]] + [{"configuration": "unknown"}]
        for changes in cases:
            with self.subTest(changes=changes):
                result = self.compare([self.log([self.record(**changes)])], [self.log([self.record()])])
                self.assertEqual(result["status"], "incomplete")
                self.assertEqual(result["before"]["issues"][0]["code"], "invalid_record")

    def test_reports_exclude_raw_log_content_paths_and_unknown_fields(self):
        marker = "synthetic-sensitive-marker"
        record = self.record(extra=marker)
        old = self.log([record], prefix=f"/synthetic/private/{marker}\nRUNE_BENCHMARK invalid-{marker}")
        result = self.compare([old], [self.log([self.record()])])
        for output in (json.dumps(result), REPORT.render_markdown(result)):
            self.assertNotIn(marker, output)
            self.assertNotIn(str(old), output)
            self.assertNotIn(self.directory.name, output)
        self.assertEqual(result["before"]["issues"][0]["code"], "invalid_record")

    def test_unsafe_metric_names_and_labels_are_rejected(self):
        for value in ["/synthetic/path", "line\nbreak", "<tag>", "", "A" * 129]:
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    REPORT.identifier(value)

    def test_extreme_finite_inputs_do_not_emit_nonfinite_json(self):
        for before, after in [([5e-324], [1]), ([1e-308], [1e308])]:
            with self.subTest(before=before):
                result = self.compare(
                    [self.log([self.record(samples_seconds=before, iterations=1)])],
                    [self.log([self.record(samples_seconds=after, iterations=1)])],
                )
                self.assertEqual(result["status"], "incomplete")
                self.assertEqual(result["metrics"][0]["status"], "invalid_numeric_range")
                self.assertNotIn("delta_percent", result["metrics"][0])
                json.dumps(result, allow_nan=False)

    def test_median_does_not_overflow_for_large_finite_samples(self):
        log = self.log([self.record(samples_seconds=[1e308, 1e308], iterations=1)])
        result = self.compare([log], [log])
        self.assertEqual(result["status"], "complete")
        self.assertEqual(result["metrics"][0]["before"]["median_seconds"], 1e308)
        json.dumps(result, allow_nan=False)
        self.assertNotIn("inf", REPORT.render_markdown(result))

    def test_unreadable_input_reports_ordinal_without_path(self):
        missing = Path(self.directory.name) / "synthetic-missing.log"
        result = self.compare([missing], [self.log([self.record()])])
        self.assertEqual(result["status"], "incomplete")
        self.assertEqual(result["before"]["issues"], [{"code": "unreadable_log", "log": 1}])
        self.assertNotIn(str(missing), json.dumps(result))

    def test_cli_writes_json_and_markdown_and_returns_incomplete_exit_code(self):
        old = self.log([self.record()], complete=False)
        new = self.log([self.record()])
        output = Path(self.directory.name)
        status = REPORT.main([
            "--before", str(old), "--after", str(new), "--before-label", "baseline", "--after-label", "candidate",
            "--json-output", str(output / "report.json"), "--markdown-output", str(output / "report.md"),
        ])
        self.assertEqual(status, 1)
        self.assertEqual(json.loads((output / "report.json").read_text())["status"], "incomplete")
        self.assertIn("**incomplete**", (output / "report.md").read_text())


if __name__ == "__main__":
    unittest.main()
