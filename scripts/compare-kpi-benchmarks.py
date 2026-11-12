#!/usr/bin/env python3
"""Compare explicit, synthetic Rune benchmark records from XCTest logs."""

import argparse
import json
import math
import re
import sys
from pathlib import Path


MARKER = "RUNE_BENCHMARK "
SAFE_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
SUMMARY = re.compile(r"Executed (\d+) tests?, with (?:(\d+) tests? skipped and )?(\d+) failures?")
COMPLETED = re.compile(r"Test Suite '(?:All tests|Selected tests)' (?:passed|failed) at ")
FAILED = re.compile(r"Test (?:Suite|Case) .+ failed(?: at | \()|^error:")


def identifier(value):
    if not isinstance(value, str) or not SAFE_IDENTIFIER.fullmatch(value):
        raise ValueError("use a synthetic identifier of 1–128 letters, digits, dots, underscores or hyphens")
    return value


def validate_record(record):
    if not isinstance(record, dict):
        raise ValueError("record must be a JSON object")
    name = identifier(record.get("name"))
    configuration = record.get("configuration")
    if configuration not in ("release", "debug"):
        raise ValueError("configuration must be release or debug")
    iterations = record.get("iterations")
    if type(iterations) is not int or iterations <= 0:
        raise ValueError("iterations must be a positive integer")
    samples = record.get("samples_seconds")
    if not isinstance(samples, list) or not samples:
        raise ValueError("samples_seconds must be a nonempty array")
    if any(type(value) not in (int, float) or value <= 0 or not math.isfinite(value) for value in samples):
        raise ValueError("samples_seconds must contain finite positive numbers")
    return name, configuration, iterations, samples


def read_logs(paths):
    metrics = {}
    issues = []
    for log_number, path in enumerate(paths, 1):
        completed = False
        executed = False
        failed = False
        skipped = 0
        records = 0
        try:
            with Path(path).open(encoding="utf-8", errors="replace") as log:
                for line_number, line in enumerate(log, 1):
                    summary = SUMMARY.search(line)
                    if summary:
                        summary_skipped = int(summary.group(2) or 0)
                        executed |= int(summary.group(1)) > summary_skipped
                        failed |= int(summary.group(3)) > 0
                        skipped = max(skipped, summary_skipped)
                    completed |= bool(COMPLETED.search(line))
                    failed |= bool(FAILED.search(line))
                    if MARKER not in line:
                        continue
                    try:
                        record = json.loads(line.split(MARKER, 1)[1])
                        name, configuration, iterations, samples = validate_record(record)
                    except (ValueError, OverflowError):
                        issues.append({"code": "invalid_record", "log": log_number, "line": line_number})
                        continue
                    records += 1
                    metric = metrics.setdefault(name, {"workloads": {}, "record_count": 0})
                    metric["record_count"] += 1
                    metric["workloads"].setdefault((configuration, iterations), []).extend(samples)
        except OSError:
            issues.append({"code": "unreadable_log", "log": log_number})
            continue
        if failed:
            issues.append({"code": "test_failure", "log": log_number})
        if skipped:
            issues.append({"code": "skipped_tests", "log": log_number, "count": skipped})
        if not completed or not executed:
            issues.append({"code": "incomplete_test_run", "log": log_number})
        if not records:
            issues.append({"code": "no_benchmark_records", "log": log_number})
    return metrics, issues


def summarize(metric):
    workloads = metric["workloads"]
    if len(workloads) != 1:
        return {"record_count": metric["record_count"], "workload_count": len(workloads)}
    (configuration, iterations), samples = next(iter(workloads.items()))
    ordered = sorted(samples)
    midpoint = len(ordered) // 2
    median = ordered[midpoint]
    if len(ordered) % 2 == 0:
        median = ordered[midpoint - 1] + (median - ordered[midpoint - 1]) / 2
    p95 = ordered[math.ceil(len(ordered) * 0.95) - 1]
    summary = {
        "configuration": configuration,
        "iterations": iterations,
        "record_count": metric["record_count"],
        "sample_count": len(ordered),
    }
    try:
        values = {
            "median_seconds": median,
            "p95_seconds": p95,
            "median_seconds_per_iteration": median / iterations,
            "median_iterations_per_second": iterations / median,
        }
        if any(not math.isfinite(value) or value <= 0 for value in values.values()):
            raise ValueError("unrepresentable derived measurement")
        summary.update(values)
    except (ValueError, OverflowError, ZeroDivisionError):
        summary["error"] = "numeric_range"
    return summary


def compare(before_paths, after_paths, before_label, after_label):
    before, before_issues = read_logs(before_paths)
    after, after_issues = read_logs(after_paths)
    result = {
        "schema_version": 1,
        "status": "complete",
        "before": {"label": identifier(before_label), "log_count": len(before_paths), "issues": before_issues},
        "after": {"label": identifier(after_label), "log_count": len(after_paths), "issues": after_issues},
        "method": {
            "aggregation": "Pool all samples for the same name, configuration and iterations; each sample is one timed batch.",
            "p95": "Nearest rank: sorted samples at ceil(0.95 * sample_count), using one-based indexing.",
            "delta_percent": "100 * (after / before - 1); negative latency deltas mean faster.",
            "interpretation": "A complete comparison is not a performance gate or evidence of statistical significance.",
        },
        "metrics": [],
    }
    for name in sorted(before.keys() | after.keys()):
        entry = {"name": name, "status": "comparable"}
        old = before.get(name)
        new = after.get(name)
        if old:
            entry["before"] = summarize(old)
        if new:
            entry["after"] = summarize(new)
        if old is None:
            entry["status"] = "missing_before"
        elif new is None:
            entry["status"] = "missing_after"
        elif len(old["workloads"]) != 1 or old["workloads"].keys() != new["workloads"].keys():
            entry["status"] = "incompatible_workload"
        elif "error" in entry["before"] or "error" in entry["after"]:
            entry["status"] = "invalid_numeric_range"
        else:
            deltas = {
                key: 100 * (entry["after"][key] / entry["before"][key] - 1)
                for key in ("median_seconds", "p95_seconds", "median_iterations_per_second")
            }
            if all(math.isfinite(value) for value in deltas.values()):
                entry["delta_percent"] = deltas
            else:
                entry["status"] = "invalid_numeric_range"
        result["metrics"].append(entry)
    if before_issues or after_issues or not result["metrics"] or any(metric["status"] != "comparable" for metric in result["metrics"]):
        result["status"] = "incomplete"
    return result


def render_markdown(result):
    def latency(seconds):
        if seconds > sys.float_info.max / 1000:
            return f"{seconds:.6g} s"
        return f"{seconds * 1000:.6g} ms"

    lines = ["# Benchmark comparison", "", f"Before: `{result['before']['label']}`. After: `{result['after']['label']}`.", ""]
    lines.extend([f"Comparison status: **{result['status']}**. This is a measurement report, not a performance gate.", ""])
    lines.extend(["## Method", "", result["method"]["aggregation"], "", result["method"]["p95"], "", result["method"]["delta_percent"], ""])
    lines.extend(["Small sample counts make p95 and changes sensitive to noise. Compare equivalent machines, workloads and build settings.", ""])
    for side in ("before", "after"):
        if result[side]["issues"]:
            lines.extend([f"## {side.capitalize()} input issues", ""])
            for issue in result[side]["issues"]:
                location = f"log {issue['log']}"
                if "line" in issue:
                    location += f", line {issue['line']}"
                if "count" in issue:
                    location += f", maximum reported count {issue['count']}"
                lines.append(f"- {issue['code']} ({location}).")
            lines.append("")
    for metric in result["metrics"]:
        lines.extend([f"## {metric['name']}", "", f"Status: `{metric['status']}`.", ""])
        for side in ("before", "after"):
            value = metric.get(side)
            if value is None:
                lines.extend([f"{side.capitalize()}: missing.", ""])
            elif "error" in value:
                lines.extend([f"{side.capitalize()}: derived measurements exceed the supported numeric range.", ""])
            elif "median_seconds" not in value:
                lines.extend([f"{side.capitalize()}: incompatible workloads in {value['record_count']} records.", ""])
            else:
                lines.extend([
                    f"{side.capitalize()}: median {latency(value['median_seconds'])}; p95 {latency(value['p95_seconds'])}; "
                    f"{value['median_iterations_per_second']:.6g} iterations/s. "
                    f"{value['sample_count']} samples from {value['record_count']} records; "
                    f"{value['iterations']} iterations/sample; `{value['configuration']}`.", "",
                ])
        if "delta_percent" in metric:
            delta = metric["delta_percent"]
            lines.extend([
                f"Change: median {delta['median_seconds']:+.2f}%; p95 {delta['p95_seconds']:+.2f}%; "
                f"throughput {delta['median_iterations_per_second']:+.2f}%.", "",
            ])
    if not result["metrics"]:
        lines.extend(["## Metrics", "", "No valid benchmark metrics were found.", ""])
    return "\n".join(lines)


def parse_identifier(value):
    try:
        return identifier(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from None


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", nargs="+", required=True, metavar="LOG")
    parser.add_argument("--after", nargs="+", required=True, metavar="LOG")
    parser.add_argument("--before-label", required=True, type=parse_identifier)
    parser.add_argument("--after-label", required=True, type=parse_identifier)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    args = parser.parse_args(argv)
    result = compare(args.before, args.after, args.before_label, args.after_label)
    markdown = render_markdown(result)
    try:
        if args.json_output:
            args.json_output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        if args.markdown_output:
            args.markdown_output.write_text(markdown, encoding="utf-8")
        if not args.json_output and not args.markdown_output:
            print(markdown, end="")
    except (OSError, ValueError, OverflowError):
        print("Could not write comparison output; check output locations and numeric input ranges.", file=sys.stderr)
        return 2
    return 0 if result["status"] == "complete" else 1


if __name__ == "__main__":
    sys.exit(main())
