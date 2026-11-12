# macOS Build Script

`build-macos-app.sh` creates a local Rune app bundle for development and testing.

## Build

```bash
scripts/build-macos-app.sh
```

Output:

```text
dist/Rune.app
```

Open it with:

```bash
open dist/Rune.app
```

## Process UI checks

The process UI helper `rune-ui-smoke-ax.swift` also checks changes to Settings
without restarting the app (`enable-skip-cluster`, then `skip-cluster-nav`).
Its `write-dialog` check requires the disposable `fake-orbit-mesh` context,
reviews the first production confirmation, and cancels the final action.

## Selected release KPIs

Run the tracked REST, native-auth, native-import, and resource-table release
gate with:

```bash
CONFIGURATION=release RUNS=3 scripts/run-selected-kpis.sh
```

The runner stores local logs and privacy-safe environment metadata under the
ignored `test-reports/` directory. Baseline policy is documented in
`scripts/SELECTED_KPI_BASELINE.md`.

## Compare benchmark measurements

`compare-kpi-benchmarks.py` compares explicit `RUNE_BENCHMARK ` JSON records
in XCTest logs. Each record contains a synthetic `name`, `configuration`
(`release` or `debug`), positive `iterations` per sample, and a nonempty
`samples_seconds` array of finite positive batch durations.

```bash
python3 scripts/compare-kpi-benchmarks.py \
  --before test-reports/before.log --after test-reports/after.log \
  --before-label baseline --after-label candidate \
  --json-output test-reports/comparison.json \
  --markdown-output test-reports/comparison.md
```

Each input option accepts multiple logs. Use equivalent hardware and workloads;
labels and metric names must be synthetic identifiers. The report pools samples
for matching names, configurations and iteration counts, reporting median, p95
(nearest rank), throughput and percentage changes. Negative latency changes
mean faster. Small sample counts do not establish statistical significance.

Exit code 0 means the comparison is complete, not that a performance target
passed. Exit code 1 preserves failed, skipped or incomplete XCTest runs, invalid records,
missing measurements and incompatible workloads; exit code 2 signals a CLI or
output error. Reports omit raw log content and input paths. Existing XCTest
clock output alone is not sufficient: build configuration and workload must be
explicit in benchmark records. Without output options, Markdown goes to stdout.

Run the synthetic report tests with
`python3 scripts/test-compare-kpi-benchmarks.py`.

## OAuth and import tuning KPIs

The selected runner includes the existing 200-initialization and rapid table
selection gates, the import metadata projection gate, and the native OAuth
foundation checks. No browser sign-in or real provider is used by these tests.

OAuth round-trip and sheet-churn benchmarks each report seven samples of 500
operations. Every round trip must return the expected synthetic code; canceled
sheets must have no active authorization. The p95 budget is 500 ms per batch in
release and 1 second in debug. A separate functional KPI rejects 128/128 stale
cross-sheet callbacks. Active authorization is limited to 16 attempts by default
(configurable from 1 to 64), while consumed, canceled and expired attempts release
their slots. Closed sheets do not retain per-sheet generation history.

Import projection reports seven samples of duplicate resolution, review
aggregation and preference-name projection for 40 synthetic files containing
160 contexts. Its existing best-of-three gate remains 90 ms in release and
180 ms in debug; the median/p95 report supplements that gate. Fixture creation
and preflight remain outside the timed resolution workload on both versions.

For a before/after comparison, build the same benchmark sources against both
production revisions. Preserve the old executable before rebuilding, run old
and new executables serially in alternating order, and keep their logs separate.
Record source and benchmark fingerprints alongside local results. Exclude
compilation from timing and retain failed runs in the comparison. Use the
ignored `test-reports/` directory for local evidence.
