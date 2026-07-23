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

## Selected release KPIs

Run the tracked REST, native-auth, native-import, and resource-table release
gate with:

```bash
CONFIGURATION=release RUNS=3 scripts/run-selected-kpis.sh
```

The runner stores local logs and privacy-safe environment metadata under the
ignored `test-reports/` directory. Baseline policy is documented in
`scripts/SELECTED_KPI_BASELINE.md`.
