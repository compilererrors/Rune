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
