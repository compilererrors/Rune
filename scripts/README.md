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

## App Store release metadata

App Store releases use `packaging/build-app-store.sh`, which reads
`BUNDLE_IDENTIFIER`, `MARKETING_VERSION` and `BUNDLE_VERSION` exclusively from
`.local/signing.env`. Edit that local file to change release metadata; do not
override these values in the environment or derive them from an existing app.
Missing metadata stops the release. Local development and smoke-test builds
continue to use `build-macos-app.sh` separately.

## Selected release KPIs

Run the tracked REST, native-auth, native-import, and resource-table release
gate with:

```bash
CONFIGURATION=release RUNS=3 scripts/run-selected-kpis.sh
```

The runner stores local logs and privacy-safe environment metadata under the
ignored `test-reports/` directory. Baseline policy is documented in
`scripts/SELECTED_KPI_BASELINE.md`.
