# Selected release KPI baseline

The tracked runner is `scripts/run-selected-kpis.sh`. It executes the small
REST, native-auth, native-import, and resource-table set that must stay green
before a release candidate is accepted.

## Run the gate

Use the release configuration for product-facing verification:

```bash
CONFIGURATION=release RUNS=3 scripts/run-selected-kpis.sh
```

Use one debug run while iterating:

```bash
CONFIGURATION=debug RUNS=1 scripts/run-selected-kpis.sh
```

Generated logs, the pass/fail manifest, and environment metadata are written
under `test-reports/`. That directory is ignored because timings and paths are
machine-local.

## Recorded environment

Each run records the UTC timestamp, configuration, run count, short Git
revision, clean/dirty state, macOS version and build, Swift version,
architecture, logical CPU count, physical memory, and CPU model. It does not
record a user name, host name, serial number, kubeconfig path, context name, or
credential data.

## Acceptance baseline

All selected filters must pass on every run. The individual XCTest methods own
their debug and release budgets so the runner cannot silently diverge from the
checked-in gate.

The selected set covers:

- REST metrics recording, retention churn, grouping, and privacy-safe debug
  highlights.
- YAML editor validation under a 100-edit burst. The budget is 350 ms in debug
  and 180 ms in release, with exactly zero Kubernetes requests.
- Ten explicit Kubernetes server dry-runs. The budget is 600 ms in debug and
  300 ms in release, with ten dry-run PATCH requests and zero apply requests.
- Projection of 500 synthetic Kubernetes validation failures into concise,
  line-scoped editor issues. The budget is 400 ms in debug and 180 ms in
  release.
- Native cloud-import diagnostic projection, admission, and end-to-end review
  binding, including the headless AKS/GKE parity flow. The parity KPI budget is
  1.2 seconds in debug and 0.6 seconds in release.
- AWS, GCP, AKS, OIDC, and exec-credential native-auth hot paths.
- Resource column solving and interactive resize preview.

When a budget intentionally changes, update the assertion and this baseline in
the same change, record the reason, and run the complete selected set at least
three times in release mode.
