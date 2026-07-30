#!/usr/bin/env bash
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORT_RUNNER="${RUNE_LOCAL_K8S_REPORT_RUNNER:-$ROOT_DIR/scripts/run-local-k8s-integration-report.sh}"
COMPOSE_FILE="$ROOT_DIR/docker-compose/docker-compose.fake-k8s.yml"
COMPOSE_PROJECT="rune-fake-k8s"
MATRIX_REPORT_ROOT="${RUNE_LOCAL_K8S_MATRIX_REPORT_DIR:-$ROOT_DIR/test-reports/local-k8s-version-matrix}"
RUN_ID="$(date -u +"%Y%m%dT%H%M%SZ")"
RUN_DIR="$MATRIX_REPORT_ROOT/$RUN_ID"
REPORT_MD="$RUN_DIR/report.md"
REPORT_JSON="$RUN_DIR/report.json"
RESULTS_JSONL="$RUN_DIR/results.jsonl"
CLEANUP_LOG="$RUN_DIR/cleanup.log"

MINIMUM_K3S_IMAGE="${RUNE_MINIMUM_K3S_IMAGE:-rancher/k3s:v1.34.9-k3s1}"
MINIMUM_KUBERNETES_VERSION="${RUNE_MINIMUM_KUBERNETES_VERSION:-v1.34.9}"
CURRENT_K3S_IMAGE="${RUNE_CURRENT_K3S_IMAGE:-rancher/k3s:v1.36.2-k3s1}"
CURRENT_KUBERNETES_VERSION="${RUNE_CURRENT_KUBERNETES_VERSION:-v1.36.2}"

FAILURES=0
ACTIVE_IMAGE="$CURRENT_K3S_IMAGE"

json_string() {
  python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
}

validate_lane() {
  local label="$1"
  local image="$2"
  local expected_version="$3"

  if [[ -z "$image" || "$image" == *":latest" || "$image" == *":stable" ]]; then
    echo "$label lane must use an exact pinned K3s image, not latest/stable." >&2
    exit 64
  fi
  if [[ ! "$expected_version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "$label lane has an invalid Kubernetes version: $expected_version" >&2
    exit 64
  fi
}

clean_generated_files() {
  rm -f "$ROOT_DIR/docker-compose/generated/orbit-seeded.ok" \
    "$ROOT_DIR/docker-compose/generated/lattice-seeded.ok" \
    "$ROOT_DIR/docker-compose/generated/orbit-internal.yaml" \
    "$ROOT_DIR/docker-compose/generated/lattice-internal.yaml" \
    "$ROOT_DIR/docker-compose/generated/orbit-host.yaml" \
    "$ROOT_DIR/docker-compose/generated/lattice-host.yaml" \
    "$ROOT_DIR/docker-compose/generated/rune-fake-kubeconfig.yaml"
}

cleanup_stack() {
  local cleanup_status=0
  {
    printf 'Cleaning Docker Compose project %s\n' "$COMPOSE_PROJECT"
    RUNE_K3S_IMAGE="$ACTIVE_IMAGE" \
      docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" down -v --remove-orphans
  } > "$CLEANUP_LOG" 2>&1 || cleanup_status=$?
  clean_generated_files
  return "$cleanup_status"
}

emergency_cleanup() {
  local status=$?
  trap - EXIT INT TERM
  cleanup_stack || true
  exit "$status"
}

report_value() {
  local report_json="$1"
  local context="$2"
  python3 -c '
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    report = json.load(handle)
print(report.get("dockerCompose", {}).get("observedServerVersions", {}).get(sys.argv[2], ""))
' "$report_json" "$context"
}

append_result() {
  local label="$1"
  local status="$2"
  local exit_code="$3"
  local image="$4"
  local expected_version="$5"
  local orbit_version="$6"
  local lattice_version="$7"
  local report_md="$8"
  local report_json="$9"
  local runner_log="${10}"

  {
    printf '{"label":%s,' "$(json_string "$label")"
    printf '"status":%s,' "$(json_string "$status")"
    printf '"exitCode":%s,' "$exit_code"
    printf '"image":%s,' "$(json_string "$image")"
    printf '"expectedKubernetesVersion":%s,' "$(json_string "$expected_version")"
    printf '"observedServerVersions":{'
    printf '"fake-orbit-mesh":%s,' "$(json_string "$orbit_version")"
    printf '"fake-lattice-spark":%s},' "$(json_string "$lattice_version")"
    printf '"reportMarkdown":%s,' "$(json_string "$report_md")"
    printf '"reportJSON":%s,' "$(json_string "$report_json")"
    printf '"runnerLog":%s}\n' "$(json_string "$runner_log")"
  } >> "$RESULTS_JSONL"
}

append_result_section() {
  local label="$1"
  local status="$2"
  local image="$3"
  local expected_version="$4"
  local orbit_version="$5"
  local lattice_version="$6"
  local report_md="$7"
  local runner_log="$8"
  local relative_report="${report_md#$RUN_DIR/}"
  local relative_log="${runner_log#$RUN_DIR/}"

  {
    printf '\n## %s\n\n' "$label"
    printf -- '- Status: `%s`\n' "$status"
    printf -- '- K3s image: `%s`\n' "$image"
    printf -- '- Expected Kubernetes version: `%s`\n' "$expected_version"
    printf -- '- Observed fake-orbit-mesh version: `%s`\n' "${orbit_version:-not recorded}"
    printf -- '- Observed fake-lattice-spark version: `%s`\n' "${lattice_version:-not recorded}"
    if [[ -n "$report_md" ]]; then
      printf -- '- Integration report: [`%s`](%s)\n' "$relative_report" "$relative_report"
    fi
    printf -- '- Runner log: [`%s`](%s)\n' "$relative_log" "$relative_log"
  } >> "$REPORT_MD"
}

run_lane() {
  local label="$1"
  local image="$2"
  local expected_version="$3"
  local lane_root="$RUN_DIR/$label"
  local runner_log="$RUN_DIR/$label-runner.log"
  local exit_code
  local status
  local report_md=""
  local report_json=""
  local orbit_version=""
  local lattice_version=""

  ACTIVE_IMAGE="$image"
  mkdir -p "$lane_root"

  printf '\nRunning %s lane with %s (%s)\n' "$label" "$image" "$expected_version"
  RUNE_K3S_IMAGE="$image" \
    RUNE_EXPECTED_KUBERNETES_VERSION="$expected_version" \
    RUNE_SKIP_DOCKER_FAKE_K8S=0 \
    RUNE_RESET_DOCKER_FAKE_K8S=1 \
    RUNE_LOCAL_K8S_REPORT_DIR="$lane_root" \
    "$REPORT_RUNNER" > >(tee "$runner_log") 2>&1
  exit_code=$?

  report_md="$(find "$lane_root" -mindepth 2 -maxdepth 2 -name report.md -print | sort | tail -n 1)"
  report_json="$(find "$lane_root" -mindepth 2 -maxdepth 2 -name report.json -print | sort | tail -n 1)"

  if [[ -n "$report_json" ]]; then
    orbit_version="$(report_value "$report_json" fake-orbit-mesh)"
    lattice_version="$(report_value "$report_json" fake-lattice-spark)"
  fi

  if [[ $exit_code -eq 0 \
      && "$orbit_version" == "$expected_version"+k3s* \
      && "$lattice_version" == "$expected_version"+k3s* ]]; then
    status="passed"
  else
    status="failed"
    FAILURES=$((FAILURES + 1))
  fi

  append_result \
    "$label" "$status" "$exit_code" "$image" "$expected_version" \
    "$orbit_version" "$lattice_version" "$report_md" "$report_json" "$runner_log"
  append_result_section \
    "$label" "$status" "$image" "$expected_version" \
    "$orbit_version" "$lattice_version" "$report_md" "$runner_log"
}

write_report_json() {
  local result="$1"
  {
    printf '{\n'
    printf '  "schemaVersion": 1,\n'
    printf '  "runId": %s,\n' "$(json_string "$RUN_ID")"
    printf '  "generatedAt": %s,\n' "$(json_string "$(date -u +"%Y-%m-%dT%H:%M:%SZ")")"
    printf '  "result": %s,\n' "$(json_string "$result")"
    printf '  "lanes": [\n'
    local first=1
    while IFS= read -r line; do
      if [[ $first -eq 0 ]]; then
        printf ',\n'
      fi
      first=0
      printf '    %s' "$line"
    done < "$RESULTS_JSONL"
    printf '\n  ]\n'
    printf '}\n'
  } > "$REPORT_JSON"
}

validate_lane minimum "$MINIMUM_K3S_IMAGE" "$MINIMUM_KUBERNETES_VERSION"
validate_lane current "$CURRENT_K3S_IMAGE" "$CURRENT_KUBERNETES_VERSION"
if [[ "$MINIMUM_K3S_IMAGE" == "$CURRENT_K3S_IMAGE" \
    || "$MINIMUM_KUBERNETES_VERSION" == "$CURRENT_KUBERNETES_VERSION" ]]; then
  echo "Minimum and current lanes must use two different Kubernetes versions." >&2
  exit 64
fi
if [[ ! -x "$REPORT_RUNNER" ]]; then
  echo "Integration report runner is not executable: $REPORT_RUNNER" >&2
  exit 64
fi

mkdir -p "$RUN_DIR"
: > "$RESULTS_JSONL"
cat > "$REPORT_MD" <<EOF
# Local Kubernetes Version Matrix Report

- Run id: \`$RUN_ID\`
- Generated: \`$(date -u +"%Y-%m-%dT%H:%M:%SZ")\`
- Execution: sequential, with fresh Docker volumes for every lane
- Safety: the delegated integration runner disables real-cluster and cloud opt-ins
EOF

trap emergency_cleanup EXIT INT TERM

run_lane minimum "$MINIMUM_K3S_IMAGE" "$MINIMUM_KUBERNETES_VERSION"
run_lane current "$CURRENT_K3S_IMAGE" "$CURRENT_KUBERNETES_VERSION"

if ! cleanup_stack; then
  FAILURES=$((FAILURES + 1))
fi
trap - EXIT INT TERM

RESULT="passed"
if [[ $FAILURES -ne 0 ]]; then
  RESULT="failed"
fi

cat >> "$REPORT_MD" <<EOF

## Result

\`$RESULT\`

## Rerun

\`\`\`bash
scripts/run-local-k8s-version-matrix.sh
\`\`\`

The matrix always resets the shared local stack between versions and removes it when finished.
EOF

write_report_json "$RESULT"

printf '\nHuman matrix report: %s\n' "$REPORT_MD"
printf 'JSON matrix report: %s\n' "$REPORT_JSON"

if [[ "$RESULT" != "passed" ]]; then
  exit 1
fi
