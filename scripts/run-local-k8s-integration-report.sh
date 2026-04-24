#!/usr/bin/env bash
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORT_ROOT="${RUNE_LOCAL_K8S_REPORT_DIR:-$ROOT_DIR/test-reports/local-k8s-integration}"
RUN_ID="$(date -u +"%Y%m%dT%H%M%SZ")"
RUN_DIR="$REPORT_ROOT/$RUN_ID"
LOG_DIR="$RUN_DIR/logs"
REPORT_MD="$RUN_DIR/report.md"
REPORT_JSON="$RUN_DIR/report.json"
STEPS_JSONL="$RUN_DIR/steps.jsonl"

COMPOSE_FILE="$ROOT_DIR/docker-compose/docker-compose.fake-k8s.yml"
COMPOSE_PROJECT="rune-fake-k8s"
MERGED_KUBECONFIG="$ROOT_DIR/docker-compose/generated/rune-fake-kubeconfig.yaml"
SCRIPT_STATE_DIR="${RUNE_FAKE_K8S_INTEGRATION_STATE:-/tmp/rune-fake-k8s-integration}"
RESET_DOCKER="${RUNE_RESET_DOCKER_FAKE_K8S:-1}"
SKIP_DOCKER="${RUNE_SKIP_DOCKER_FAKE_K8S:-0}"

FAILURES=0

mkdir -p "$LOG_DIR"
: > "$STEPS_JSONL"

json_string() {
  python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
}

json_number_or_null() {
  if [[ -n "$1" ]]; then
    printf '%s' "$1"
  else
    printf 'null'
  fi
}

append_step_json() {
  local name="$1"
  local status="$2"
  local exit_code="$3"
  local duration="$4"
  local log_path="$5"
  local command="$6"
  local note="$7"

  {
    printf '{"name":%s,' "$(json_string "$name")"
    printf '"status":%s,' "$(json_string "$status")"
    printf '"exitCode":%s,' "$(json_number_or_null "$exit_code")"
    printf '"durationSeconds":%s,' "$(json_number_or_null "$duration")"
    printf '"logPath":%s,' "$(json_string "$log_path")"
    printf '"command":%s,' "$(json_string "$command")"
    printf '"note":%s}\n' "$(json_string "$note")"
  } >> "$STEPS_JSONL"
}

append_step_md() {
  local name="$1"
  local status="$2"
  local exit_code="$3"
  local duration="$4"
  local log_path="$5"
  local note="$6"
  local rel_log="${log_path#$RUN_DIR/}"
  local exit_text="${exit_code:-n/a}"
  local duration_text="${duration:-0}"
  printf '| `%s` | %s | %s | %ss | [%s](%s) | %s |\n' \
    "$name" "$status" "$exit_text" "$duration_text" "$rel_log" "$rel_log" "$note" >> "$REPORT_MD"
}

run_step() {
  local name="$1"
  shift
  local log_file="$LOG_DIR/$name.log"
  local command_text="$*"
  local started
  local ended
  local duration
  local exit_code
  started="$(date +%s)"
  {
    printf '$ %s\n\n' "$command_text"
    "$@"
  } > "$log_file" 2>&1
  exit_code=$?
  ended="$(date +%s)"
  duration=$((ended - started))

  if [[ $exit_code -eq 0 ]]; then
    append_step_json "$name" "passed" "$exit_code" "$duration" "$log_file" "$command_text" ""
    append_step_md "$name" "passed" "$exit_code" "$duration" "$log_file" ""
  else
    FAILURES=$((FAILURES + 1))
    append_step_json "$name" "failed" "$exit_code" "$duration" "$log_file" "$command_text" "See log for stderr/stdout."
    append_step_md "$name" "failed" "$exit_code" "$duration" "$log_file" "See log."
  fi
}

skip_step() {
  local name="$1"
  local note="$2"
  append_step_json "$name" "skipped" "" "" "" "" "$note"
  append_step_md "$name" "skipped" "" "" "" "$note"
}

write_report_header() {
  cat > "$REPORT_MD" <<EOF
# Local Kubernetes Integration Test Report

- Run id: \`$RUN_ID\`
- Generated: \`$(date -u +"%Y-%m-%dT%H:%M:%SZ")\`
- Repository: \`$ROOT_DIR\`
- Script fake-k8s state: \`$SCRIPT_STATE_DIR\`
- Docker Compose kubeconfig: \`$MERGED_KUBECONFIG\`
- Safety gate: tests require \`RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1\` and hard-fail unless kubeconfigs are fake/local only.

## Steps

| Step | Status | Exit | Duration | Log | Note |
| --- | --- | ---: | ---: | --- | --- |
EOF
}

write_report_json() {
  local result="$1"
  local started_at="$2"
  local ended_at="$3"
  {
    printf '{\n'
    printf '  "schemaVersion": 1,\n'
    printf '  "runId": %s,\n' "$(json_string "$RUN_ID")"
    printf '  "generatedAt": %s,\n' "$(json_string "$(date -u +"%Y-%m-%dT%H:%M:%SZ")")"
    printf '  "result": %s,\n' "$(json_string "$result")"
    printf '  "startedAt": %s,\n' "$(json_string "$started_at")"
    printf '  "endedAt": %s,\n' "$(json_string "$ended_at")"
    printf '  "repoRoot": %s,\n' "$(json_string "$ROOT_DIR")"
    printf '  "safety": {\n'
    printf '    "requiresFlag": "RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1",\n'
    printf '    "scriptFakeK8sServers": [".fake.rune.local"],\n'
    printf '    "dockerComposeServers": ["https://127.0.0.1:16443", "https://127.0.0.1:17443"],\n'
    printf '    "defaultKubeconfigDiscovery": "disabled by test environment"\n'
    printf '  },\n'
    printf '  "clusters": [\n'
    printf '    {"name":"fake-orbit-mesh","type":"script-fake","namespace":"alpha-zone"},\n'
    printf '    {"name":"fake-lattice-spark","type":"script-fake","namespace":"delta-zone"},\n'
    printf '    {"name":"fake-orbit-mesh","type":"docker-compose","server":"https://127.0.0.1:16443","namespace":"alpha-zone"},\n'
    printf '    {"name":"fake-lattice-spark","type":"docker-compose","server":"https://127.0.0.1:17443","namespace":"delta-zone"}\n'
    printf '  ],\n'
    printf '  "steps": [\n'
    local first=1
    while IFS= read -r line; do
      if [[ $first -eq 0 ]]; then
        printf ',\n'
      fi
      first=0
      printf '    %s' "$line"
    done < "$STEPS_JSONL"
    printf '\n  ]\n'
    printf '}\n'
  } > "$REPORT_JSON"
}

safe_docker_kubeconfig_check() {
  [[ -f "$MERGED_KUBECONFIG" ]] || return 1
  grep -q 'name: fake-orbit-mesh' "$MERGED_KUBECONFIG" || return 1
  grep -q 'name: fake-lattice-spark' "$MERGED_KUBECONFIG" || return 1
  grep -q 'server: https://127.0.0.1:16443' "$MERGED_KUBECONFIG" || return 1
  grep -q 'server: https://127.0.0.1:17443' "$MERGED_KUBECONFIG" || return 1
}

STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
write_report_header

cd "$ROOT_DIR" || exit 1

run_step build_RuneFakeK8s swift build --product RuneFakeK8s

FAKE_BIN="${RUNE_FAKE_K8S_BINARY:-$ROOT_DIR/.build/debug/RuneFakeK8s}"
if [[ ! -x "$FAKE_BIN" && -x "$ROOT_DIR/.build/arm64-apple-macosx/debug/RuneFakeK8s" ]]; then
  FAKE_BIN="$ROOT_DIR/.build/arm64-apple-macosx/debug/RuneFakeK8s"
fi

run_step script_fake_setup env \
  HOME="$SCRIPT_STATE_DIR/home" \
  RUNE_FAKE_K8S_BINARY="$FAKE_BIN" \
  RUNE_FAKE_K8S_STATE="$SCRIPT_STATE_DIR" \
  bash scripts/rune-fake-k8s.sh setup

run_step script_fake_integration_test env \
  RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 \
  RUNE_FAKE_K8S_BINARY="$FAKE_BIN" \
  swift test --filter LocalKubernetesIntegrationTests/testRuneFakeK8sEventsPointAtExistingPods

if [[ "$SKIP_DOCKER" == "1" ]]; then
  skip_step docker_compose_stack "Skipped because RUNE_SKIP_DOCKER_FAKE_K8S=1."
  skip_step docker_compose_integration_test "Skipped because RUNE_SKIP_DOCKER_FAKE_K8S=1."
else
  if [[ "$RESET_DOCKER" == "1" ]]; then
    run_step docker_compose_reset docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" down -v --remove-orphans
  else
    skip_step docker_compose_reset "Skipped because RUNE_RESET_DOCKER_FAKE_K8S=0."
  fi

  rm -f "$ROOT_DIR/docker-compose/generated/orbit-seeded.ok" \
    "$ROOT_DIR/docker-compose/generated/lattice-seeded.ok" \
    "$ROOT_DIR/docker-compose/generated/orbit-host.yaml" \
    "$ROOT_DIR/docker-compose/generated/lattice-host.yaml" \
    "$ROOT_DIR/docker-compose/generated/rune-fake-kubeconfig.yaml"

  run_step docker_compose_up docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" up -d
  run_step docker_compose_wait_seeded bash -lc \
    'for i in {1..180}; do [[ -f docker-compose/generated/orbit-seeded.ok && -f docker-compose/generated/lattice-seeded.ok ]] && exit 0; sleep 2; done; exit 1'
  run_step docker_compose_merge_kubeconfig bash docker-compose/merge-kubeconfig.sh
  run_step docker_compose_safe_kubeconfig bash -lc 'grep -q "server: https://127.0.0.1:16443" docker-compose/generated/rune-fake-kubeconfig.yaml && grep -q "server: https://127.0.0.1:17443" docker-compose/generated/rune-fake-kubeconfig.yaml'

  if safe_docker_kubeconfig_check; then
    run_step docker_compose_integration_test env \
      RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 \
      swift test --filter LocalKubernetesIntegrationTests/testDockerComposeFakeK8sResourceGraphAndEventsAreLocalAndResolvable
    run_step docker_compose_read_write_integration_test env \
      RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 \
      swift test --filter LocalKubernetesIntegrationTests/testDockerComposeFakeK8sReadWriteOperationsAreReversible
  else
    FAILURES=$((FAILURES + 1))
    skip_step docker_compose_integration_test "Skipped because merged kubeconfig did not pass local-only safety check."
    skip_step docker_compose_read_write_integration_test "Skipped because merged kubeconfig did not pass local-only safety check."
  fi
fi

RESULT="passed"
if [[ $FAILURES -ne 0 ]]; then
  RESULT="failed"
fi
ENDED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

cat >> "$REPORT_MD" <<EOF

## Result

\`$RESULT\`

## Rerun

\`\`\`bash
scripts/run-local-k8s-integration-report.sh
\`\`\`

Use \`RUNE_SKIP_DOCKER_FAKE_K8S=1\` to run only the script fake-k8s part.
Use \`RUNE_RESET_DOCKER_FAKE_K8S=0\` to reuse an existing Docker Compose stack.

## Machine Report

\`$REPORT_JSON\`
EOF

write_report_json "$RESULT" "$STARTED_AT" "$ENDED_AT"

printf 'Human report: %s\n' "$REPORT_MD"
printf 'JSON report: %s\n' "$REPORT_JSON"

if [[ "$RESULT" != "passed" ]]; then
  exit 1
fi
