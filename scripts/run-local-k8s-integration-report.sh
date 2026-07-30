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
MODULE_CACHE_DIR="${RUNE_LOCAL_K8S_MODULE_CACHE_DIR:-$ROOT_DIR/.build/local-k8s-integration-cache}"

COMPOSE_FILE="$ROOT_DIR/docker-compose/docker-compose.fake-k8s.yml"
COMPOSE_PROJECT="rune-fake-k8s"
MERGED_KUBECONFIG="$ROOT_DIR/docker-compose/generated/rune-fake-kubeconfig.yaml"
SCRIPT_STATE_DIR="${RUNE_FAKE_K8S_INTEGRATION_STATE:-/tmp/rune-fake-k8s-integration}"
RESET_DOCKER="${RUNE_RESET_DOCKER_FAKE_K8S:-1}"
SKIP_DOCKER="${RUNE_SKIP_DOCKER_FAKE_K8S:-1}"
K3S_IMAGE="${RUNE_K3S_IMAGE:-rancher/k3s:v1.36.2-k3s1}"
K3S_IMAGE_TAG="${K3S_IMAGE##*:}"
DERIVED_KUBERNETES_VERSION="${K3S_IMAGE_TAG%%-k3s*}"
EXPECTED_KUBERNETES_VERSION="${RUNE_EXPECTED_KUBERNETES_VERSION:-$DERIVED_KUBERNETES_VERSION}"
ORBIT_SERVER_VERSION=""
LATTICE_SERVER_VERSION=""

FAILURES=0

if [[ ! "$EXPECTED_KUBERNETES_VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "Could not derive a Kubernetes version from RUNE_K3S_IMAGE=$K3S_IMAGE." >&2
  echo "Set RUNE_EXPECTED_KUBERNETES_VERSION to an exact value such as v1.36.2." >&2
  exit 64
fi

mkdir -p "$LOG_DIR" "$MODULE_CACHE_DIR/clang" "$MODULE_CACHE_DIR/swiftpm"
: > "$STEPS_JSONL"

export CLANG_MODULE_CACHE_PATH="${CLANG_MODULE_CACHE_PATH:-$MODULE_CACHE_DIR/clang}"
export SWIFTPM_MODULECACHE_OVERRIDE="${SWIFTPM_MODULECACHE_OVERRIDE:-$MODULE_CACHE_DIR/swiftpm}"
export RUNE_K3S_IMAGE="$K3S_IMAGE"

# This report is intentionally fake-cluster only. Clear live-test opt-ins so a
# developer shell with RUNE_LIVE_* exported cannot make the report touch real clusters.
unset RUNE_LIVE_K8S_CONTEXT
unset RUNE_LIVE_K8S_CONTEXTS
unset RUNE_LIVE_KUBECONFIG
unset RUNE_LIVE_CLOUD_PROVIDER
unset RUNE_LIVE_AKS_CLUSTER
unset RUNE_LIVE_AKS_RESOURCE_GROUP
unset RUNE_LIVE_EKS_CLUSTER
unset RUNE_LIVE_EKS_REGION
unset RUNE_LIVE_EKS_ROLE_ARN
unset RUNE_LIVE_GKE_CLUSTER
unset RUNE_LIVE_GKE_LOCATION
unset RUNE_LIVE_GKE_PROJECT
unset RUNE_LIVE_CLOUD_PROFILE_OR_SUBSCRIPTION
export RUNE_ALLOW_LIVE_K8S_TESTS=0
export RUNE_ALLOW_LIVE_CLOUD_TESTS=0

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

  {
    printf '\n### `%s`\n\n' "$name"
    printf -- '- Status: `%s`\n' "$status"
    printf -- '- Exit: `%s`\n' "$exit_text"
    printf -- '- Duration: `%ss`\n' "$duration_text"
    if [[ -n "$rel_log" ]]; then
      printf -- '- Log: [`%s`](%s)\n' "$rel_log" "$rel_log"
    else
      printf -- '- Log: `n/a`\n'
    fi
    if [[ -n "$note" ]]; then
      printf -- '- Note: %s\n' "$note"
    fi
  } >> "$REPORT_MD"
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
  if [[ $exit_code -eq 0 ]] && grep -q 'No matching test cases were run' "$log_file"; then
    exit_code=66
  fi
  if [[ $exit_code -eq 0 && "$name" == docker_compose_*_test ]] \
      && grep -Eq "Test Case .* skipped|with [1-9][0-9]* tests? skipped" "$log_file"; then
    exit_code=67
  fi
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
  return "$exit_code"
}

skip_step() {
  local name="$1"
  local note="$2"
  append_step_json "$name" "skipped" "" "" "" "" "$note"
  append_step_md "$name" "skipped" "" "" "" "$note"
}

can_bind_loopback_socket() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); s.close()' >/dev/null 2>&1
}

write_report_header() {
  cat > "$REPORT_MD" <<EOF
# Local Kubernetes Integration Test Report

- Run id: \`$RUN_ID\`
- Generated: \`$(date -u +"%Y-%m-%dT%H:%M:%SZ")\`
- Repository: \`$ROOT_DIR\`
- Script fake-k8s state: \`$SCRIPT_STATE_DIR\`
- Docker Compose kubeconfig: \`$MERGED_KUBECONFIG\`
- Docker Compose K3s image: \`$K3S_IMAGE\`
- Expected Kubernetes server version: \`$EXPECTED_KUBERNETES_VERSION\`
- Safety gate: tests require \`RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1\` and hard-fail unless kubeconfigs are fake/local only.

## Steps
EOF
}

write_report_json() {
  local result="$1"
  local started_at="$2"
  local ended_at="$3"
  {
    printf '{\n'
    printf '  "schemaVersion": 2,\n'
    printf '  "runId": %s,\n' "$(json_string "$RUN_ID")"
    printf '  "generatedAt": %s,\n' "$(json_string "$(date -u +"%Y-%m-%dT%H:%M:%SZ")")"
    printf '  "result": %s,\n' "$(json_string "$result")"
    printf '  "startedAt": %s,\n' "$(json_string "$started_at")"
    printf '  "endedAt": %s,\n' "$(json_string "$ended_at")"
    printf '  "repoRoot": %s,\n' "$(json_string "$ROOT_DIR")"
    printf '  "dockerCompose": {\n'
    printf '    "image": %s,\n' "$(json_string "$K3S_IMAGE")"
    printf '    "expectedKubernetesVersion": %s,\n' "$(json_string "$EXPECTED_KUBERNETES_VERSION")"
    printf '    "observedServerVersions": {\n'
    printf '      "fake-orbit-mesh": %s,\n' "$(json_string "$ORBIT_SERVER_VERSION")"
    printf '      "fake-lattice-spark": %s\n' "$(json_string "$LATTICE_SERVER_VERSION")"
    printf '    }\n'
    printf '  },\n'
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

read_server_git_version() {
  local context="$1"
  KUBECONFIG="$MERGED_KUBECONFIG" \
    kubectl --context "$context" --request-timeout=10s version -o json \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["serverVersion"]["gitVersion"])'
}

assert_docker_server_versions() {
  ORBIT_SERVER_VERSION="$(read_server_git_version fake-orbit-mesh)" || return 1
  LATTICE_SERVER_VERSION="$(read_server_git_version fake-lattice-spark)" || return 1

  printf 'fake-orbit-mesh: expected %s, observed %s\n' \
    "$EXPECTED_KUBERNETES_VERSION" "$ORBIT_SERVER_VERSION"
  printf 'fake-lattice-spark: expected %s, observed %s\n' \
    "$EXPECTED_KUBERNETES_VERSION" "$LATTICE_SERVER_VERSION"

  [[ "${ORBIT_SERVER_VERSION%%+*}" == "$EXPECTED_KUBERNETES_VERSION" ]] || return 1
  [[ "${LATTICE_SERVER_VERSION%%+*}" == "$EXPECTED_KUBERNETES_VERSION" ]] || return 1
}

wait_for_docker_fixture_readiness() {
  KUBECONFIG="$MERGED_KUBECONFIG" \
    kubectl --context fake-orbit-mesh -n kube-system \
    rollout status deployment/local-path-provisioner --timeout=180s || return 1
  KUBECONFIG="$MERGED_KUBECONFIG" \
    kubectl --context fake-lattice-spark -n kube-system \
    rollout status deployment/local-path-provisioner --timeout=180s || return 1
  KUBECONFIG="$MERGED_KUBECONFIG" \
    kubectl --context fake-orbit-mesh -n alpha-zone \
    wait --for=condition=Ready pod/alpha-log-matrix --timeout=180s || return 1
  KUBECONFIG="$MERGED_KUBECONFIG" \
    kubectl --context fake-orbit-mesh -n alpha-zone \
    rollout status statefulset/orbit-vault --timeout=180s || return 1
  KUBECONFIG="$MERGED_KUBECONFIG" \
    kubectl --context fake-lattice-spark -n delta-zone \
    rollout status statefulset/delta-vault-core --timeout=180s || return 1

  local previous_logs=""
  for _ in {1..90}; do
    previous_logs="$(
      KUBECONFIG="$MERGED_KUBECONFIG" \
        kubectl --context fake-orbit-mesh -n alpha-zone \
        logs pod/alpha-previous-log-probe -c crasher --previous 2>/dev/null
    )"
    if [[ "$previous_logs" == *"alpha-previous-log-probe previous marker"* ]]; then
      printf 'Previous-log probe is readable.\n'
      return 0
    fi
    sleep 1
  done

  printf 'Previous-log probe did not become readable. Last output: %s\n' "$previous_logs" >&2
  return 1
}

STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
write_report_header

cd "$ROOT_DIR" || exit 1

run_step build_RuneFakeK8s swift build --disable-sandbox --product RuneFakeK8s

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
  swift test --disable-sandbox --filter LocalKubernetesIntegrationTests/testRuneFakeK8sEventsPointAtExistingPods

if can_bind_loopback_socket; then
  run_step rest_fake_integration_test swift test --disable-sandbox --filter RuneFakeK8sRESTServerTests
  run_step fake_cluster_view_model_workflow_test swift test --disable-sandbox --filter RuneFakeClusterViewModelIntegrationTests
  run_step fake_cluster_yaml_workflow_test swift test --disable-sandbox --filter RuneFakeClusterYAMLWorkflowIntegrationTests
else
  skip_step rest_fake_integration_test "Skipped because this environment cannot bind local loopback sockets."
  skip_step fake_cluster_view_model_workflow_test "Skipped because this environment cannot bind local loopback sockets."
  skip_step fake_cluster_yaml_workflow_test "Skipped because this environment cannot bind local loopback sockets."
fi

if [[ "$SKIP_DOCKER" == "1" ]]; then
  skip_step docker_compose_stack "Skipped because RUNE_SKIP_DOCKER_FAKE_K8S defaults to 1."
  skip_step docker_compose_integration_test "Skipped because RUNE_SKIP_DOCKER_FAKE_K8S defaults to 1."
else
  DOCKER_READY=1
  if [[ "$RESET_DOCKER" == "1" ]]; then
    if ! run_step docker_compose_reset docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" down -v --remove-orphans; then
      DOCKER_READY=0
    fi
    rm -f "$ROOT_DIR/docker-compose/generated/orbit-seeded.ok" \
      "$ROOT_DIR/docker-compose/generated/lattice-seeded.ok" \
      "$ROOT_DIR/docker-compose/generated/orbit-internal.yaml" \
      "$ROOT_DIR/docker-compose/generated/lattice-internal.yaml" \
      "$ROOT_DIR/docker-compose/generated/orbit-host.yaml" \
      "$ROOT_DIR/docker-compose/generated/lattice-host.yaml" \
      "$ROOT_DIR/docker-compose/generated/rune-fake-kubeconfig.yaml"
  else
    skip_step docker_compose_reset "Skipped because RUNE_RESET_DOCKER_FAKE_K8S=0."
  fi

  if [[ "$DOCKER_READY" == "1" ]]; then
    if ! run_step docker_compose_up docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" up -d; then
      DOCKER_READY=0
    fi
  else
    skip_step docker_compose_up "Skipped because Docker reset failed."
  fi

  if [[ "$DOCKER_READY" == "1" ]]; then
    if ! run_step docker_compose_wait_seeded bash -lc \
      'for i in {1..180}; do [[ -f docker-compose/generated/orbit-seeded.ok && -f docker-compose/generated/lattice-seeded.ok ]] && exit 0; sleep 2; done; exit 1'; then
      DOCKER_READY=0
    fi
  else
    skip_step docker_compose_wait_seeded "Skipped because Docker Compose stack did not start."
  fi

  if [[ "$DOCKER_READY" == "1" ]]; then
    if ! run_step docker_compose_merge_kubeconfig bash docker-compose/merge-kubeconfig.sh; then
      DOCKER_READY=0
    fi
  else
    skip_step docker_compose_merge_kubeconfig "Skipped because Docker Compose stack was not ready."
  fi

  if [[ "$DOCKER_READY" == "1" ]]; then
    if ! run_step docker_compose_safe_kubeconfig bash -lc 'grep -q "server: https://127.0.0.1:16443" docker-compose/generated/rune-fake-kubeconfig.yaml && grep -q "server: https://127.0.0.1:17443" docker-compose/generated/rune-fake-kubeconfig.yaml'; then
      DOCKER_READY=0
    fi
  else
    skip_step docker_compose_safe_kubeconfig "Skipped because Docker Compose kubeconfig was not generated."
  fi

  if [[ "$DOCKER_READY" == "1" ]]; then
    if ! run_step docker_compose_server_version assert_docker_server_versions; then
      DOCKER_READY=0
    fi
  else
    skip_step docker_compose_server_version "Skipped because Docker Compose kubeconfig safety checks did not pass."
  fi

  if [[ "$DOCKER_READY" == "1" ]]; then
    if ! run_step docker_compose_wait_fixture_ready wait_for_docker_fixture_readiness; then
      DOCKER_READY=0
    fi
  else
    skip_step docker_compose_wait_fixture_ready "Skipped because Docker Compose server versions did not match."
  fi

  if [[ "$DOCKER_READY" == "1" ]] && safe_docker_kubeconfig_check; then
    run_step docker_compose_single_context_namespace_test env \
      RUNE_ALLOW_LIVE_K8S_TESTS=1 \
      RUNE_LIVE_KUBECONFIG="$MERGED_KUBECONFIG" \
      RUNE_LIVE_K8S_CONTEXT=fake-orbit-mesh \
      swift test --disable-sandbox --filter LocalKubernetesIntegrationTests/testLiveKubeconfigContextListsNamespacesWhenExplicitlyEnabled
    run_step docker_compose_multi_context_namespace_test env \
      RUNE_ALLOW_LIVE_K8S_TESTS=1 \
      RUNE_LIVE_KUBECONFIG="$MERGED_KUBECONFIG" \
      RUNE_LIVE_K8S_CONTEXTS=fake-orbit-mesh,fake-lattice-spark \
      swift test --disable-sandbox --filter LocalKubernetesIntegrationTests/testLiveKubeconfigContextsListNamespacesWhenExplicitlyEnabled
    run_step docker_compose_integration_test env \
      RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 \
      swift test --disable-sandbox --filter LocalKubernetesIntegrationTests/testDockerComposeFakeK8sResourceGraphAndEventsAreLocalAndResolvable
    run_step docker_compose_read_write_integration_test env \
      RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 \
      swift test --disable-sandbox --filter LocalKubernetesIntegrationTests/testDockerComposeFakeK8sReadWriteOperationsAreReversible
    run_step docker_compose_terminal_smoke_test env \
      RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 \
      swift test --disable-sandbox --filter RuneDockerComposeViewModelIntegrationTests/testDockerComposeTerminalRightPanelLogWorkflowDoesNotFollowShellPodFallback
    run_step docker_compose_add_cluster_import_integration_test env \
      RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 \
      swift test --disable-sandbox --filter RuneDockerComposeKubeConfigImportIntegrationTests/testAddClusterImportPublishesAndActivatesBothDockerComposeContexts
    run_step docker_compose_view_model_feature_integration_test env \
      RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 \
      swift test --disable-sandbox --filter RuneDockerComposeViewModelIntegrationTests
  else
    skip_step docker_compose_single_context_namespace_test "Skipped because Docker Compose stack or kubeconfig safety gate did not pass."
    skip_step docker_compose_multi_context_namespace_test "Skipped because Docker Compose stack or kubeconfig safety gate did not pass."
    skip_step docker_compose_integration_test "Skipped because Docker Compose stack or kubeconfig safety gate did not pass."
    skip_step docker_compose_read_write_integration_test "Skipped because Docker Compose stack or kubeconfig safety gate did not pass."
    skip_step docker_compose_terminal_smoke_test "Skipped because Docker Compose stack or kubeconfig safety gate did not pass."
    skip_step docker_compose_add_cluster_import_integration_test "Skipped because Docker Compose stack or kubeconfig safety gate did not pass."
    skip_step docker_compose_view_model_feature_integration_test "Skipped because Docker Compose stack or kubeconfig safety gate did not pass."
  fi
fi

RESULT="passed"
if [[ $FAILURES -ne 0 ]]; then
  RESULT="failed"
fi
ENDED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

if [[ "$SKIP_DOCKER" == "1" ]]; then
  RERUN_COMMAND="RUNE_SKIP_DOCKER_FAKE_K8S=1 scripts/run-local-k8s-integration-report.sh"
  RERUN_NOTE='This run skipped Docker Compose k3s. Set `RUNE_SKIP_DOCKER_FAKE_K8S=0` to include it.'
else
  printf -v K3S_IMAGE_SHELL '%q' "$K3S_IMAGE"
  if [[ "$RESET_DOCKER" == "1" ]]; then
    RERUN_RESET_DOCKER=1
  else
    RERUN_RESET_DOCKER=0
  fi
  RERUN_COMMAND="RUNE_K3S_IMAGE=$K3S_IMAGE_SHELL RUNE_SKIP_DOCKER_FAKE_K8S=0 RUNE_RESET_DOCKER_FAKE_K8S=$RERUN_RESET_DOCKER scripts/run-local-k8s-integration-report.sh"
  RERUN_NOTE='This run included Docker Compose k3s. Set `RUNE_RESET_DOCKER_FAKE_K8S=0` to reuse an existing local stack.'
fi

cat >> "$REPORT_MD" <<EOF

## Result

\`$RESULT\`

## Rerun

\`\`\`bash
$RERUN_COMMAND
\`\`\`

$RERUN_NOTE

## Machine Report

\`$REPORT_JSON\`
EOF

write_report_json "$RESULT" "$STARTED_AT" "$ENDED_AT"

printf 'Human report: %s\n' "$REPORT_MD"
printf 'JSON report: %s\n' "$REPORT_JSON"

if [[ "$RESULT" != "passed" ]]; then
  exit 1
fi
