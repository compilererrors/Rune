#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose/docker-compose.fake-k8s.yml"
COMPOSE_PROJECT="${RUNE_UI_SMOKE_COMPOSE_PROJECT:-rune-fake-k8s}"
MERGED_KUBECONFIG="$ROOT_DIR/docker-compose/generated/rune-fake-kubeconfig.yaml"
OUT_ROOT="${RUNE_UI_SCREENSHOT_DIR:-$ROOT_DIR/assets/screenshot/ui}"
RUN_ID="${RUNE_UI_SCREENSHOT_RUN_ID:-$(date -u +"%Y%m%dT%H%M%SZ")}"
RUN_DIR="$OUT_ROOT/$RUN_ID"
APP_STATE_DIR="${RUNE_UI_SMOKE_APP_STATE_DIR:-/tmp/rune-ui-smoke-state/$RUN_ID}"
APP_BUNDLE="$ROOT_DIR/dist/Rune.app"
APP_BIN="$APP_BUNDLE/Contents/MacOS/RuneApp"
APP_PROCESS="${RUNE_UI_SMOKE_APP_PROCESS:-RuneApp}"
APP_LOG="$APP_STATE_DIR/app.log"
SWIFT_SCRIPT_CACHE_DIR="${RUNE_UI_SMOKE_SWIFT_CACHE_DIR:-$APP_STATE_DIR/swift-module-cache}"
STEP_TIMEOUT_SECONDS="${RUNE_UI_SMOKE_STEP_TIMEOUT_SECONDS:-35}"
SHORTCUT_DWELL_SECONDS="${RUNE_UI_SMOKE_SHORTCUT_DWELL_SECONDS:-2}"
SCENARIO_DWELL_MS="${RUNE_UI_SMOKE_SCENARIO_DWELL_MS:-1800}"
SCENARIO_SNAPSHOT_HOLD_MS="${RUNE_UI_SMOKE_SCENARIO_SNAPSHOT_HOLD_MS:-1800}"
SMOKE_DETAIL_WIDTH="${RUNE_UI_SMOKE_DETAIL_WIDTH:-900}"
RESET_DOCKER="${RUNE_UI_SMOKE_RESET_DOCKER:-0}"
SKIP_BUILD="${RUNE_UI_SMOKE_SKIP_BUILD:-0}"
ALLOW_TIMEOUT_FALLBACK="${RUNE_UI_SMOKE_ALLOW_TIMEOUT_FALLBACK:-0}"
APP_PID=""

require_command() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "Missing required command: $name" >&2
    exit 1
  fi
}

launch_rune_app() {
  mkdir -p "$APP_STATE_DIR"
  : > "$APP_LOG"
  local existing_pids
  existing_pids="$(pgrep -f "$APP_BIN" || true)"

  open -n -F \
    --stdout "$APP_LOG" \
    --stderr "$APP_LOG" \
    --env "HOME=$APP_STATE_DIR" \
    --env "CFFIXED_USER_HOME=$APP_STATE_DIR" \
    --env "RUNE_ISOLATED_KUBECONFIG=$MERGED_KUBECONFIG" \
    --env "KUBECONFIG=$MERGED_KUBECONFIG" \
    --env "RUNE_DISABLE_DEFAULT_KUBECONFIG_DISCOVERY=1" \
    --env "RUNE_DISABLE_BOOKMARKED_KUBECONFIGS=1" \
    --env "RUNE_K8S_AGENT=" \
    --env "RUNE_DIAGNOSTICS_LOGGING=1" \
    --env "RUNE_LOG_TO_STDERR=1" \
    --env "RUNE_DEBUG_LAYOUT=1" \
    --env "RUNE_DEBUG_LAYOUT_LIVE_SCENARIO=1" \
    --env "RUNE_DEBUG_LAYOUT_CONTEXT=fake-orbit-mesh" \
    --env "RUNE_DEBUG_LAYOUT_NAMESPACE=alpha-zone" \
    --env "RUNE_DEBUG_LAYOUT_DETAIL_WIDTH=$SMOKE_DETAIL_WIDTH" \
    --env "RUNE_DEBUG_LAYOUT_POD_DWELL_MS=$SCENARIO_DWELL_MS" \
    --env "RUNE_DEBUG_LAYOUT_SNAPSHOT_HOLD_MS=$SCENARIO_SNAPSHOT_HOLD_MS" \
    "$APP_BUNDLE"

  for _ in {1..80}; do
    local candidate_pids
    candidate_pids="$(pgrep -f "$APP_BIN" || true)"
    while IFS= read -r candidate_pid; do
      if [[ -z "$candidate_pid" ]]; then
        continue
      fi
      if grep -qx "$candidate_pid" <<< "$existing_pids"; then
        continue
      fi
      if kill -0 "$candidate_pid" >/dev/null 2>&1; then
        APP_PID="$candidate_pid"
        echo "Launched Rune pid: $APP_PID"
        return 0
      fi
    done <<< "$candidate_pids"
    sleep 0.25
  done

  echo "Timed out waiting for launched Rune process for $APP_BIN" >&2
  exit 1
}

cleanup() {
  if [[ -n "$APP_PID" ]] && kill -0 "$APP_PID" >/dev/null 2>&1; then
    kill "$APP_PID" >/dev/null 2>&1 || true
    wait "$APP_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

run() {
  echo "+ $*"
  "$@"
}

safe_docker_kubeconfig_check() {
  [[ -f "$MERGED_KUBECONFIG" ]] || return 1
  grep -q 'name: fake-orbit-mesh' "$MERGED_KUBECONFIG" || return 1
  grep -q 'name: fake-lattice-spark' "$MERGED_KUBECONFIG" || return 1
  grep -q 'server: https://127.0.0.1:16443' "$MERGED_KUBECONFIG" || return 1
  grep -q 'server: https://127.0.0.1:17443' "$MERGED_KUBECONFIG" || return 1
}

assert_only_local_fake_kubectl_contexts() {
  local contexts
  contexts="$(KUBECONFIG="$MERGED_KUBECONFIG" kubectl config get-contexts -o name | sort | tr '\n' ' ')"
  if [[ "$contexts" != "fake-lattice-spark fake-orbit-mesh " ]]; then
    echo "Refusing UI smoke: expected only fake local contexts, got: $contexts" >&2
    exit 1
  fi
}

wait_for_seed_files() {
  local orbit="$ROOT_DIR/docker-compose/generated/orbit-seeded.ok"
  local lattice="$ROOT_DIR/docker-compose/generated/lattice-seeded.ok"
  for _ in {1..180}; do
    if [[ -f "$orbit" && -f "$lattice" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "Timed out waiting for Docker Compose fake-k8s seed files." >&2
  exit 1
}

wait_for_app_window() {
  if [[ -z "$APP_PID" ]]; then
    echo "Internal error: APP_PID is empty before waiting for Rune window." >&2
    exit 1
  fi
  mkdir -p "$SWIFT_SCRIPT_CACHE_DIR"

  for _ in {1..80}; do
    osascript >/dev/null 2>&1 <<APPLESCRIPT || true
tell application "System Events"
  set runeProcesses to processes whose unix id is $APP_PID
  if (count of runeProcesses) > 0 then
    tell item 1 of runeProcesses to set frontmost to true
  end if
end tell
APPLESCRIPT
    if SWIFT_MODULE_CACHE_PATH="$SWIFT_SCRIPT_CACHE_DIR" CLANG_MODULE_CACHE_PATH="$SWIFT_SCRIPT_CACHE_DIR" swift "$ROOT_DIR/scripts/rune-window-id.swift" "$APP_PROCESS" "$APP_PID" >/dev/null 2>&1
    then
      return 0
    fi
    sleep 0.25
  done

  echo "Timed out waiting for Rune window. If macOS blocks this, grant Accessibility permission to your terminal." >&2
  exit 1
}

focus_and_size_window() {
  osascript >/dev/null 2>&1 <<APPLESCRIPT || true
tell application "System Events"
  set runeProcesses to processes whose unix id is $APP_PID
  if (count of runeProcesses) is 0 then return
  tell item 1 of runeProcesses
    set frontmost to true
    if (count of windows) > 0 then
      try
        set bounds of window 1 to {64, 64, 1984, 1220}
      end try
    end if
  end tell
end tell
APPLESCRIPT
}

capture() {
  local name="$1"
  local path="$RUN_DIR/$name.png"
  local window_id
  mkdir -p "$SWIFT_SCRIPT_CACHE_DIR"
  window_id="$(SWIFT_MODULE_CACHE_PATH="$SWIFT_SCRIPT_CACHE_DIR" CLANG_MODULE_CACHE_PATH="$SWIFT_SCRIPT_CACHE_DIR" swift "$ROOT_DIR/scripts/rune-window-id.swift" "$APP_PROCESS" "$APP_PID" 2>/dev/null || true)"
  if [[ -z "$window_id" || ! "$window_id" =~ ^[0-9]+$ ]]; then
    echo "Refusing screenshot: could not resolve Rune window id for pid $APP_PID ($APP_PROCESS)." >&2
    exit 1
  fi
  screencapture -l"$window_id" -o -x "$path"
  if [[ ! -s "$path" ]]; then
    echo "Refusing screenshot: screencapture did not create a non-empty Rune window image at $path." >&2
    exit 1
  fi
  echo "$name.png" >> "$RUN_DIR/screenshots.txt"
  echo "captured $path"
}

wait_for_log_pattern() {
  local pattern="$1"
  local timeout="$2"
  local deadline=$((SECONDS + timeout))
  while (( SECONDS < deadline )); do
    if grep -q "$pattern" "$APP_LOG"; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

capture_live_scenario_step() {
  local index="$1"
  local step="$2"
  local label="$3"

  echo "waiting live scenario $(printf "%02d" "$index"): $step"
  if wait_for_log_pattern "step=$step status=snapshot" "$STEP_TIMEOUT_SECONDS"; then
    focus_and_size_window
    sleep 0.3
    capture "$(printf "%02d" "$index")-$label"
  else
    if [[ "$ALLOW_TIMEOUT_FALLBACK" != "1" ]]; then
      echo "Refusing screenshot: no live scenario snapshot for $step within ${STEP_TIMEOUT_SECONDS}s. See $APP_LOG" >&2
      exit 1
    fi
    echo "warning: no live scenario snapshot for $step; taking best-effort screenshot" >&2
    focus_and_size_window
    capture "$(printf "%02d" "$index")-$label-timeout"
  fi
}

press_section_shortcut() {
  local number="$1"
  osascript <<APPLESCRIPT
tell application "System Events"
  set runeProcesses to processes whose unix id is $APP_PID
  if (count of runeProcesses) is 0 then error "Rune process pid $APP_PID not found"
  tell item 1 of runeProcesses
    set frontmost to true
    keystroke "$number" using command down
  end tell
end tell
APPLESCRIPT
}

open_palette_with_query() {
  local query="$1"
  osascript <<APPLESCRIPT
tell application "System Events"
  set runeProcesses to processes whose unix id is $APP_PID
  if (count of runeProcesses) is 0 then error "Rune process pid $APP_PID not found"
  tell item 1 of runeProcesses
    set frontmost to true
    keystroke "k" using command down
    delay 0.25
    keystroke "$query"
  end tell
end tell
APPLESCRIPT
}

close_sheet_or_palette() {
  osascript <<APPLESCRIPT
tell application "System Events"
  set runeProcesses to processes whose unix id is $APP_PID
  if (count of runeProcesses) is 0 then error "Rune process pid $APP_PID not found"
  tell item 1 of runeProcesses
    set frontmost to true
    key code 53
  end tell
end tell
APPLESCRIPT
}

write_manifest_header() {
  cat > "$RUN_DIR/manifest.md" <<EOF
# Rune UI Smoke Screenshots

- Run id: \`$RUN_ID\`
- Generated: \`$(date -u +"%Y-%m-%dT%H:%M:%SZ")\`
- Kubeconfig: \`docker-compose/generated/rune-fake-kubeconfig.yaml\`
- Output: \`assets/screenshot/ui/$RUN_ID\`
- Isolated app state: \`$APP_STATE_DIR\`
- App log: \`$APP_LOG\`
- Capture mode: \`Rune window only via screencapture -l\`
- Safety: \`fake local kubeconfig only; timeout fallback=$ALLOW_TIMEOUT_FALLBACK\`

## Screenshots

EOF
}

write_manifest_footer() {
  local file
  while IFS= read -r file; do
    printf -- '- [%s](%s)\n' "$file" "$file" >> "$RUN_DIR/manifest.md"
  done < "$RUN_DIR/screenshots.txt"
}

prepare_run_dir() {
  mkdir -p "$RUN_DIR"
  if [[ -f "$RUN_DIR/screenshots.txt" ]] || find "$RUN_DIR" -maxdepth 1 -type f -name '*.png' -print -quit | grep -q .; then
    echo "Refusing UI smoke: output run directory already contains screenshots: $RUN_DIR" >&2
    echo "Use a fresh RUNE_UI_SCREENSHOT_RUN_ID or move the existing directory." >&2
    exit 1
  fi
  : > "$RUN_DIR/screenshots.txt"
}

validate_captured_files() {
  local file
  local missing=0
  while IFS= read -r file; do
    if [[ ! -s "$RUN_DIR/$file" ]]; then
      echo "Missing captured screenshot listed in manifest: $RUN_DIR/$file" >&2
      missing=1
    fi
  done < "$RUN_DIR/screenshots.txt"
  if (( missing )); then
    exit 1
  fi
}

main() {
  require_command docker
  require_command osascript
  require_command screencapture
  require_command swift
  require_command kubectl
  require_command open
  require_command pgrep

  prepare_run_dir
  write_manifest_header

  cd "$ROOT_DIR"

  if [[ "$RESET_DOCKER" == "1" ]]; then
    run docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" down -v --remove-orphans
    rm -f "$ROOT_DIR/docker-compose/generated/orbit-seeded.ok" \
      "$ROOT_DIR/docker-compose/generated/lattice-seeded.ok" \
      "$ROOT_DIR/docker-compose/generated/orbit-host.yaml" \
      "$ROOT_DIR/docker-compose/generated/lattice-host.yaml" \
      "$MERGED_KUBECONFIG"
  fi

  run docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" up -d
  wait_for_seed_files
  run bash "$ROOT_DIR/docker-compose/merge-kubeconfig.sh"

  if ! safe_docker_kubeconfig_check; then
    echo "Refusing to launch UI smoke: merged kubeconfig is not the expected localhost fake-k8s config." >&2
    exit 1
  fi
  assert_only_local_fake_kubectl_contexts

  if [[ "$SKIP_BUILD" != "1" || ! -x "$APP_BIN" ]]; then
    run "$ROOT_DIR/scripts/build-macos-app.sh"
  fi

  if [[ ! -x "$APP_BIN" ]]; then
    echo "Rune app binary not found: $APP_BIN" >&2
    exit 1
  fi

  echo "Launching Rune for UI smoke screenshots..."
  launch_rune_app

  wait_for_app_window
  focus_and_size_window
  if ! wait_for_log_pattern "bootstrap resolved sources count=1" 20; then
    echo "Refusing UI smoke: Rune did not report exactly one kubeconfig source. See $APP_LOG" >&2
    exit 1
  fi
  if ! wait_for_log_pattern "reloadContexts contexts=2" 20; then
    echo "Refusing UI smoke: Rune did not report exactly two fake contexts. See $APP_LOG" >&2
    exit 1
  fi
  if grep -E 'aks-|prod|arn:|gke_|minikube|docker-desktop|rancher-desktop' "$APP_LOG" >/dev/null 2>&1; then
    echo "Refusing UI smoke: app log contains non-fake context/server markers. See $APP_LOG" >&2
    exit 1
  fi
  sleep 1

  local scenario_steps=(
    "overview:overview"
    "workloadPodOverview:workloads-pod-overview"
    "workloadPodLogs:workloads-pod-logs"
    "workloadPodExec:workloads-pod-exec"
    "workloadPodPortForward:workloads-pod-port-forward"
    "workloadPodYAMLReadOnly:workloads-pod-yaml-readonly"
    "workloadPodYAMLQuickEdit:workloads-pod-yaml-quick-edit"
    "workloadPodYAMLEditorSheet:workloads-pod-yaml-editor-sheet"
    "workloadPodDescribe:workloads-pod-describe"
    "workloadDeploymentOverview:workloads-deployment-overview"
    "workloadDeploymentUnifiedLogs:workloads-deployment-unified-logs"
    "workloadDeploymentRollout:workloads-deployment-rollout"
    "workloadDeploymentYAMLReadOnly:workloads-deployment-yaml-readonly"
    "workloadDeploymentYAMLQuickEdit:workloads-deployment-yaml-quick-edit"
    "workloadDeploymentDescribe:workloads-deployment-describe"
    "networkingServiceOverview:networking-service-overview"
    "networkingServiceUnifiedLogs:networking-service-unified-logs"
    "networkingServicePortForward:networking-service-port-forward"
    "networkingServiceYAMLReadOnly:networking-service-yaml-readonly"
    "networkingServiceYAMLQuickEdit:networking-service-yaml-quick-edit"
    "networkingServiceDescribe:networking-service-describe"
    "configConfigMapPrepare:config-configmap-overview"
    "configConfigMapYAMLReadOnly:config-configmap-yaml-readonly"
    "configConfigMapYAMLQuickEdit:config-configmap-yaml-quick-edit"
    "configConfigMapDescribe:config-configmap-describe"
    "storagePVCDescribe:storage-pvc-describe"
    "storagePVCYAML:storage-pvc-yaml"
    "eventsDetail:events-detail"
    "rbacRole:rbac-role-describe"
    "terminal:terminal"
  )
  local scenario_index=1
  local scenario_entry
  for scenario_entry in "${scenario_steps[@]}"; do
    capture_live_scenario_step "$scenario_index" "${scenario_entry%%:*}" "${scenario_entry#*:}"
    scenario_index=$((scenario_index + 1))
  done

  wait_for_log_pattern "step=overview status=finished" 10 || true

  local section_names=(
    overview
    workloads
    networking
    storage
    config
    rbac
    events
    helm
    terminal
  )
  local number=1
  for section_name in "${section_names[@]}"; do
    echo "capturing section $(printf "%02d" "$((scenario_index + number - 1))"): $section_name"
    press_section_shortcut "$number"
    sleep "$SHORTCUT_DWELL_SECONDS"
    focus_and_size_window
    capture "$(printf "%02d" "$((scenario_index + number - 1))")-section-$section_name"
    number=$((number + 1))
  done

  local palette_index=$((scenario_index + ${#section_names[@]}))
  local palette_queries=(
    "help:?"
    "pods::po"
    "deployments::deploy"
    "services::svc"
    "configmaps::cm"
    "rbac::rbac"
    "reload::reload"
  )
  local palette_entry
  for palette_entry in "${palette_queries[@]}"; do
    echo "capturing command palette $(printf "%02d" "$palette_index"): ${palette_entry%%:*}"
    open_palette_with_query "${palette_entry#*:}"
    sleep 1
    capture "$(printf "%02d" "$palette_index")-command-palette-${palette_entry%%:*}"
    close_sheet_or_palette
    sleep 0.4
    palette_index=$((palette_index + 1))
  done

  validate_captured_files
  write_manifest_footer

  echo "Screenshots: $RUN_DIR"
  echo "Manifest: $RUN_DIR/manifest.md"
  echo "App log: $APP_LOG"
}

main "$@"
