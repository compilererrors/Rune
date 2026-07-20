#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose/docker-compose.fake-k8s.yml"
COMPOSE_PROJECT="${RUNE_ADD_CLUSTER_E2E_COMPOSE_PROJECT:-rune-fake-k8s}"
KUBECONFIG_FIXTURE="$ROOT_DIR/docker-compose/generated/rune-fake-kubeconfig.yaml"
APP_BUNDLE="$ROOT_DIR/dist/Rune.app"
APP_BIN="$APP_BUNDLE/Contents/MacOS/RuneApp"
SKIP_BUILD="${RUNE_ADD_CLUSTER_E2E_SKIP_BUILD:-0}"

if [[ -n "${RUNE_ADD_CLUSTER_E2E_STATE_DIR:-}" ]]; then
  TEST_STATE_DIR="$RUNE_ADD_CLUSTER_E2E_STATE_DIR"
  REMOVE_STATE_DIR=0
else
  TEST_STATE_DIR="$(mktemp -d /tmp/rune-add-cluster-e2e.XXXXXX)"
  REMOVE_STATE_DIR=1
fi
AX_HELPER="$TEST_STATE_DIR/rune-ui-smoke-ax"
LAUNCH_HELPER="$TEST_STATE_DIR/rune-app-launch"
MODULE_CACHE_DIR="$TEST_STATE_DIR/module-cache"
TEST_APP_BUNDLE="$TEST_STATE_DIR/RuneAddClusterE2E.app"
TEST_APP_BIN="$TEST_APP_BUNDLE/Contents/MacOS/RuneApp"
ACTIVATION_PROOF="$TEST_STATE_DIR/Library/Application Support/Rune/namespace-lists/fake-orbit-mesh.json"
APP_PID=""

cleanup() {
  local script_status=$?
  local cleanup_failed=0
  trap - EXIT

  if [[ -n "$APP_PID" ]]; then
    if [[ -x "$LAUNCH_HELPER" ]]; then
      if ! "$LAUNCH_HELPER" --terminate "$TEST_APP_BUNDLE" "$APP_PID"; then
        echo "Failed to terminate the isolated Rune E2E process: $APP_PID" >&2
        cleanup_failed=1
      fi
    elif kill -0 "$APP_PID" >/dev/null 2>&1; then
      echo "Refusing to signal Rune PID $APP_PID without the ownership-checking launch helper." >&2
      cleanup_failed=1
    fi
  fi

  if [[ "$cleanup_failed" == "0" \
      && "$REMOVE_STATE_DIR" == "1" \
      && "$TEST_STATE_DIR" == /tmp/rune-add-cluster-e2e.* ]]; then
    rm -rf "$TEST_STATE_DIR"
  elif [[ "$cleanup_failed" != "0" ]]; then
    echo "Preserving Add Cluster E2E state after cleanup failure: $TEST_STATE_DIR" >&2
  fi

  if [[ "$script_status" == "0" && "$cleanup_failed" != "0" ]]; then
    script_status=1
  fi
  exit "$script_status"
}
trap cleanup EXIT

for command_name in codesign ditto docker find kubectl plutil xcrun; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Missing required command: $command_name" >&2
    exit 1
  fi
done

mkdir -p "$TEST_STATE_DIR" "$MODULE_CACHE_DIR"

docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" up -d

ready=0
for _ in {1..180}; do
  if [[ -f "$ROOT_DIR/docker-compose/generated/orbit-seeded.ok" \
      && -f "$ROOT_DIR/docker-compose/generated/lattice-seeded.ok" ]]; then
    bash "$ROOT_DIR/docker-compose/merge-kubeconfig.sh" >/dev/null
    if KUBECONFIG="$KUBECONFIG_FIXTURE" kubectl --context fake-orbit-mesh --request-timeout=3s get --raw=/readyz >/dev/null 2>&1 \
      && KUBECONFIG="$KUBECONFIG_FIXTURE" kubectl --context fake-lattice-spark --request-timeout=3s get --raw=/readyz >/dev/null 2>&1; then
      ready=1
      break
    fi
  fi
  sleep 1
done
if [[ "$ready" != "1" ]]; then
  echo "Timed out waiting for the two local Docker Compose clusters." >&2
  exit 1
fi

required_markers=(
  "name: fake-orbit-mesh"
  "name: fake-lattice-spark"
  "server: https://127.0.0.1:16443"
  "server: https://127.0.0.1:17443"
)
for marker in "${required_markers[@]}"; do
  if ! grep -q "$marker" "$KUBECONFIG_FIXTURE"; then
    echo "Refusing Add Cluster E2E: local-only kubeconfig marker is missing: $marker" >&2
    exit 1
  fi
done
contexts="$(KUBECONFIG="$KUBECONFIG_FIXTURE" kubectl config get-contexts -o name | sort | tr '\n' ' ')"
if [[ "$contexts" != "fake-lattice-spark fake-orbit-mesh " ]]; then
  echo "Refusing Add Cluster E2E: expected only the two local fake contexts." >&2
  exit 1
fi

if [[ "$SKIP_BUILD" != "1" || ! -x "$APP_BIN" ]]; then
  "$ROOT_DIR/scripts/build-macos-app.sh"
fi
if [[ ! -x "$APP_BIN" ]]; then
  echo "Rune app binary is missing: $APP_BIN" >&2
  exit 1
fi

# LaunchServices identifies applications by bundle ID. A running developer copy
# must never steal activation or AX ownership from this isolated process test.
rm -rf "$TEST_APP_BUNDLE"
ditto "$APP_BUNDLE" "$TEST_APP_BUNDLE"
TEST_BUNDLE_IDENTIFIER="app.rune.local.e2e.add-cluster.$$"
plutil -replace CFBundleIdentifier -string "$TEST_BUNDLE_IDENTIFIER" \
  "$TEST_APP_BUNDLE/Contents/Info.plist"
codesign --force --deep --sign - --timestamp=none "$TEST_APP_BUNDLE" >/dev/null
codesign --verify --deep --strict "$TEST_APP_BUNDLE"
if [[ ! -x "$TEST_APP_BIN" ]]; then
  echo "Isolated Rune E2E binary is missing: $TEST_APP_BIN" >&2
  exit 1
fi

SWIFT_MODULE_CACHE_PATH="$MODULE_CACHE_DIR" \
CLANG_MODULE_CACHE_PATH="$MODULE_CACHE_DIR" \
  xcrun swiftc "$ROOT_DIR/scripts/rune-ui-smoke-ax.swift" -o "$AX_HELPER"
SWIFT_MODULE_CACHE_PATH="$MODULE_CACHE_DIR" \
CLANG_MODULE_CACHE_PATH="$MODULE_CACHE_DIR" \
  xcrun swiftc -parse-as-library "$ROOT_DIR/scripts/rune-app-launch.swift" -o "$LAUNCH_HELPER"

rm -f "$ACTIVATION_PROOF"
APP_PID="$(
  RUNE_APP_LAUNCH_ENABLE_DEMO_CLUSTER=1 \
    "$LAUNCH_HELPER" "$TEST_APP_BUNDLE" "$TEST_STATE_DIR"
)"
if [[ ! "$APP_PID" =~ ^[0-9]+$ ]] || ! kill -0 "$APP_PID" >/dev/null 2>&1; then
  echo "LaunchServices did not return a live isolated Rune PID." >&2
  exit 1
fi

IMPORT_ROOT="$TEST_STATE_DIR/Library/Application Support/Rune/kubeconfigs/imports"
expected_import_result="import-kubeconfig-e2e passed route=add-cluster-popover contexts=2 source=app-owned"
import_result="$(
  RUNE_UI_SMOKE_IMPORT_KUBECONFIG="$KUBECONFIG_FIXTURE" \
  RUNE_UI_SMOKE_IMPORT_ROOT="$IMPORT_ROOT" \
  RUNE_UI_SMOKE_ACTIVATION_PROOF="$ACTIVATION_PROOF" \
    "$AX_HELPER" "$APP_PID" import-kubeconfig
)"
printf '%s\n' "$import_result"
if [[ "$import_result" != "$expected_import_result" ]]; then
  echo "Add Cluster E2E used an unexpected import route. Expected: $expected_import_result; got: $import_result" >&2
  exit 1
fi

imported_count="$(find "$IMPORT_ROOT" -type f -name '*.yaml' 2>/dev/null | wc -l | tr -d ' ')"
if [[ "$imported_count" != "1" ]]; then
  echo "Add Cluster UI completed, but expected exactly one app-owned kubeconfig; found $imported_count." >&2
  exit 1
fi
IMPORTED_KUBECONFIG="$(find "$IMPORT_ROOT" -type f -name '*.yaml' -print -quit)"
for marker in "${required_markers[@]}"; do
  if ! grep -q "$marker" "$IMPORTED_KUBECONFIG"; then
    echo "App-owned kubeconfig is missing the local-only marker: $marker" >&2
    exit 1
  fi
done

imported_contexts="$(
  kubectl --kubeconfig "$IMPORTED_KUBECONFIG" config get-contexts -o name \
    | LC_ALL=C sort \
    | tr '\n' ' '
)"
if [[ "$imported_contexts" != "fake-lattice-spark fake-orbit-mesh " ]]; then
  echo "App-owned kubeconfig does not contain exactly the two expected local contexts." >&2
  exit 1
fi

expected_contexts=("fake-orbit-mesh" "fake-lattice-spark")
expected_namespaces=("alpha-zone" "delta-zone")
for index in "${!expected_contexts[@]}"; do
  expected_context="${expected_contexts[$index]}"
  expected_namespace="${expected_namespaces[$index]}"

  if ! kubectl --kubeconfig "$IMPORTED_KUBECONFIG" \
      --context "$expected_context" \
      --request-timeout=5s \
      get --raw=/readyz >/dev/null; then
    echo "App-owned kubeconfig could not reach /readyz for $expected_context." >&2
    exit 1
  fi

  resource_name="$(
    kubectl --kubeconfig "$IMPORTED_KUBECONFIG" \
      --context "$expected_context" \
      --request-timeout=5s \
      get namespace "$expected_namespace" -o name
  )"
  if [[ "$resource_name" != "namespace/$expected_namespace" ]]; then
    echo "App-owned kubeconfig did not return the expected namespace for $expected_context." >&2
    exit 1
  fi
done

activation_proof_ready=0
for _ in {1..350}; do
  if [[ -f "$ACTIVATION_PROOF" ]] \
      && grep -Fq '"alpha-zone"' "$ACTIVATION_PROOF" \
      && grep -Fq '"bravo-zone"' "$ACTIVATION_PROOF"; then
    activation_proof_ready=1
    break
  fi
  sleep 0.1
done
if [[ "$activation_proof_ready" != "1" ]]; then
  echo "Imported kubeconfig was saved, but Rune never completed a live namespace read for the activated context." >&2
  exit 1
fi

echo "add-cluster-import-process-e2e passed contexts=2 source=app-owned app-context-loaded=1 readyz=2 resource-reads=2"
