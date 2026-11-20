#!/usr/bin/env bash
# Fails when a release app bundle ships symbols, debug-only code, test data or
# local paths, or grows past the size budget.
set -euo pipefail

MAX_APP_SIZE_MB="${MAX_APP_SIZE_MB:-25}"
DEBUG_ONLY_MARKERS=(RuneUIComponentGallery RuneDebugFakePodShell RuneFakeK8s)
# Fake-cluster fixtures, the scripted debug scenario, screenshot capture and
# build-machine paths belong to tests only.
TEST_DATA_MARKERS=(fake-orbit-mesh fake-lattice-spark alpha-zone RUNE_DEBUG_LAYOUT liveScenario RuneAppStoreCapture RUNE_UI_APPSTORE /Users/ docker-compose/generated)

fail_gate() {
  echo "Lean release gate failed: $*" >&2
  exit 1
}

app_bundle="${1:-}"
[[ -n "${app_bundle}" ]] || { echo "Usage: $0 /path/to/App.app" >&2; exit 64; }

PRODUCT_NAME="$(/usr/bin/plutil -extract CFBundleExecutable raw -o - "${app_bundle}/Contents/Info.plist" 2>/dev/null || echo RuneApp)"
binary="${app_bundle}/Contents/MacOS/${PRODUCT_NAME}"
[[ -f "${binary}" ]] || fail_gate "missing ${binary}."

work="$(mktemp -d "${TMPDIR:-/tmp}/rune-lean-gate.XXXXXX")"
trap 'rm -rf "${work}"' EXIT

cp "${binary}" "${work}/binary"
xcrun strip "${work}/binary" 2>/dev/null || fail_gate "could not strip a copy of ${PRODUCT_NAME} to compare sizes."
shipped_bytes="$(stat -f %z "${binary}")"
stripped_bytes="$(stat -f %z "${work}/binary")"
# Re-stripping only drops the code signature, so allow a small slack.
if (( shipped_bytes - stripped_bytes > 1048576 )); then
  fail_gate "${PRODUCT_NAME} is not stripped (${shipped_bytes} bytes, ${stripped_bytes} after strip)."
fi

strings -a "${binary}" > "${work}/strings"
for marker in "${DEBUG_ONLY_MARKERS[@]}"; do
  if grep -q "${marker}" "${work}/strings"; then
    fail_gate "${PRODUCT_NAME} contains debug-only code (${marker})."
  fi
done

for marker in "${TEST_DATA_MARKERS[@]}"; do
  if grep -qF -- "${marker}" "${work}/strings"; then
    fail_gate "${PRODUCT_NAME} contains test data or a local path (${marker})."
  fi
done

stray="$(find "${app_bundle}" \( -iname '*fixture*' -o -iname '*.yaml' -o -iname '*.yml' -o -iname '*kubeconfig*' \
  -o -iname '*.xctest' -o -iname '*test*' -o -iname '*screenshot*' -o -name '.DS_Store' \) -print | head -5)"
if [[ -n "${stray}" ]]; then
  fail_gate "the bundle contains files that are not part of the app: ${stray//$'\n'/, }."
fi

app_kb="$(du -sk "${app_bundle}" | cut -f1)"
if (( app_kb > MAX_APP_SIZE_MB * 1024 )); then
  fail_gate "app is $(( app_kb / 1024 )) MB, budget is ${MAX_APP_SIZE_MB} MB."
fi

echo "Verified lean release: ${app_bundle} ($(( app_kb / 1024 )) MB)"
