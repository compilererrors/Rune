#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [--bundle-identifier VALUE] [--team-identifier VALUE] [--entitlements-output PATH] /path/to/App.app" >&2
}

fail_gate() {
  echo "App Store artifact gate failed: $*" >&2
  return 1
}

require_plist_value() {
  local plist_path="$1"
  local key="$2"
  local expected_type="$3"
  local expected_value="$4"
  local escaped_key="${key//./\\.}"
  local actual_value

  if ! actual_value="$(/usr/bin/plutil -extract "${escaped_key}" raw -expect "${expected_type}" -o - "${plist_path}" 2>/dev/null)"; then
    fail_gate "${plist_path} must contain ${key} as ${expected_type}."
    return 1
  fi
  if [[ "${actual_value}" != "${expected_value}" ]]; then
    fail_gate "${plist_path} has ${key}=${actual_value}; expected ${expected_value}."
    return 1
  fi
}

app_bundle=""
expected_bundle_identifier=""
expected_team_identifier=""
entitlements_output=""

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --bundle-identifier)
      [[ "$#" -ge 2 ]] || { usage; exit 64; }
      expected_bundle_identifier="$2"
      shift 2
      ;;
    --team-identifier)
      [[ "$#" -ge 2 ]] || { usage; exit 64; }
      expected_team_identifier="$2"
      shift 2
      ;;
    --entitlements-output)
      [[ "$#" -ge 2 ]] || { usage; exit 64; }
      entitlements_output="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --*)
      usage
      exit 64
      ;;
    *)
      if [[ -n "${app_bundle}" ]]; then
        usage
        exit 64
      fi
      app_bundle="$1"
      shift
      ;;
  esac
done

if [[ -z "${app_bundle}" || ( -n "${expected_team_identifier}" && -z "${expected_bundle_identifier}" ) ]]; then
  usage
  exit 64
fi

if [[ -z "${entitlements_output}" ]]; then
  entitlements_output="$(mktemp "${TMPDIR:-/tmp}/rune-signed-entitlements.XXXXXX")"
  trap 'rm -f "${entitlements_output}"' EXIT
fi

info_plist="${app_bundle}/Contents/Info.plist"
if [[ ! -f "${info_plist}" ]]; then
  fail_gate "missing ${info_plist}."
  exit 1
fi
if ! /usr/bin/codesign --verify --deep --strict "${app_bundle}" >/dev/null 2>&1; then
  fail_gate "${app_bundle} does not have a valid strict code signature."
  exit 1
fi
if ! /usr/bin/codesign -d --entitlements :- "${app_bundle}" >"${entitlements_output}" 2>/dev/null; then
  fail_gate "could not read signed entitlements from ${app_bundle}."
  exit 1
fi
if ! /usr/bin/plutil -lint "${entitlements_output}" >/dev/null; then
  fail_gate "signed entitlements are not a valid property list."
  exit 1
fi

require_plist_value "${info_plist}" "RuneDistribution" string "app-store"
require_plist_value "${info_plist}" "RuneExternalCommandsEnabled" bool "false"
require_plist_value "${entitlements_output}" "com.apple.security.app-sandbox" bool "true"
require_plist_value "${entitlements_output}" "com.apple.security.network.client" bool "true"
require_plist_value "${entitlements_output}" "com.apple.security.files.user-selected.read-write" bool "true"
require_plist_value "${entitlements_output}" "com.apple.security.files.bookmarks.app-scope" bool "true"

if [[ -n "${expected_bundle_identifier}" ]]; then
  require_plist_value "${info_plist}" "CFBundleIdentifier" string "${expected_bundle_identifier}"
fi
if [[ -n "${expected_team_identifier}" ]]; then
  require_plist_value "${entitlements_output}" "com.apple.application-identifier" string "${expected_team_identifier}.${expected_bundle_identifier}"
  require_plist_value "${entitlements_output}" "com.apple.developer.team-identifier" string "${expected_team_identifier}"
fi

echo "Verified App Store artifact: ${app_bundle}"
