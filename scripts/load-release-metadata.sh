#!/usr/bin/env bash

# Source this file from App Store build and packaging entrypoints.
function rune_load_release_metadata() {
  local signing_env
  signing_env="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.local/signing.env"
  if [[ ! -f "${signing_env}" ]]; then
    echo "Missing release configuration: ${signing_env}" >&2
    return 1
  fi

  # Missing file values must never fall back to the caller's environment.
  unset BUNDLE_IDENTIFIER MARKETING_VERSION BUNDLE_VERSION
  source "${signing_env}" || return 1
  if [[ -z "${BUNDLE_IDENTIFIER:-}" || -z "${MARKETING_VERSION:-}" || -z "${BUNDLE_VERSION:-}" ]]; then
    echo "Set BUNDLE_IDENTIFIER, MARKETING_VERSION and BUNDLE_VERSION in ${signing_env}." >&2
    return 1
  fi
  if [[ ! "${BUNDLE_IDENTIFIER}" =~ ^[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)+$ ]]; then
    echo "Invalid BUNDLE_IDENTIFIER in ${signing_env}." >&2
    return 1
  fi
  if [[ ! "${MARKETING_VERSION}" =~ ^[0-9]+(\.[0-9]+){0,2}$ || ! "${BUNDLE_VERSION}" =~ ^[0-9]+(\.[0-9]+){0,2}$ ]]; then
    echo "MARKETING_VERSION and BUNDLE_VERSION in ${signing_env} must contain one to three numeric components." >&2
    return 1
  fi
  export BUNDLE_IDENTIFIER MARKETING_VERSION BUNDLE_VERSION
}

function rune_verify_release_metadata() {
  local info_plist="${1:?Pass the app bundle to verify}/Contents/Info.plist"
  local metadata_key plist_key actual
  for metadata_key in BUNDLE_IDENTIFIER MARKETING_VERSION BUNDLE_VERSION; do
    case "${metadata_key}" in
      BUNDLE_IDENTIFIER) plist_key="CFBundleIdentifier" ;;
      MARKETING_VERSION) plist_key="CFBundleShortVersionString" ;;
      BUNDLE_VERSION) plist_key="CFBundleVersion" ;;
    esac
    actual="$(/usr/libexec/PlistBuddy -c "Print :${plist_key}" "${info_plist}")" || return 1
    if [[ "${actual}" != "${!metadata_key}" ]]; then
      echo "${plist_key} does not match .local/signing.env. Rebuild the app before packaging or uploading." >&2
      return 1
    fi
  done
}
