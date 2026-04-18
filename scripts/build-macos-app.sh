#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
APP_NAME="Rune"
PRODUCT_NAME="RuneApp"
CONFIGURATION="${CONFIGURATION:-release}"
DIST_DIR="${ROOT_DIR}/dist"
APP_BUNDLE="${DIST_DIR}/${APP_NAME}.app"
ICON_SOURCE="${ICON_SOURCE:-${ROOT_DIR}/assets/rune_wheel.icns}"
ICON_NAME="AppIcon.icns"

cd "${ROOT_DIR}"

# Rune använder inte längre rune-k8s-agent i appen. Bygg den bara om du vill (t.ex. experiment): RUNE_BUILD_K8S_AGENT=1
if [[ "${RUNE_BUILD_K8S_AGENT:-}" == "1" ]] && command -v go >/dev/null 2>&1; then
  echo "Bygger rune-k8s-agent (Go + client-go, valfritt)…"
  (cd "${ROOT_DIR}/go/rune-k8s-agent" && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o rune-k8s-agent ./cmd/rune-k8s-agent)
elif [[ "${RUNE_BUILD_K8S_AGENT:-}" == "1" ]]; then
  echo "Varning: RUNE_BUILD_K8S_AGENT=1 men go saknas i PATH." >&2
fi

swift build -c "${CONFIGURATION}" --product "${PRODUCT_NAME}"

BIN_DIR="$(swift build -c "${CONFIGURATION}" --show-bin-path)"
BIN_PATH="${BIN_DIR}/${PRODUCT_NAME}"

if [[ ! -x "${BIN_PATH}" ]]; then
  echo "Kunde inte hitta binären på: ${BIN_PATH}" >&2
  exit 1
fi

rm -rf "${APP_BUNDLE}"
mkdir -p "${APP_BUNDLE}/Contents/MacOS"
mkdir -p "${APP_BUNDLE}/Contents/Resources"

cp "${BIN_PATH}" "${APP_BUNDLE}/Contents/MacOS/${PRODUCT_NAME}"

if [[ "${RUNE_BUILD_K8S_AGENT:-}" == "1" ]] && [[ -x "${ROOT_DIR}/go/rune-k8s-agent/rune-k8s-agent" ]]; then
  cp "${ROOT_DIR}/go/rune-k8s-agent/rune-k8s-agent" "${APP_BUNDLE}/Contents/MacOS/rune-k8s-agent"
fi

if [[ -f "${ICON_SOURCE}" ]]; then
  cp "${ICON_SOURCE}" "${APP_BUNDLE}/Contents/Resources/${ICON_NAME}"
else
  echo "Varning: hittade ingen app-ikon på ${ICON_SOURCE}, fortsätter utan ikon." >&2
fi

cat > "${APP_BUNDLE}/Contents/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>CFBundleName</key>
	<string>Rune</string>
	<key>CFBundleDisplayName</key>
	<string>Rune</string>
	<key>CFBundleExecutable</key>
	<string>RuneApp</string>
	<key>CFBundleIdentifier</key>
	<string>com.rune.desktop</string>
	<key>CFBundleIconFile</key>
	<string>AppIcon.icns</string>
	<key>CFBundlePackageType</key>
	<string>APPL</string>
	<key>CFBundleShortVersionString</key>
	<string>0.1.0</string>
	<key>CFBundleVersion</key>
	<string>1</string>
	<key>LSMinimumSystemVersion</key>
	<string>14.0</string>
	<key>NSHighResolutionCapable</key>
	<true/>
</dict>
</plist>
PLIST

codesign --force --deep --sign - "${APP_BUNDLE}" >/dev/null 2>&1 || true

echo "Byggt: ${APP_BUNDLE}"
echo "Starta med: open \"${APP_BUNDLE}\""
