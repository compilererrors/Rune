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
BUNDLE_IDENTIFIER="${BUNDLE_IDENTIFIER:-com.rune.local}"
MARKETING_VERSION="${MARKETING_VERSION:-0.1.0}"
BUNDLE_VERSION="${BUNDLE_VERSION:-1}"

cd "${ROOT_DIR}"

swift build -c "${CONFIGURATION}" --product "${PRODUCT_NAME}"

BIN_DIR="$(swift build -c "${CONFIGURATION}" --show-bin-path)"
BIN_PATH="${BIN_DIR}/${PRODUCT_NAME}"

if [[ ! -x "${BIN_PATH}" ]]; then
  echo "Could not find binary at: ${BIN_PATH}" >&2
  exit 1
fi

rm -rf "${APP_BUNDLE}"
mkdir -p "${APP_BUNDLE}/Contents/MacOS"
mkdir -p "${APP_BUNDLE}/Contents/Resources"

cp "${BIN_PATH}" "${APP_BUNDLE}/Contents/MacOS/${PRODUCT_NAME}"

if [[ -f "${ICON_SOURCE}" ]]; then
  cp "${ICON_SOURCE}" "${APP_BUNDLE}/Contents/Resources/${ICON_NAME}"
else
  echo "Warning: no app icon found at ${ICON_SOURCE}; continuing without an icon." >&2
fi

cat > "${APP_BUNDLE}/Contents/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0">
<dict>
	<key>CFBundleName</key>
	<string>Rune</string>
	<key>CFBundleDisplayName</key>
	<string>Rune</string>
	<key>CFBundleExecutable</key>
	<string>RuneApp</string>
	<key>CFBundleIdentifier</key>
	<string>__BUNDLE_IDENTIFIER__</string>
	<key>CFBundleIconFile</key>
	<string>AppIcon.icns</string>
	<key>CFBundlePackageType</key>
	<string>APPL</string>
	<key>CFBundleShortVersionString</key>
	<string>__MARKETING_VERSION__</string>
	<key>CFBundleVersion</key>
	<string>__BUNDLE_VERSION__</string>
	<key>LSMinimumSystemVersion</key>
	<string>14.0</string>
	<key>LSApplicationCategoryType</key>
	<string>public.app-category.developer-tools</string>
	<key>ITSAppUsesNonExemptEncryption</key>
	<false/>
	<key>NSHighResolutionCapable</key>
	<true/>
	<key>NSAppTransportSecurity</key>
	<dict>
		<!-- Kubernetes API servers are user-configured dynamic hosts, often with private CAs.
		     Rune still validates TLS explicitly from kubeconfig CA/client settings in code. -->
		<key>NSAllowsArbitraryLoads</key>
		<true/>
	</dict>
</dict>
</plist>
PLIST

perl -0pi \
  -e "s/__BUNDLE_IDENTIFIER__/${BUNDLE_IDENTIFIER}/g; s/__MARKETING_VERSION__/${MARKETING_VERSION}/g; s/__BUNDLE_VERSION__/${BUNDLE_VERSION}/g" \
  "${APP_BUNDLE}/Contents/Info.plist"

echo "Byggt: ${APP_BUNDLE}"
echo "Start with: open \"${APP_BUNDLE}\""
echo "Debug (stdout+stderr via tee): RUNE_VERBOSE_DEBUG_TRACE=1 RUNE_DIAGNOSTICS_LOGGING=1 RUNE_LOG_TO_STDERR=1 \"${APP_BUNDLE}/Contents/MacOS/${PRODUCT_NAME}\" 2>&1 | tee ~/Desktop/rune-k8s-debug.log"
