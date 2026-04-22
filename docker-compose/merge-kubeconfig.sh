#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GENERATED_DIR="$ROOT_DIR/generated"
OUTPUT_FILE="$GENERATED_DIR/rune-fake-kubeconfig.yaml"
ORBIT_CONFIG="$GENERATED_DIR/orbit-host.yaml"
LATTICE_CONFIG="$GENERATED_DIR/lattice-host.yaml"

if [[ ! -f "$ORBIT_CONFIG" || ! -f "$LATTICE_CONFIG" ]]; then
  echo "Both host kubeconfig files must exist before merging." >&2
  exit 1
fi

KUBECONFIG="$ORBIT_CONFIG:$LATTICE_CONFIG" kubectl config view --flatten --raw > "$OUTPUT_FILE"
echo "Wrote merged kubeconfig to $OUTPUT_FILE"
