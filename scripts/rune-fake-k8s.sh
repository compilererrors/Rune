#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${RUNE_FAKE_K8S_STATE:-$ROOT_DIR/.rune-fake-k8s}"

build_fake_k8s() {
  if [[ -n "${RUNE_FAKE_K8S_BINARY:-}" ]]; then
    if [[ ! -x "$RUNE_FAKE_K8S_BINARY" ]]; then
      echo "RUNE_FAKE_K8S_BINARY is not executable: $RUNE_FAKE_K8S_BINARY" >&2
      exit 1
    fi
    printf '%s\n' "$RUNE_FAKE_K8S_BINARY"
    return
  fi

  swift build --product RuneFakeK8s >/dev/null
  local bin_dir
  bin_dir="$(swift build --show-bin-path)"
  printf '%s\n' "$bin_dir/RuneFakeK8s"
}

setup_fake_k8s() {
  local binary_path="$1"
  "$binary_path" setup --state-dir "$STATE_DIR" --binary "$binary_path" >/dev/null
  mkdir -p "$STATE_DIR/home"
}

export_env() {
  cat <<EOF
export KUBECONFIG="$STATE_DIR/kubeconfig.yaml"
export RUNE_K8S_AGENT=""
export RUNE_FAKE_K8S_STATE="$STATE_DIR"
export RUNE_FAKE_K8S_BINARY="$BINARY_PATH"
export HOME="$STATE_DIR/home"
export RUNE_DISABLE_DEFAULT_KUBECONFIG_DISCOVERY="1"
EOF
}

SUBCOMMAND="${1:-app}"
if [[ $# -gt 0 ]]; then
  shift
fi

BINARY_PATH="$(build_fake_k8s)"
setup_fake_k8s "$BINARY_PATH"

case "$SUBCOMMAND" in
  setup)
    "$BINARY_PATH" summary
    ;;
  env)
    export_env
    ;;
  summary)
    "$BINARY_PATH" summary
    ;;
  kubectl)
    exec "$BINARY_PATH" kubectl --state-dir "$STATE_DIR" "$@"
    ;;
  app)
    export KUBECONFIG="$STATE_DIR/kubeconfig.yaml"
    export RUNE_K8S_AGENT=""
    export RUNE_FAKE_K8S_STATE="$STATE_DIR"
    export RUNE_FAKE_K8S_BINARY="$BINARY_PATH"
    export HOME="$STATE_DIR/home"
    export RUNE_DISABLE_DEFAULT_KUBECONFIG_DISCOVERY="1"
    exec swift run RuneApp "$@"
    ;;
  *)
    cat <<EOF
usage:
  scripts/rune-fake-k8s.sh setup
  scripts/rune-fake-k8s.sh summary
  scripts/rune-fake-k8s.sh env
  scripts/rune-fake-k8s.sh app
EOF
    exit 1
    ;;
esac
