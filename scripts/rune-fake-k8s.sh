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

setup_rest_fake_k8s() {
  local binary_path="$1"
  local server_url="$2"
  "$binary_path" setup-rest --state-dir "$STATE_DIR" --binary "$binary_path" --server-url "$server_url" >/dev/null
  mkdir -p "$STATE_DIR/home"
}

start_rest_fake_k8s() {
  mkdir -p "$STATE_DIR"
  local log_file="$STATE_DIR/rest-server.log"
  : > "$log_file"
  "$BINARY_PATH" serve --host 127.0.0.1 --port 0 > "$log_file" 2>&1 &
  REST_FAKE_PID="$!"
  trap 'kill "$REST_FAKE_PID" >/dev/null 2>&1 || true' EXIT

  local server_url=""
  for _ in {1..100}; do
    if ! kill -0 "$REST_FAKE_PID" >/dev/null 2>&1; then
      echo "RuneFakeK8s REST server exited early. Log:" >&2
      cat "$log_file" >&2
      exit 1
    fi
    server_url="$(sed -n 's/.*\(http:\/\/127\.0\.0\.1:[0-9][0-9]*\).*/\1/p' "$log_file" | tail -n 1)"
    if [[ -n "$server_url" ]]; then
      REST_SERVER_URL="$server_url"
      return
    fi
    sleep 0.05
  done

  echo "Timed out waiting for RuneFakeK8s REST server. Log:" >&2
  cat "$log_file" >&2
  exit 1
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

case "$SUBCOMMAND" in
  setup)
    setup_fake_k8s "$BINARY_PATH"
    "$BINARY_PATH" summary
    ;;
  env)
    setup_fake_k8s "$BINARY_PATH"
    export_env
    ;;
  summary)
    setup_fake_k8s "$BINARY_PATH"
    "$BINARY_PATH" summary
    ;;
  kubectl)
    setup_fake_k8s "$BINARY_PATH"
    exec "$BINARY_PATH" kubectl --state-dir "$STATE_DIR" "$@"
    ;;
  app)
    start_rest_fake_k8s
    setup_rest_fake_k8s "$BINARY_PATH" "$REST_SERVER_URL"
    export KUBECONFIG="$STATE_DIR/kubeconfig.yaml"
    export RUNE_K8S_AGENT=""
    export RUNE_FAKE_K8S_STATE="$STATE_DIR"
    export RUNE_FAKE_K8S_BINARY="$BINARY_PATH"
    export RUNE_FAKE_K8S_REST_SERVER="$REST_SERVER_URL"
    export HOME="$STATE_DIR/home"
    export RUNE_DISABLE_DEFAULT_KUBECONFIG_DISCOVERY="1"
    swift run RuneApp "$@"
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
