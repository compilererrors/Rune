#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose/docker-compose.fake-k8s.yml"
COMPOSE_PROJECT="${RUNE_COMPOSE_FAKE_K8S_PROJECT:-rune-fake-k8s}"
MERGED_KUBECONFIG="$ROOT_DIR/docker-compose/generated/rune-fake-kubeconfig.yaml"
APP_STATE_DIR="${RUNE_COMPOSE_FAKE_K8S_APP_STATE:-$ROOT_DIR/.rune-compose-fake-k8s/home}"

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

safe_kubeconfig_check() {
  [[ -f "$MERGED_KUBECONFIG" ]] || return 1
  grep -q 'name: fake-orbit-mesh' "$MERGED_KUBECONFIG" || return 1
  grep -q 'name: fake-lattice-spark' "$MERGED_KUBECONFIG" || return 1
  grep -q 'server: https://127.0.0.1:16443' "$MERGED_KUBECONFIG" || return 1
  grep -q 'server: https://127.0.0.1:17443' "$MERGED_KUBECONFIG" || return 1
}

assert_only_local_fake_contexts() {
  local contexts
  contexts="$(KUBECONFIG="$MERGED_KUBECONFIG" kubectl config get-contexts -o name | sort | tr '\n' ' ')"
  if [[ "$contexts" != "fake-lattice-spark fake-orbit-mesh " ]]; then
    echo "Refusing to launch Rune: expected only fake local contexts, got: $contexts" >&2
    exit 1
  fi
}

start_stack() {
  docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" up -d
  wait_for_seed_files
  bash "$ROOT_DIR/docker-compose/merge-kubeconfig.sh"
  if ! safe_kubeconfig_check; then
    echo "Refusing to launch Rune: merged kubeconfig is not the expected localhost fake-k8s config." >&2
    exit 1
  fi
  assert_only_local_fake_contexts
}

run_app() {
  mkdir -p "$APP_STATE_DIR"
  export HOME="$APP_STATE_DIR"
  export CFFIXED_USER_HOME="$APP_STATE_DIR"
  export RUNE_ISOLATED_KUBECONFIG="$MERGED_KUBECONFIG"
  export KUBECONFIG="$MERGED_KUBECONFIG"
  export RUNE_DISABLE_DEFAULT_KUBECONFIG_DISCOVERY="1"
  export RUNE_DISABLE_BOOKMARKED_KUBECONFIGS="1"
  export RUNE_K8S_AGENT=""
  swift run RuneApp "$@"
}

SUBCOMMAND="${1:-app}"
if [[ $# -gt 0 ]]; then
  shift
fi

cd "$ROOT_DIR"

case "$SUBCOMMAND" in
  app)
    start_stack
    run_app "$@"
    ;;
  start)
    start_stack
    ;;
  stop)
    docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" down
    ;;
  reset)
    docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" down -v --remove-orphans
    rm -f "$ROOT_DIR/docker-compose/generated/orbit-seeded.ok" \
      "$ROOT_DIR/docker-compose/generated/lattice-seeded.ok" \
      "$ROOT_DIR/docker-compose/generated/orbit-host.yaml" \
      "$ROOT_DIR/docker-compose/generated/lattice-host.yaml" \
      "$MERGED_KUBECONFIG"
    start_stack
    ;;
  env)
    start_stack
    cat <<EOF
export HOME="$APP_STATE_DIR"
export CFFIXED_USER_HOME="$APP_STATE_DIR"
export RUNE_ISOLATED_KUBECONFIG="$MERGED_KUBECONFIG"
export KUBECONFIG="$MERGED_KUBECONFIG"
export RUNE_DISABLE_DEFAULT_KUBECONFIG_DISCOVERY="1"
export RUNE_DISABLE_BOOKMARKED_KUBECONFIGS="1"
export RUNE_K8S_AGENT=""
EOF
    ;;
  *)
    cat <<EOF
usage:
  scripts/rune-compose-fake-k8s.sh app
  scripts/rune-compose-fake-k8s.sh start
  scripts/rune-compose-fake-k8s.sh stop
  scripts/rune-compose-fake-k8s.sh reset
  scripts/rune-compose-fake-k8s.sh env
EOF
    exit 1
    ;;
esac
