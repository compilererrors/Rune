# Docker Compose Fake Kubernetes

This folder contains a two-cluster local `k3s` sandbox for Rune UI debugging.

The stack starts:
- `fake-orbit-mesh` on `https://127.0.0.1:16443`
- `fake-lattice-spark` on `https://127.0.0.1:17443`

Each cluster is seeded with synthetic namespaces, deployments, StatefulSets, DaemonSets, Jobs, CronJobs, Services, Ingresses, ConfigMaps, Secrets, RBAC objects, NetworkPolicies, PVC-backed workloads, HPAs, Helm release ConfigMaps, and common operator resources.

The `fake-orbit-mesh` cluster also includes local-only diagnostics workloads for release verification:

- `alpha-log-matrix` covers init-container YAML/describe output plus explicit multi-container log selection.
- `alpha-previous-log-probe` covers `previous=true` pod log reads after a container restart.
- Flux, ArgoCD, and cert-manager custom resources cover operator resource discovery, YAML, and describe paths.

## Start

```bash
docker compose -p rune-fake-k8s -f docker-compose/docker-compose.fake-k8s.yml up -d
```

## Check readiness

```bash
docker compose -p rune-fake-k8s -f docker-compose/docker-compose.fake-k8s.yml ps
docker compose -p rune-fake-k8s -f docker-compose/docker-compose.fake-k8s.yml logs orbit-seed lattice-seed
```

The seed step is complete when these files exist:

- `docker-compose/generated/orbit-seeded.ok`
- `docker-compose/generated/lattice-seeded.ok`

## Host kubeconfig files

The stack writes one kubeconfig per cluster:

- `docker-compose/generated/orbit-host.yaml`
- `docker-compose/generated/lattice-host.yaml`

You can point `kubectl` at either file directly:

```bash
kubectl --kubeconfig docker-compose/generated/orbit-host.yaml get pods -A
kubectl --kubeconfig docker-compose/generated/lattice-host.yaml get pods -A
```

Or merge both kubeconfigs into one file for Rune:

```bash
bash docker-compose/merge-kubeconfig.sh
```

That produces:

- `docker-compose/generated/rune-fake-kubeconfig.yaml`

## Integration Test Report

Run the local-only integration suite and produce both a human-readable Markdown report and a machine-readable JSON report:

```bash
scripts/run-local-k8s-integration-report.sh
```

The default report run exercises the script and REST fake-cluster suites without starting Docker Compose. This keeps the normal run lightweight and avoids kubelet/cgroup noise. Reports are written under:

- `test-reports/local-k8s-integration/<run-id>/report.md`
- `test-reports/local-k8s-integration/<run-id>/report.json`

To reset the local `rune-fake-k8s` project, start both k3s clusters, merge the localhost kubeconfig, and run the guarded Docker integration tests:

```bash
RUNE_SKIP_DOCKER_FAKE_K8S=0 RUNE_RESET_DOCKER_FAKE_K8S=1 scripts/run-local-k8s-integration-report.sh
```

The Docker suite includes read checks plus reversible writes for manifest apply/update/delete, CronJob suspend/create Job, deployment scale, rollout restart, pod delete, port-forward start/stop, exec, and logs. To reuse an already running local stack, set `RUNE_RESET_DOCKER_FAKE_K8S=0` while keeping `RUNE_SKIP_DOCKER_FAKE_K8S=0`.

## Local Cluster Test Commands

Start or refresh the local fake clusters:

```bash
scripts/rune-compose-fake-k8s.sh reset
```

Run only the Docker Compose local-cluster ViewModel workflow tests:

```bash
RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 swift test --filter RuneDockerComposeViewModelIntegrationTests
```

Run only the multi-container, previous-log, and operator-resource coverage:

```bash
RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 swift test --filter RuneDockerComposeViewModelIntegrationTests/testDockerComposeLogsCoverMultiContainerPreviousLogsAndOperatorResources
```

Run the local integration report, including Docker Compose setup:

```bash
RUNE_RESET_DOCKER_FAKE_K8S=1 RUNE_SKIP_DOCKER_FAKE_K8S=0 scripts/run-local-k8s-integration-report.sh
```

## Stop and clean up

```bash
docker compose -p rune-fake-k8s -f docker-compose/docker-compose.fake-k8s.yml down -v
```
