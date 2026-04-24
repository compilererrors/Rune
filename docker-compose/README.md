# Docker Compose Fake Kubernetes

This folder contains a two-cluster local `k3s` sandbox for Rune UI debugging.

The stack starts:
- `fake-orbit-mesh` on `https://127.0.0.1:16443`
- `fake-lattice-spark` on `https://127.0.0.1:17443`

Each cluster is seeded with synthetic namespaces, deployments, StatefulSets, DaemonSets, Jobs, CronJobs, Services, Ingresses, ConfigMaps, Secrets, RBAC objects, NetworkPolicies, PVC-backed workloads, and HPAs.

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

The report runner resets only the local `rune-fake-k8s` Docker Compose project by default, starts both fake clusters, merges the generated localhost kubeconfig, and runs the guarded integration tests. The Docker Compose suite includes read checks plus reversible writes for manifest apply/update/delete, CronJob suspend/create Job, deployment scale, rollout restart, pod delete, port-forward start/stop, exec, and logs. Reports are written under:

- `test-reports/local-k8s-integration/<run-id>/report.md`
- `test-reports/local-k8s-integration/<run-id>/report.json`

Useful switches:

```bash
RUNE_SKIP_DOCKER_FAKE_K8S=1 scripts/run-local-k8s-integration-report.sh
RUNE_RESET_DOCKER_FAKE_K8S=0 scripts/run-local-k8s-integration-report.sh
```

## Stop and clean up

```bash
docker compose -p rune-fake-k8s -f docker-compose/docker-compose.fake-k8s.yml down -v
```
