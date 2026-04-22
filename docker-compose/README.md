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

## Stop and clean up

```bash
docker compose -p rune-fake-k8s -f docker-compose/docker-compose.fake-k8s.yml down -v
```
