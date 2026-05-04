# Rune

Rune is a fast native macOS Kubernetes cluster manager for people who debug real clusters every day.

It is built for fast Kubernetes troubleshooting: quick navigation, strong keyboard support, direct access to resources, and no heavy platform console in the way. Rune adds a native macOS interface with more room for logs, YAML, describe output, port-forwarding, metrics, events, and focused troubleshooting without turning into bloat.

![Rune overview](assets/screenshot/ui/update260503/overview-context-overview.png)

## Why Rune

Kubernetes debugging is often about keeping enough context in view: the pod, its controller, related service, current YAML, recent events, and the logs that actually contain the error. Rune keeps those workflows close together.

- Browse contexts, namespaces, workloads, networking, storage, config, RBAC, events, Helm releases, and a terminal view.
- Inspect full pod logs and unified logs across related workloads.
- Search through multi-pod logs without constantly changing log limits or jumping between panes.
- Edit YAML with syntax highlighting plus validation feedback for errors and warnings.
- Port-forward pods and services from the resource you are already inspecting.
- Open exec and terminal workflows in context.
- Check pod and node metrics when your cluster exposes metrics.
- Move quickly with a command palette and k9s-style resource jumps.
- Stay local: Rune does not use analytics, tracking, advertising, or telemetry.

## Support Rune

If you want to support Rune development, buy the app on the Mac App Store:

- [Buy RuneApp on the Mac App Store](https://apps.apple.com/us/app/runeapp/id6762515322?mt=12)

You can also support development through PayPal:

- [☕ Send 1 USD](https://paypal.me/viktornyberg1/1USD)
- [🍺 Send 2.99 USD](https://paypal.me/viktornyberg1/2.99USD)
- [🍻 Send 4.99 USD](https://paypal.me/viktornyberg1/4.99USD)

## Screenshots

### Core Workspace

<p>
  <img src="assets/screenshot/ui/update260503/overview-context-overview.png" alt="Rune overview" width="49%">
  <img src="assets/screenshot/ui/update260503/helm-release-overview.png" alt="Rune Helm release overview" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/events-pulled-go-to-pod.png" alt="Rune event linked to pod source" width="49%">
  <img src="assets/screenshot/ui/update260503/terminal-port-forward-pod-shell.png" alt="Rune terminal with pod shell and port-forwarding" width="49%">
</p>

### Workloads

<p>
  <img src="assets/screenshot/ui/update260503/workloads-pods-pod-overview.png" alt="Rune pod overview" width="49%">
  <img src="assets/screenshot/ui/update260503/workloads-pods-pod-logs.png" alt="Rune pod logs" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/workloads-pods-pod-describe.png" alt="Rune pod describe output" width="49%">
  <img src="assets/screenshot/ui/update260503/workloads-pods-pod-yaml.png" alt="Rune pod YAML" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/workloads-deployments-deployment-describe.png" alt="Rune deployment describe output" width="49%">
  <img src="assets/screenshot/ui/update260503/workloads-deployments-deployment-unified-logs.png" alt="Rune deployment unified logs" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/workloads-deployments-deployment-unified-logs-wide-panel.png" alt="Rune expanded deployment unified logs" width="49%">
  <img src="assets/screenshot/ui/update260503/workloads-statefulsets-statefulset-describe.png" alt="Rune StatefulSet describe output" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/workloads-daemonsets-daemonset-describe.png" alt="Rune DaemonSet describe output" width="49%">
  <img src="assets/screenshot/ui/update260503/workloads-daemonsets-daemonset-yaml.png" alt="Rune DaemonSet YAML" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/workloads-jobs-job-yaml.png" alt="Rune Job YAML" width="49%">
  <img src="assets/screenshot/ui/update260503/workloads-cronjobs-cronjob-yaml.png" alt="Rune CronJob YAML" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/workloads-hpas-hpa-describe.png" alt="Rune HPA describe output" width="49%">
  <img src="assets/screenshot/ui/update260503/workloads-hpas-hpa-yaml.png" alt="Rune HPA YAML" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/workloads-replicasets-replicaset-describe.png" alt="Rune ReplicaSet describe output" width="49%">
  <img src="assets/screenshot/ui/update260503/workloads-replicasets-replicaset-yaml-errors.png" alt="Rune ReplicaSet YAML validation" width="49%">
</p>

### YAML Editing And Validation

<p>
  <img src="assets/screenshot/ui/update260503/workloads-pods-pod-yaml-editor-modal.png" alt="Rune YAML editor" width="49%">
  <img src="assets/screenshot/ui/update260503/workloads-pods-pod-yaml-unsaved-errors.png" alt="Rune YAML validation in editor" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/workloads-deployments-deployment-yaml-errors.png" alt="Rune YAML validation error" width="49%">
  <img src="assets/screenshot/ui/update260503/workloads-deployments-deployment-yaml-errors-scrolled.png" alt="Rune YAML validation scrolled" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/workloads-pods-pod-yaml-from-event.png" alt="Rune pod YAML opened from an event" width="49%">
</p>

### Networking

<p>
  <img src="assets/screenshot/ui/update260503/networking-services-service-overview.png" alt="Rune service overview" width="49%">
  <img src="assets/screenshot/ui/update260503/networking-services-service-describe.png" alt="Rune service describe output" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/networking-services-service-yaml.png" alt="Rune service YAML" width="49%">
  <img src="assets/screenshot/ui/update260503/networking-services-service-unified-logs.png" alt="Rune service unified logs" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/networking-ingresses-ingress-yaml.png" alt="Rune ingress YAML" width="49%">
  <img src="assets/screenshot/ui/update260503/networking-networkpolicies-networkpolicy-yaml.png" alt="Rune network policy YAML" width="49%">
</p>

### Config, Storage, And RBAC

<p>
  <img src="assets/screenshot/ui/update260503/config-configmaps-configmap-yaml.png" alt="Rune ConfigMap YAML" width="49%">
  <img src="assets/screenshot/ui/update260503/storage-pvcs-pvc-yaml.png" alt="Rune PVC YAML" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/rbac-roles-role-yaml.png" alt="Rune RBAC role YAML" width="49%">
</p>

### Terminal And Port Forwarding

<p>
  <img src="assets/screenshot/ui/update260503/terminal-port-forward-expanded.png" alt="Rune expanded port-forwarding" width="49%">
  <img src="assets/screenshot/ui/update260503/terminal-pod-shell-session-picker.png" alt="Rune shell session picker" width="49%">
</p>

### Command Palette

<p>
  <img src="assets/screenshot/ui/update260503/command-palette-default-actions.png" alt="Rune command palette actions" width="49%">
  <img src="assets/screenshot/ui/update260503/command-palette-pods-search-results.png" alt="Rune command palette pods" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/command-palette-deployments-search-results.png" alt="Rune command palette deployments" width="49%">
  <img src="assets/screenshot/ui/update260503/command-palette-services-search-results.png" alt="Rune command palette services" width="49%">
</p>

<p>
  <img src="assets/screenshot/ui/update260503/command-palette-namespaces-search-results.png" alt="Rune command palette namespaces" width="49%">
  <img src="assets/screenshot/ui/update260503/command-palette-cronjobs-shortcut.png" alt="Rune command palette CronJobs" width="49%">
</p>

## Navigation

- **Main sections:** use the sidebar or **Cmd+1** through **Cmd+9**: Overview, Workloads, Networking, Storage, Config, RBAC, Events, Helm, and Terminal.
- **Toolbar:** choose the Kubernetes context and namespace for the data you browse.
- **History:** use **Cmd+Option+Left Arrow** and **Cmd+Option+Right Arrow** to move back and forward in the navigation stack.
- **Reload:** use **Cmd+R** to refresh the current view.

## Command Palette

Open the palette with **Cmd+K**, or click the **Palette** button in the toolbar. Search by free text across contexts, namespaces, resources, and actions, or type a `:` prefix to run command-style jumps.

- **Syntax:** `:command` or `:command filter`, for example `:po api`, `:svc billing`, or `:ns kube-system`.
- **Cluster and scope:** `:ctx` switches context and `:ns` switches namespace.
- **Workloads:** `:po` / `:pod`, `:deploy`, `:sts`, `:ds`, and `:wl`.
- **Networking:** `:svc` / `:service` / `:services`, `:ing`, and `:net`.
- **Configuration:** `:cm`, `:sec`, and `:cfg`.
- **RBAC:** `:rbac`, `:role`, `:rb`, `:cr`, and `:crb`.
- **Helm:** `:helm` / `:hr` opens Helm releases and can filter by release name.
- **More:** `:ev`, `:reload`, `:import`, `:ro`, and `:readonly`.

Type `:` by itself to see the built-in command cheat sheet.

## Privacy

Rune does not collect personal data or usage data. It does not use analytics, tracking, advertising, or telemetry, and it does not send your cluster data to a Rune backend.

The only network traffic is the traffic required for Rune to communicate with the Kubernetes clusters and services you choose to connect to. Kubeconfig files, cluster endpoints, credentials, Keychain items, and security-scoped bookmarks are handled locally on your Mac.

See [PRIVACY.md](PRIVACY.md) for the full privacy policy.

## Requirements

- macOS 14 or later
- Swift 6, for example via Xcode
- Rune talks to Kubernetes through its native in-app Kubernetes client.

## Build and Run

```bash
swift build
swift run RuneApp
```

Release build:

```bash
swift build -c release --product RuneApp
```

## App Bundle

```bash
./scripts/build-macos-app.sh
```

Produces `dist/Rune.app`.

## Development

```bash
swift test
```

## License

Rune is source-available. Personal and other non-commercial use is free.
Business use requires a paid commercial license or an authorized Mac App Store
purchase. See [LICENSE](LICENSE).
