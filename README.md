# Rune

Rune is a fast native macOS Kubernetes cluster manager for people who debug real clusters every day.

It is built for the workflow many of us like in `kubectl` and `k9s`: quick navigation, strong keyboard support, direct access to resources, and no heavy platform console in the way. Rune adds a native macOS interface with more room for logs, YAML, describe output, port-forwarding, metrics, Helm, events, and focused troubleshooting without turning into bloat.

![Rune overview](assets/screenshot/readme/Overview.png)

## Why Rune

Kubernetes debugging is often about keeping enough context in view: the pod, its controller, related service, current YAML, recent events, and the logs that actually contain the error. Rune keeps those workflows close together.

- Browse contexts, namespaces, workloads, networking, storage, config, RBAC, events, Helm, and a terminal view.
- Inspect full pod logs and unified logs across related workloads.
- Search through multi-pod logs without constantly changing log limits or jumping between panes.
- Edit YAML with syntax highlighting plus validation feedback for errors and warnings.
- Port-forward pods and services from the resource you are already inspecting.
- Open exec and terminal workflows in context.
- Check pod and node metrics when your cluster exposes metrics.
- Move quickly with a command palette and k9s-style resource jumps.
- Stay local: Rune does not use analytics, tracking, advertising, or telemetry.

## Screenshots

### Cluster Overview

![Rune cluster overview](assets/screenshot/readme/Overview.png)

Rune keeps cluster status, resource navigation, metrics, and the active inspector in one native macOS workspace.

### Workloads, Logs, and YAML

<table>
  <tr>
    <td width="50%">
      <img src="assets/screenshot/readme/WorkloadsOverview.png" alt="Rune workloads overview">
      <br>
      <sub>Browse workloads while keeping the active resource inspector visible.</sub>
    </td>
    <td width="50%">
      <img src="assets/screenshot/readme/WorkloadsLogs.png" alt="Rune pod logs">
      <br>
      <sub>Read and search pod logs without leaving the workload context.</sub>
    </td>
  </tr>
  <tr>
    <td width="50%">
      <img src="assets/screenshot/readme/UnifiedLogs.png" alt="Rune unified logs">
      <br>
      <sub>Unified workload logs make related pod output easier to search and compare.</sub>
    </td>
    <td width="50%">
      <img src="assets/screenshot/readme/EditYaml.png" alt="Rune YAML editor sheet">
      <br>
      <sub>Edit YAML in a focused editor with syntax highlighting and validation feedback.</sub>
    </td>
  </tr>
  <tr>
    <td width="50%">
      <img src="assets/screenshot/readme/YamlError.png" alt="Rune YAML validation">
      <br>
      <sub>YAML problems are surfaced directly in the editor.</sub>
    </td>
    <td width="50%">
      <img src="assets/screenshot/readme/WorkloadsDescribe.png" alt="Rune describe view">
      <br>
      <sub>Describe output stays close to YAML, logs, exec, and port-forward actions.</sub>
    </td>
  </tr>
</table>

### Networking, Config, Storage, and Terminal

<table>
  <tr>
    <td width="50%">
      <img src="assets/screenshot/readme/DeploymentYaml.png" alt="Rune deployment YAML view">
      <br>
      <sub>Inspect controller YAML without losing surrounding context.</sub>
    </td>
    <td width="50%">
      <img src="assets/screenshot/readme/DeploymentDescribe.png" alt="Rune deployment describe view">
      <br>
      <sub>Describe output is available in the same resource workflow.</sub>
    </td>
  </tr>
  <tr>
    <td width="50%">
      <img src="assets/screenshot/readme/ConfigYaml.png" alt="Rune config YAML view">
      <br>
      <sub>Configuration resources can be browsed and inspected directly.</sub>
    </td>
    <td width="50%">
      <img src="assets/screenshot/readme/RbacYaml.png" alt="Rune RBAC YAML view">
      <br>
      <sub>RBAC resources use the same YAML and describe workflow.</sub>
    </td>
  </tr>
  <tr>
    <td width="50%">
      <img src="assets/screenshot/readme/StorageYaml.png" alt="Rune storage YAML view">
      <br>
      <sub>Storage resources and PVC YAML are available for inspection.</sub>
    </td>
    <td width="50%">
      <img src="assets/screenshot/readme/Event.png" alt="Rune event detail">
      <br>
      <sub>Events stay close to the resources you are investigating.</sub>
    </td>
  </tr>
  <tr>
    <td width="50%">
      <img src="assets/screenshot/readme/Terminal.png" alt="Rune terminal view">
      <br>
      <sub>Drop into a terminal when the direct command line is the right tool.</sub>
    </td>
    <td width="50%">
      <img src="assets/screenshot/readme/PortForward.png" alt="Rune port forward view">
      <br>
      <sub>Port-forward controls are available from resource and terminal workflows.</sub>
    </td>
  </tr>
</table>

### Command Palette

<table>
  <tr>
    <td width="50%">
      <img src="assets/screenshot/readme/41-command-palette-pods.png" alt="Rune command palette pods">
      <br>
      <sub>Jump to pods with palette commands like <code>:po</code>.</sub>
    </td>
    <td width="50%">
      <img src="assets/screenshot/readme/42-command-palette-deployments.png" alt="Rune command palette deployments">
      <br>
      <sub>Find deployments quickly from the keyboard.</sub>
    </td>
  </tr>
  <tr>
    <td width="50%">
      <img src="assets/screenshot/readme/43-command-palette-services.png" alt="Rune command palette services">
      <br>
      <sub>Navigate to services without clicking through sections.</sub>
    </td>
    <td width="50%">
      <img src="assets/screenshot/readme/44-command-palette-configmaps.png" alt="Rune command palette config maps">
      <br>
      <sub>Open config maps and other resources by name.</sub>
    </td>
  </tr>
  <tr>
    <td width="50%">
      <img src="assets/screenshot/readme/45-command-palette-rbac.png" alt="Rune command palette RBAC">
      <br>
      <sub>RBAC resources are available from the same command flow.</sub>
    </td>
    <td width="50%">
      <img src="assets/screenshot/readme/46-command-palette-reload.png" alt="Rune command palette reload">
      <br>
      <sub>Refresh and app actions live in the palette too.</sub>
    </td>
  </tr>
</table>

## Navigation

- **Main sections:** use the sidebar or **Cmd+1** through **Cmd+9**: Overview, Workloads, Networking, Storage, Config, RBAC, Events, Helm, and Terminal.
- **Toolbar:** choose the Kubernetes context and namespace for the data you browse.
- **History:** use **Cmd+Option+[** and **Cmd+Option+]** to move back and forward in the navigation stack.
- **Reload:** use **Cmd+R** to refresh the current view.

## Command Palette

Open the palette with **Cmd+K**, or click the **Palette** button in the toolbar. Search by free text across contexts, namespaces, resources, and actions, or type a `:` prefix to run command-style jumps.

- **Syntax:** `:command` or `:command filter`, for example `:po api`, `:svc billing`, or `:ns kube-system`.
- **Cluster and scope:** `:ctx` switches context and `:ns` switches namespace.
- **Workloads:** `:po` / `:pod`, `:deploy`, `:sts`, `:ds`, and `:wl`.
- **Networking:** `:svc` / `:service` / `:services`, `:ing`, and `:net`.
- **Configuration:** `:cm`, `:sec`, and `:cfg`.
- **RBAC:** `:rbac`, `:role`, `:rb`, `:cr`, and `:crb`.
- **More:** `:ev`, `:helm`, `:hr`, `:reload`, `:import`, `:ro`, and `:readonly`.

Type `:` by itself to see the built-in command cheat sheet.

## Privacy

Rune does not collect personal data or usage data. It does not use analytics, tracking, advertising, or telemetry, and it does not send your cluster data to a Rune backend.

The only network traffic is the traffic required for Rune to communicate with the Kubernetes clusters and services you choose to connect to.

## Requirements

- macOS 14 or later
- Swift 6, for example via Xcode
- `kubectl` on your `PATH` when the app talks to a cluster

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
