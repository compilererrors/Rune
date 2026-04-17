# Rune

Rune is a **native macOS client for Kubernetes**: contexts, namespaces, resources, logs, YAML/describe, and Helm — with clear navigation and a command palette.

## Screenshots

### Overview

![Overview](assets/screenshots/overview.png)

### Workloads

![Workloads](assets/screenshots/workloads.png)

### Networking

![Networking](assets/screenshots/networking.png)

### Storage

![Storage](assets/screenshots/storage.png)

### Configuration

![Configuration](assets/screenshots/config.png)

### RBAC

![RBAC](assets/screenshots/rbac.png)

### Events

![Events](assets/screenshots/events.png)

### Helm

![Helm](assets/screenshots/helm.png)

### Terminal

![Terminal](assets/screenshots/terminal.png)

### Command palette

![Command palette](assets/screenshots/command-palette.png)

## Requirements

- macOS 14 or later  
- Swift 6 (e.g. via Xcode)  
- `kubectl` on your `PATH` when the app talks to a cluster  

## Build and run

```bash
swift build
swift run RuneApp
```

Release:

```bash
swift build -c release --product RuneApp
```

## App bundle

```bash
./scripts/build-macos-app.sh
```

Produces `dist/Rune.app`.

## Development

```bash
swift test
```
