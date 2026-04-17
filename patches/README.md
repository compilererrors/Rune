# Sparade UI-diffar (YAML + högerpanel)

## `yaml-and-right-panel-ui.patch`

Innehåller **ocommittad** diff mot `HEAD` för:

- `Sources/RuneCore/State/RuneAppState.swift` — YAML-baseline, `updateResourceYAMLDraft`, revert, dirty-flag
- `Sources/RuneUI/ViewModels/RuneAppViewModel.swift` — bl.a. `importResourceYAMLFromFile`, `revertResourceYAMLDraft`
- `Sources/RuneUI/Views/RuneRootView.swift` — **hela filens ändringar**, inkl. workloads-lista / `contentPane`-layout (kan vara det du vill backa)

**Återanvändning efter rollback:** applicera mot en ren branch och granska konflikter, särskilt i `RuneRootView.swift` om du bara vill behålla YAML-fliken + inspector och **inte** workload-layoutändringar.

```bash
git apply --reject --whitespace=fix patches/yaml-and-right-panel-ui.patch
```

Efter commit finns samma innehåll i git-historiken; patchen motsvarar vad som committades om inget annat ändrades i dessa filer före commit.
