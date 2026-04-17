# Sparade UI-diffar (YAML + högerpanel)

**Commit som innehåller allt:** `06f5c23` (använd `git show 06f5c23` eller `git format-patch -1 06f5c23` för hela ändringen).

## `yaml-and-right-panel-ui.patch`

Innehåller diff (från commit ovan) endast för:

- `Sources/RuneCore/State/RuneAppState.swift` — YAML-baseline, `updateResourceYAMLDraft`, revert, dirty-flag
- `Sources/RuneUI/ViewModels/RuneAppViewModel.swift` — bl.a. `importResourceYAMLFromFile`, `revertResourceYAMLDraft`
- `Sources/RuneUI/Views/RuneRootView.swift` — **hela filens ändringar**, inkl. workloads-lista / `contentPane`-layout (kan vara det du vill backa)

**Återanvändning efter rollback:** applicera mot en ren branch och granska konflikter, särskilt i `RuneRootView.swift` om du bara vill behålla YAML-fliken + inspector och **inte** workload-layoutändringar.

```bash
git apply --reject --whitespace=fix patches/yaml-and-right-panel-ui.patch
```

Hela projektändringen (kubectl, tester, m.m.) ligger i samma commit; denna patch är en **urklippning** av tre filer för enklare återinförande av YAML/högerpanel efter rollback.
