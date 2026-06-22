import XCTest
@testable import RuneCore

final class RuneCoreTests: XCTestCase {
    @MainActor
    func testResourceListFreshnessTracksRefreshingLiveAndStalePerFamily() {
        let state = RuneAppState()
        let loadedAt = Date(timeIntervalSince1970: 1_700)

        state.markResourceListsRefreshing([.pods, .services], message: "Refreshing synthetic")

        XCTAssertEqual(state.freshness(for: .pods).status, .refreshing)
        XCTAssertNil(state.freshness(for: .pods).updatedAt)
        XCTAssertEqual(state.freshness(for: .services).status, .refreshing)
        XCTAssertEqual(state.freshness(for: .events).status, .idle)

        state.markResourceListsLive([.pods], updatedAt: loadedAt, message: "Live synthetic")
        state.markResourceListsFailed([.services], message: "Partial load: services forbidden")

        XCTAssertEqual(state.freshness(for: .pods).status, .live)
        XCTAssertEqual(state.freshness(for: .pods).updatedAt, loadedAt)
        XCTAssertEqual(state.freshness(for: .pods).message, "Live synthetic")
        XCTAssertEqual(state.freshness(for: .services).status, .failed)
        XCTAssertNil(state.freshness(for: .services).updatedAt)

        state.markResourceListsLive([.services], updatedAt: loadedAt, message: "Live synthetic")
        state.markResourceListsFailed([.services], message: "Partial load: services timeout")

        XCTAssertEqual(state.freshness(for: .services).status, .stale)
        XCTAssertEqual(state.freshness(for: .services).updatedAt, loadedAt)
        XCTAssertEqual(state.freshness(for: .services).message, "Partial load: services timeout")

        state.markResourceListsReconnecting([.pods, .events], message: "Refresh cancelled; reconnecting")

        XCTAssertEqual(state.freshness(for: .pods).status, .reconnecting)
        XCTAssertEqual(state.freshness(for: .pods).updatedAt, loadedAt)
        XCTAssertEqual(state.freshness(for: .events).status, .reconnecting)
        XCTAssertNil(state.freshness(for: .events).updatedAt)
        XCTAssertEqual(state.freshness(for: .events).message, "Refresh cancelled; reconnecting")

        state.clearResourceListFreshness()

        XCTAssertEqual(state.freshness(for: .pods).status, .idle)
        XCTAssertEqual(state.freshness(for: .services).status, .idle)
    }

    func testResourceListFreshnessMapsSnapshotWarningLabelsToFamilies() {
        XCTAssertEqual(RuneResourceListFamily(snapshotWarningLabel: "pods"), .pods)
        XCTAssertEqual(RuneResourceListFamily(snapshotWarningLabel: "services-count"), .services)
        XCTAssertEqual(RuneResourceListFamily(snapshotWarningLabel: "cronjobs-count"), .cronJobs)
        XCTAssertEqual(RuneResourceListFamily(snapshotWarningLabel: "roles"), .rbacRoles)
        XCTAssertEqual(RuneResourceListFamily(snapshotWarningLabel: "clusterrolebindings"), .rbacClusterRoleBindings)
        XCTAssertNil(RuneResourceListFamily(snapshotWarningLabel: "unknown-resource"))
    }

    @MainActor
    func testAppendPodLogTailTrimsOlderSegmentsWhenSessionCacheExceedsCharacterBudget() {
        let state = RuneAppState()
        let earlySentinel = "RUNE_EARLY_TAIL_SENTINEL"
        let firstBody = "\(earlySentinel)\n\(String(repeating: "a", count: 520_000))"
        state.appendPodLogRead(firstBody, contextName: "ctx", namespace: "ns", podName: "pod-a")

        let secondBody = String(repeating: "b", count: 520_000)
        state.appendPodLogRead(secondBody, contextName: "ctx", namespace: "ns", podName: "pod-a")

        XCTAssertTrue(
            state.podLogs.contains("[older session log cache truncated]"),
            "Expected session log merge to exceed the in-memory cap and record truncation."
        )
        XCTAssertFalse(
            state.podLogs.contains(earlySentinel),
            "Tail-append mode must drop the oldest merged log bytes; otherwise the UI looks like logs “disappear” or never fully load."
        )
        XCTAssertTrue(state.podLogs.contains("b"), "The newest tail read should still be present after truncation.")
    }

    @MainActor
    func testSessionLogCacheEvictsLeastRecentlyUsedResourceEntries() {
        let defaults = UserDefaults.standard
        let oldValue = defaults.object(forKey: RuneSettingsKeys.sessionLogCacheEntryLimit)
        defer {
            if let oldValue {
                defaults.set(oldValue, forKey: RuneSettingsKeys.sessionLogCacheEntryLimit)
            } else {
                defaults.removeObject(forKey: RuneSettingsKeys.sessionLogCacheEntryLimit)
            }
        }
        defaults.runeSessionLogCacheEntryLimit = RuneSettingsKeys.sessionLogCacheEntryLimitDefault

        let state = RuneAppState()

        for index in 0..<129 {
            state.appendPodLogRead(
                "line \(index)",
                contextName: "context-synthetic",
                namespace: "namespace-synthetic",
                podName: "pod-\(index)",
                loadedAt: Date(timeIntervalSince1970: TimeInterval(index))
            )
        }

        XCTAssertEqual(state.sessionLogCache.count, 128)
        XCTAssertEqual(
            state.cachedLogs(
                contextName: "context-synthetic",
                namespace: "namespace-synthetic",
                kind: .pod,
                resourceName: "pod-0"
            ),
            ""
        )
        XCTAssertTrue(
            state.cachedLogs(
                contextName: "context-synthetic",
                namespace: "namespace-synthetic",
                kind: .pod,
                resourceName: "pod-1"
            ).contains("line 1")
        )

        state.appendPodLogRead(
            "line 129",
            contextName: "context-synthetic",
            namespace: "namespace-synthetic",
            podName: "pod-129",
            loadedAt: Date(timeIntervalSince1970: 129)
        )

        XCTAssertEqual(state.sessionLogCache.count, 128)
        XCTAssertTrue(
            state.cachedLogs(
                contextName: "context-synthetic",
                namespace: "namespace-synthetic",
                kind: .pod,
                resourceName: "pod-1"
            ).contains("line 1")
        )
        XCTAssertEqual(
            state.cachedLogs(
                contextName: "context-synthetic",
                namespace: "namespace-synthetic",
                kind: .pod,
                resourceName: "pod-2"
            ),
            ""
        )
    }

    @MainActor
    func testSessionLogCacheUsesConfiguredLowerEntryLimit() {
        let defaults = UserDefaults.standard
        let oldValue = defaults.object(forKey: RuneSettingsKeys.sessionLogCacheEntryLimit)
        defer {
            if let oldValue {
                defaults.set(oldValue, forKey: RuneSettingsKeys.sessionLogCacheEntryLimit)
            } else {
                defaults.removeObject(forKey: RuneSettingsKeys.sessionLogCacheEntryLimit)
            }
        }
        defaults.runeSessionLogCacheEntryLimit = RuneSettingsKeys.sessionLogCacheEntryLimitMinimum

        let state = RuneAppState()

        for index in 0..<20 {
            state.replacePodLogRead(
                "line \(index)",
                contextName: "context-synthetic",
                namespace: "namespace-synthetic",
                podName: "pod-\(index)",
                loadedAt: Date(timeIntervalSince1970: TimeInterval(index))
            )
        }

        XCTAssertEqual(state.sessionLogCache.count, RuneSettingsKeys.sessionLogCacheEntryLimitMinimum)
        XCTAssertEqual(
            state.cachedLogs(
                contextName: "context-synthetic",
                namespace: "namespace-synthetic",
                kind: .pod,
                resourceName: "pod-0"
            ),
            ""
        )
        XCTAssertTrue(
            state.cachedLogs(
                contextName: "context-synthetic",
                namespace: "namespace-synthetic",
                kind: .pod,
                resourceName: "pod-19"
            ).contains("line 19")
        )
    }

    @MainActor
    func testResourceYAMLUndoHistoryIsBounded() {
        let defaults = UserDefaults.standard
        let oldValue = defaults.object(forKey: RuneSettingsKeys.resourceYAMLUndoSnapshotLimit)
        defer {
            if let oldValue {
                defaults.set(oldValue, forKey: RuneSettingsKeys.resourceYAMLUndoSnapshotLimit)
            } else {
                defaults.removeObject(forKey: RuneSettingsKeys.resourceYAMLUndoSnapshotLimit)
            }
        }
        defaults.runeResourceYAMLUndoSnapshotLimit = RuneSettingsKeys.resourceYAMLUndoSnapshotLimitDefault

        let state = RuneAppState()
        state.setResourceYAML("manifest-0")

        for index in 1...70 {
            state.updateResourceYAMLDraft("manifest-\(index)")
        }

        var undoCount = 0
        while state.canUndoResourceYAMLEdit {
            state.undoResourceYAMLEdit()
            undoCount += 1
        }

        XCTAssertEqual(undoCount, 64)
        XCTAssertEqual(state.resourceYAML, "manifest-6")
    }

    @MainActor
    func testResourceYAMLUndoHistoryUsesConfiguredLowerLimit() {
        let defaults = UserDefaults.standard
        let oldValue = defaults.object(forKey: RuneSettingsKeys.resourceYAMLUndoSnapshotLimit)
        defer {
            if let oldValue {
                defaults.set(oldValue, forKey: RuneSettingsKeys.resourceYAMLUndoSnapshotLimit)
            } else {
                defaults.removeObject(forKey: RuneSettingsKeys.resourceYAMLUndoSnapshotLimit)
            }
        }
        defaults.runeResourceYAMLUndoSnapshotLimit = RuneSettingsKeys.resourceYAMLUndoSnapshotLimitMinimum

        let state = RuneAppState()
        state.setResourceYAML("manifest-0")

        for index in 1...12 {
            state.updateResourceYAMLDraft("manifest-\(index)")
        }

        var undoCount = 0
        while state.canUndoResourceYAMLEdit {
            state.undoResourceYAMLEdit()
            undoCount += 1
        }

        XCTAssertEqual(undoCount, RuneSettingsKeys.resourceYAMLUndoSnapshotLimitMinimum)
        XCTAssertEqual(state.resourceYAML, "manifest-4")
    }

    func testPerformanceMemorySettingsClampAndPersist() {
        let suiteName = "RuneCoreTests.performanceMemory.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }

        XCTAssertEqual(defaults.runeSessionLogCacheEntryLimit, RuneSettingsKeys.sessionLogCacheEntryLimitDefault)
        XCTAssertEqual(defaults.runeResourceYAMLUndoSnapshotLimit, RuneSettingsKeys.resourceYAMLUndoSnapshotLimitDefault)

        defaults.runeSessionLogCacheEntryLimit = 1
        defaults.runeResourceYAMLUndoSnapshotLimit = 1

        XCTAssertEqual(defaults.runeSessionLogCacheEntryLimit, RuneSettingsKeys.sessionLogCacheEntryLimitMinimum)
        XCTAssertEqual(defaults.runeResourceYAMLUndoSnapshotLimit, RuneSettingsKeys.resourceYAMLUndoSnapshotLimitMinimum)

        let powerUserLogCacheLimit = 10_000
        let powerUserUndoLimit = 5_000
        defaults.runeSessionLogCacheEntryLimit = powerUserLogCacheLimit
        defaults.runeResourceYAMLUndoSnapshotLimit = powerUserUndoLimit

        XCTAssertEqual(defaults.runeSessionLogCacheEntryLimit, powerUserLogCacheLimit)
        XCTAssertEqual(defaults.runeResourceYAMLUndoSnapshotLimit, powerUserUndoLimit)
    }

    func testTerminalWorkspacePersistenceSettingDefaultsOffAndPersists() {
        let suiteName = "RuneCoreTests.terminalWorkspacePersistence.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }

        XCTAssertFalse(defaults.runePersistTerminalWorkspaceState)

        defaults.runePersistTerminalWorkspaceState = true
        XCTAssertTrue(defaults.runePersistTerminalWorkspaceState)

        defaults.runePersistTerminalWorkspaceState = false
        XCTAssertFalse(defaults.runePersistTerminalWorkspaceState)
    }

    func testAppearanceRecentThemesRecordDedupeAndTrim() {
        let suiteName = "RuneCoreTests.appearanceRecentThemes.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }

        XCTAssertEqual(defaults.runeAppearanceRecentThemes, [RuneSettingsKeys.appearanceThemeDefault])

        defaults.recordRuneAppearanceTheme("theme-a", limit: 3)
        defaults.recordRuneAppearanceTheme("theme-b", limit: 3)
        defaults.recordRuneAppearanceTheme("theme-c", limit: 3)
        defaults.recordRuneAppearanceTheme("theme-b", limit: 3)
        defaults.recordRuneAppearanceTheme("theme-d", limit: 3)

        XCTAssertEqual(defaults.runeAppearanceRecentThemes, ["theme-d", "theme-b", "theme-c"])

        defaults.runeAppearanceRecentThemes = [" theme-x ", "theme-y", "theme-x", "", "theme-z"]

        XCTAssertEqual(defaults.runeAppearanceRecentThemes, ["theme-x", "theme-y", "theme-z"])
    }

    func testExportDestinationSettingsPersistWithoutDefaultLocalPaths() {
        let suiteName = "RuneCoreTests.exportDestination.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }

        XCTAssertNil(defaults.runeExportFolderBookmarkData)
        XCTAssertEqual(defaults.runeExportFolderDisplayName, "")
        XCTAssertNil(defaults.runeExportTextOpenerBundleIdentifier)
        XCTAssertNil(defaults.runeExportArchiveOpenerBundleIdentifier)
        XCTAssertFalse(defaults.runeExportUsesPrivacySafeFilenames)

        let bookmark = Data([0x52, 0x75, 0x6e, 0x65])
        defaults.runeExportFolderBookmarkData = bookmark
        defaults.runeExportFolderDisplayName = " Synthetic Exports "
        defaults.runeExportTextOpenerBundleIdentifier = " com.example.TextViewer "
        defaults.runeExportArchiveOpenerBundleIdentifier = ""
        defaults.runeExportUsesPrivacySafeFilenames = true

        XCTAssertEqual(defaults.runeExportFolderBookmarkData, bookmark)
        XCTAssertEqual(defaults.runeExportFolderDisplayName, "Synthetic Exports")
        XCTAssertEqual(defaults.runeExportTextOpenerBundleIdentifier, "com.example.TextViewer")
        XCTAssertNil(defaults.runeExportArchiveOpenerBundleIdentifier)
        XCTAssertTrue(defaults.runeExportUsesPrivacySafeFilenames)

        defaults.runeExportFolderBookmarkData = nil
        XCTAssertNil(defaults.runeExportFolderBookmarkData)
    }

    func testRuneKeyboardShortcutParsesAndMatchesShiftBinding() {
        let shortcut = RuneKeyboardShortcut(storageValue: "shift-f")

        XCTAssertEqual(shortcut?.key, "f")
        XCTAssertEqual(shortcut?.displayValue, "⇧F")
        XCTAssertTrue(shortcut?.matches(baseKey: "f", requiresShift: true) ?? false)
        XCTAssertFalse(shortcut?.matches(baseKey: "f", requiresShift: false) ?? true)
    }

    func testRuneKeyboardShortcutParsesCommandOptionArrowBinding() {
        let shortcut = RuneKeyboardShortcut(storageValue: "option-command-left")

        XCTAssertEqual(shortcut?.key, "left")
        XCTAssertEqual(shortcut?.storageValue, "command-option-left")
        XCTAssertEqual(shortcut?.displayValue, "⌘⌥←")
        XCTAssertTrue(shortcut?.matches(
            baseKey: "left",
            requiresShift: false,
            requiresCommand: true,
            requiresOption: true
        ) ?? false)
        XCTAssertFalse(shortcut?.matches(
            baseKey: "[",
            requiresShift: false,
            requiresCommand: false,
            requiresOption: true
        ) ?? true)
    }

    func testRuneKeyboardShortcutParsesControlBinding() {
        let shortcut = RuneKeyboardShortcut(storageValue: "control-d")

        XCTAssertEqual(shortcut?.key, "d")
        XCTAssertEqual(shortcut?.storageValue, "control-d")
        XCTAssertEqual(shortcut?.displayValue, "⌃D")
        XCTAssertTrue(shortcut?.matches(
            baseKey: "d",
            requiresShift: false,
            requiresControl: true
        ) ?? false)
        XCTAssertFalse(shortcut?.matches(
            baseKey: "d",
            requiresShift: false
        ) ?? true)
    }

    func testRuneKeyboardShortcutParsesShiftPeriodForSwedishColonKey() {
        let shortcut = RuneKeyboardShortcut(storageValue: "shift-.")

        XCTAssertEqual(shortcut?.key, ".")
        XCTAssertEqual(shortcut?.storageValue, "shift-.")
        XCTAssertEqual(shortcut?.displayValue, "⇧.")
        XCTAssertTrue(shortcut?.matches(
            baseKey: ".",
            requiresShift: true
        ) ?? false)
        XCTAssertFalse(shortcut?.matches(
            baseKey: ":",
            requiresShift: false
        ) ?? true)
    }

    func testRuneKeyboardShortcutRejectsUnsupportedValues() {
        XCTAssertNil(RuneKeyboardShortcut(storageValue: "shift--"))
        XCTAssertNil(RuneKeyboardShortcut(storageValue: "describe"))
        XCTAssertNil(RuneKeyboardShortcut(storageValue: "shift-shift-f"))
        XCTAssertNil(RuneKeyboardShortcut(key: "-", requiresShift: false))
        XCTAssertNil(RuneKeyboardShortcut(key: "up", requiresShift: false))
    }

    func testUserDefaultsFallsBackToDefaultRuneKeyBindingShortcut() {
        let suiteName = "RuneCoreTests-\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        let action = RuneKeyBindingAction.describe

        defer { defaults.removePersistentDomain(forName: suiteName) }

        XCTAssertEqual(defaults.runeKeyBindingShortcut(for: action), action.defaultShortcut)

        let custom = RuneKeyboardShortcut(key: "x", requiresShift: true)!
        defaults.setRuneKeyBindingShortcut(custom, for: action)

        XCTAssertEqual(defaults.runeKeyBindingShortcut(for: action), custom)
        XCTAssertEqual(
            defaults.runeKeyBindingShortcut(for: .historyBack),
            RuneKeyboardShortcut(key: "left", requiresShift: false, requiresCommand: true, requiresOption: true)
        )
    }

    func testUserDefaultsCommandPaletteShortcutCanPersistShiftPeriodWorkflow() {
        let suiteName = "RuneCoreTests.commandPaletteShortcut.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }

        let custom = RuneKeyboardShortcut(key: ".", requiresShift: true)!
        defaults.setRuneKeyBindingShortcut(custom, for: .commandPalette)
        let persisted = defaults.runeKeyBindingShortcut(for: .commandPalette)
        let resolver = RuneKeyBindingResolver { action in
            action == .commandPalette ? persisted : action.defaultShortcut
        }

        XCTAssertEqual(persisted, custom)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: ".", modifiers: [.shift])), .commandPalette)
        XCTAssertNil(resolver.action(for: RuneKeyBindingInput(baseKey: ":", modifiers: [])))
    }

    func testHoverTooltipSettingDefaultsOnAndPersists() {
        let suiteName = "RuneCoreTests.tooltips.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }

        XCTAssertTrue(defaults.runeShowHoverTooltips)

        defaults.runeShowHoverTooltips = false

        XCTAssertFalse(defaults.runeShowHoverTooltips)
    }

    func testDefaultRuneKeyBindingsResolveExactActions() {
        let resolver = RuneKeyBindingResolver()

        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: ":", modifiers: [])), .commandPalette)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "/", modifiers: [])), .filterResources)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "left", modifiers: [.command, .option])), .historyBack)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "right", modifiers: [.command, .option])), .historyForward)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "left", modifiers: [.command, .shift])), .focusPreviousPane)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "right", modifiers: [.command, .shift])), .focusNextPane)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "d", modifiers: [])), .describe)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "l", modifiers: [])), .logs)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "s", modifiers: [.command])), .saveLogs)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "s", modifiers: [.control])), .saveLogs)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "s", modifiers: [])), .shell)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "e", modifiers: [])), .edit)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "y", modifiers: [])), .yaml)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "d", modifiers: [.control])), .delete)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "f", modifiers: [.shift])), .portForward)
        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "r", modifiers: [])), .rollout)
    }

    func testRuneKeyBindingResolverRejectsTextInputFunctionAndExtraModifiers() {
        let resolver = RuneKeyBindingResolver()

        XCTAssertNil(resolver.action(for: RuneKeyBindingInput(baseKey: "d", modifiers: [], isTextInputFocused: true)))
        XCTAssertNil(resolver.action(for: RuneKeyBindingInput(baseKey: "d", modifiers: [.function])))
        XCTAssertNil(resolver.action(for: RuneKeyBindingInput(baseKey: "d", modifiers: [.shift])))
        XCTAssertNil(resolver.action(for: RuneKeyBindingInput(baseKey: "s", modifiers: [.command, .shift])))
        XCTAssertNil(resolver.action(for: RuneKeyBindingInput(baseKey: "left", modifiers: [.command])))
        XCTAssertNil(resolver.action(for: RuneKeyBindingInput(baseKey: "", modifiers: [])))
    }

    func testRuneKeyBindingResolverUsesCustomShortcutProvider() {
        let resolver = RuneKeyBindingResolver { action in
            if action == .describe {
                return RuneKeyboardShortcut(key: "x", requiresShift: true)!
            }
            return action.defaultShortcut
        }

        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: "x", modifiers: [.shift])), .describe)
        XCTAssertNil(resolver.action(for: RuneKeyBindingInput(baseKey: "d", modifiers: [])))
    }

    func testRuneKeyBindingResolverCanMapCommandPaletteToShiftPeriod() {
        let resolver = RuneKeyBindingResolver { action in
            if action == .commandPalette {
                return RuneKeyboardShortcut(key: ".", requiresShift: true)!
            }
            return action.defaultShortcut
        }

        XCTAssertEqual(resolver.action(for: RuneKeyBindingInput(baseKey: ".", modifiers: [.shift])), .commandPalette)
        XCTAssertNil(resolver.action(for: RuneKeyBindingInput(baseKey: ":", modifiers: [])))
    }

    func testDefaultRuneKeyBindingsHaveUniqueShortcuts() {
        let grouped = Dictionary(grouping: RuneKeyBindingAction.allCases) { action in
            action.defaultShortcut.storageValue
        }
        let conflicts = grouped.filter { $0.value.count > 1 }

        XCTAssertTrue(conflicts.isEmpty, "Conflicting default shortcuts: \(conflicts)")
    }

    func testExecutableSearchPathKeepsShellPathAndAddsMacFallbacks() {
        let directories = RuneExecutableSearchPath.directories(from: ["PATH": "/synthetic/bin:/usr/bin:/synthetic/bin"])

        XCTAssertEqual(directories.first, "/synthetic/bin")
        XCTAssertEqual(directories.filter { $0 == "/synthetic/bin" }.count, 1)
        XCTAssertTrue(directories.contains("/usr/bin"))
        XCTAssertTrue(directories.contains("/opt/homebrew/bin"))
        XCTAssertTrue(directories.contains("/usr/local/bin"))
    }

    func testShellCommandFormattingQuotesUnsafeArgumentsWithoutChangingSafeOnes() {
        XCTAssertEqual(ShellCommandFormatting.shellQuoted("kubectl"), "kubectl")
        XCTAssertEqual(ShellCommandFormatting.shellQuoted("pod/api:8080"), "pod/api:8080")
        XCTAssertEqual(ShellCommandFormatting.shellQuoted(""), "''")
        XCTAssertEqual(ShellCommandFormatting.shellQuoted("api service"), "'api service'")
        XCTAssertEqual(ShellCommandFormatting.shellQuoted("api;rm"), "'api;rm'")
        XCTAssertEqual(ShellCommandFormatting.shellQuoted("printf 'hello'"), "'printf '\\''hello'\\'''")
        XCTAssertEqual(
            ShellCommandFormatting.shellCommand([
                "kubectl",
                "--context",
                "prod west",
                "exec",
                "api;debug",
                "--",
                "sh",
                "-lc",
                "printf 'hello'"
            ]),
            "kubectl --context 'prod west' exec 'api;debug' -- sh -lc 'printf '\\''hello'\\'''"
        )
    }

    func testLogTimeFilterUsesSinceTimeOnlyForAbsoluteDate() {
        XCTAssertFalse(LogTimeFilter.lastMinutes(15).usesSinceTime)
        XCTAssertFalse(LogTimeFilter.lastHours(1).usesSinceTime)
        XCTAssertTrue(LogTimeFilter.since(Date(timeIntervalSince1970: 0)).usesSinceTime)
    }

    func testKubeConfigSourceUsesPathAsIdentifier() {
        let source = KubeConfigSource(url: URL(fileURLWithPath: "/tmp/kubeconfig"))

        XCTAssertEqual(source.id, "/tmp/kubeconfig")
        XCTAssertEqual(source.displayName, "kubeconfig")
    }

    func testKubernetesAgeDescribe() {
        let ref = Date(timeIntervalSince1970: 1_700_000_000)
        let age = KubernetesAgeFormatting.describe(creationISO8601: "2023-11-14T12:00:00Z", reference: ref)
        XCTAssertNotEqual(age, "—")
        XCTAssertFalse(age.isEmpty)
    }

    func testTerminalTranscriptSanitizerRemovesAnsiAndDeviceStatusQueries() {
        let raw = "/ # \u{001B}[6nls -la\r\n\u{001B}[1;34mbin\u{001B}[m\n"

        XCTAssertEqual(TerminalTranscriptSanitizer.sanitize(raw), "/ # ls -la\nbin\n")
    }

    func testTerminalTranscriptSanitizerCarriesSplitEscapeSequences() {
        var pendingEscape = ""

        let first = TerminalTranscriptSanitizer.sanitize("hello \u{001B}[1;", pendingEscape: &pendingEscape)
        let second = TerminalTranscriptSanitizer.sanitize("34mworld\u{001B}[m", pendingEscape: &pendingEscape)

        XCTAssertEqual(first, "hello ")
        XCTAssertEqual(second, "world")
        XCTAssertTrue(pendingEscape.isEmpty)
    }

    func testTerminalTranscriptSanitizerHandlesCarriageReturnProgressUpdates() {
        let raw = "pulling layer 10%\rpulling layer 20%\nready\n"

        XCTAssertEqual(TerminalTranscriptSanitizer.sanitize(raw), "pulling layer 20%\nready\n")
    }

    func testTerminalTranscriptSanitizerHandlesEraseLineSequences() {
        let raw = "pulling layer 10%\u{001B}[2Kpulling layer 20%\nready\n"

        XCTAssertEqual(TerminalTranscriptSanitizer.sanitize(raw), "pulling layer 20%\nready\n")
    }

    func testTerminalTranscriptSanitizerCarriesSplitEraseLineSequences() {
        var pendingEscape = ""

        let first = TerminalTranscriptSanitizer.sanitize("pulling layer 10%\u{001B}[", pendingEscape: &pendingEscape)
        let second = TerminalTranscriptSanitizer.sanitize("2Kpulling layer 20%\n", pendingEscape: &pendingEscape)

        XCTAssertEqual(first, "pulling layer 10%")
        XCTAssertEqual(second, "pulling layer 20%\n")
        XCTAssertTrue(pendingEscape.isEmpty)
    }

    func testTerminalTranscriptSanitizerHandlesCursorLeftOverwriteSequences() {
        let raw = "pulling layer 10%\u{001B}[3D20%\n"

        XCTAssertEqual(TerminalTranscriptSanitizer.sanitize(raw), "pulling layer 20%\n")
    }

    func testTerminalTranscriptSanitizerHandlesClearScreenSequences() {
        let raw = "stale menu\nold status\u{001B}[2Jfresh menu\nready\n"

        XCTAssertEqual(TerminalTranscriptSanitizer.sanitize(raw), "fresh menu\nready\n")
    }

    func testTerminalTranscriptSanitizerDropsCharsetAndSingleCharacterEscapes() {
        let raw = "\u{001B}(Bhello\u{001B}7world\u{001B}8"

        XCTAssertEqual(TerminalTranscriptSanitizer.sanitize(raw), "helloworld")
    }

    func testTerminalTranscriptSanitizerCarriesSplitCharsetEscapeSequences() {
        var pendingEscape = ""

        let first = TerminalTranscriptSanitizer.sanitize("hello \u{001B}(", pendingEscape: &pendingEscape)
        let second = TerminalTranscriptSanitizer.sanitize("Bworld", pendingEscape: &pendingEscape)

        XCTAssertEqual(first, "hello ")
        XCTAssertEqual(second, "world")
        XCTAssertTrue(pendingEscape.isEmpty)
    }

    func testTerminalFailureDiagnosticClassifiesRbacDenial() {
        let diagnostic = PodTerminalSessionDiagnostic.classify(
            errorMessage: "pods \"pod-0\" is forbidden: User cannot create resource \"pods/exec\" in namespace \"default\"",
            podName: "pod-0",
            containerName: "app",
            shell: "sh"
        )

        XCTAssertEqual(diagnostic.category, .rbacDenied)
        XCTAssertTrue(diagnostic.summary.contains("RBAC denied"))
        XCTAssertTrue(diagnostic.recoveryHint.contains("pods/exec"))
    }

    func testTerminalFailureDiagnosticClassifiesMissingShell() {
        let diagnostic = PodTerminalSessionDiagnostic.classify(
            errorMessage: #"exec: "bash": executable file not found in $PATH"#,
            podName: "pod-0",
            containerName: nil,
            shell: "bash"
        )

        XCTAssertEqual(diagnostic.category, .missingShell)
        XCTAssertTrue(diagnostic.summary.contains("Shell not found"))
        XCTAssertTrue(diagnostic.recoveryHint.contains("sh"))
    }

    func testTerminalFailureDiagnosticClassifiesPodOrContainerUnavailable() {
        let diagnostic = PodTerminalSessionDiagnostic.classify(
            errorMessage: #"container "worker" not found in pod "pod-0""#,
            podName: "pod-0",
            containerName: "worker",
            shell: "sh"
        )

        XCTAssertEqual(diagnostic.category, .podOrContainerUnavailable)
        XCTAssertTrue(diagnostic.summary.contains("Pod or container unavailable"))
        XCTAssertTrue(diagnostic.recoveryHint.contains("Refresh"))
    }

    func testTerminalFailureDiagnosticClassifiesPodRestart() {
        let diagnostic = PodTerminalSessionDiagnostic.classify(
            errorMessage: #"container "app" restarted while the exec session was attached"#,
            podName: "pod-0",
            containerName: "app",
            shell: "sh"
        )

        XCTAssertEqual(diagnostic.category, .podRestarted)
        XCTAssertTrue(diagnostic.summary.contains("Pod restarted"))
        XCTAssertTrue(diagnostic.recoveryHint.contains("Ready"))
    }

    func testTerminalFailureDiagnosticClassifiesTransportDisconnect() {
        let diagnostic = PodTerminalSessionDiagnostic.classify(
            errorMessage: "The network connection was lost while opening the exec stream.",
            podName: "pod-0",
            containerName: nil,
            shell: "sh"
        )

        XCTAssertEqual(diagnostic.category, .transportDisconnected)
        XCTAssertTrue(diagnostic.summary.contains("Terminal stream disconnected"))
        XCTAssertTrue(diagnostic.recoveryHint.contains("Reconnect"))
    }

    func testTerminalScrollbackRetentionKeepsRecentLinesWithMarker() {
        let transcript = (0..<6).map { "line \($0)" }.joined(separator: "\n")

        let retained = TerminalScrollbackRetention.retainingRecentLines(transcript, maxLines: 3)

        XCTAssertTrue(retained.hasPrefix(TerminalScrollbackRetention.truncationMarker))
        XCTAssertFalse(retained.contains("line 0"))
        XCTAssertFalse(retained.contains("line 2"))
        XCTAssertTrue(retained.contains("line 3"))
        XCTAssertTrue(retained.contains("line 5"))
    }

    func testTerminalScrollbackRetentionDoesNotTrimWithinLimit() {
        let transcript = "line 0\nline 1\nline 2"

        XCTAssertEqual(
            TerminalScrollbackRetention.retainingRecentLines(transcript, maxLines: 3),
            transcript
        )
    }

    func testKubernetesRequestRetryPolicyClassifiesTransientHTTPStatuses() {
        let throttled = KubernetesRequestRetryPolicy.classifyHTTPStatus(429, retryAfterHeader: "2")
        let unavailable = KubernetesRequestRetryPolicy.classifyHTTPStatus(503)
        let unauthorized = KubernetesRequestRetryPolicy.classifyHTTPStatus(401)

        XCTAssertTrue(throttled.isRetryable)
        XCTAssertEqual(throttled.category, .throttled)
        XCTAssertEqual(throttled.suggestedDelayNanoseconds, 2_000_000_000)
        XCTAssertTrue(unavailable.isRetryable)
        XCTAssertEqual(unavailable.category, .serverUnavailable)
        XCTAssertFalse(unauthorized.isRetryable)
        XCTAssertEqual(unauthorized.category, .notRetryable)
    }

    func testKubernetesRequestRetryPolicyClassifiesNetworkErrors() {
        let timedOut = KubernetesRequestRetryPolicy.classifyNetworkError(URLError(.timedOut))
        let disconnected = KubernetesRequestRetryPolicy.classifyNetworkError(URLError(.networkConnectionLost))
        let cancelled = KubernetesRequestRetryPolicy.classifyNetworkError(URLError(.cancelled))

        XCTAssertTrue(timedOut.isRetryable)
        XCTAssertEqual(timedOut.category, .networkTransient)
        XCTAssertTrue(disconnected.isRetryable)
        XCTAssertFalse(cancelled.isRetryable)
        XCTAssertEqual(cancelled.category, .cancelled)
    }

    func testKubernetesRequestRetryPolicyAllowsOnlySafeReadRetries() {
        let transient = KubernetesRequestRetryPolicy.classifyHTTPStatus(503)
        let permanent = KubernetesRequestRetryPolicy.classifyHTTPStatus(404)

        XCTAssertTrue(KubernetesRequestRetryPolicy.shouldRetry(method: "GET", decision: transient, attempt: 1))
        XCTAssertTrue(KubernetesRequestRetryPolicy.shouldRetry(method: "head", decision: transient, attempt: 1))
        XCTAssertFalse(KubernetesRequestRetryPolicy.shouldRetry(method: "POST", decision: transient, attempt: 1))
        XCTAssertFalse(KubernetesRequestRetryPolicy.shouldRetry(method: "PATCH", decision: transient, attempt: 1))
        XCTAssertFalse(KubernetesRequestRetryPolicy.shouldRetry(method: "GET", decision: permanent, attempt: 1))
        XCTAssertFalse(KubernetesRequestRetryPolicy.shouldRetry(method: "GET", decision: transient, attempt: 3))
    }

    func testKubernetesRequestRetryPolicyBackoffIsBoundedAndDeterministic() {
        let throttled = KubernetesRequestRetryPolicy.classifyHTTPStatus(429, retryAfterHeader: "10")
        let transient = KubernetesRequestRetryPolicy.classifyNetworkError(URLError(.timedOut))

        XCTAssertEqual(KubernetesRequestRetryPolicy.boundedDelayNanoseconds(for: throttled, attempt: 1), 2_000_000_000)
        XCTAssertEqual(KubernetesRequestRetryPolicy.boundedDelayNanoseconds(for: transient, attempt: 1), 500_000_000)
        XCTAssertEqual(KubernetesRequestRetryPolicy.boundedDelayNanoseconds(for: transient, attempt: 2), 1_000_000_000)
        XCTAssertEqual(KubernetesRequestRetryPolicy.boundedDelayNanoseconds(for: transient, attempt: 3), 2_000_000_000)
    }

    func testPendingLaunchActionRoundTripsAndConsumesFromUserDefaults() {
        let suiteName = "RuneCoreTests.pendingLaunchAction.\(UUID().uuidString)"
        let defaults = UserDefaults(suiteName: suiteName)!
        defer { defaults.removePersistentDomain(forName: suiteName) }

        XCTAssertNil(defaults.runePendingLaunchAction)

        defaults.runePendingLaunchAction = .savedWorkspaces

        XCTAssertEqual(defaults.runePendingLaunchAction, .savedWorkspaces)
        XCTAssertEqual(defaults.consumeRunePendingLaunchAction(), .savedWorkspaces)
        XCTAssertNil(defaults.runePendingLaunchAction)
        XCTAssertNil(defaults.consumeRunePendingLaunchAction())

        defaults.runePendingLaunchAction = .recentContexts

        XCTAssertEqual(defaults.consumeRunePendingLaunchAction(), .recentContexts)
        XCTAssertNil(defaults.runePendingLaunchAction)

        defaults.setRunePendingLaunchRequest(
            RunePendingLaunchRequest(action: .savedWorkspaces, query: "  Workspace Alpha\nshared  ")
        )

        let request = defaults.consumeRunePendingLaunchRequest()
        XCTAssertEqual(request?.action, .savedWorkspaces)
        XCTAssertEqual(request?.query, "Workspace Alpha shared")
        XCTAssertNil(defaults.runePendingLaunchAction)
        XCTAssertNil(defaults.runePendingLaunchQuery)
    }

    @MainActor
    func testUpdatingResourceYAMLDraftClearsValidationIssues() {
        let state = RuneAppState()
        state.setResourceYAML("kind: Pod\n")
        state.setResourceYAMLValidationIssues([
            YAMLValidationIssue(
                source: .syntax,
                severity: .error,
                message: "Tabs are not allowed in YAML indentation."
            )
        ])
        state.beginResourceYAMLValidation()

        state.updateResourceYAMLDraft("kind: Pod\nmetadata:\n")

        XCTAssertTrue(state.resourceYAMLValidationIssues.isEmpty)
        XCTAssertFalse(state.isValidatingResourceYAML)
    }

    @MainActor
    func testRevertResourceYAMLToClusterSnapshotClearsValidationIssues() {
        let state = RuneAppState()
        state.setResourceYAML("kind: Pod\n")
        state.updateResourceYAMLDraft("kind:\tPod\n")
        state.setResourceYAMLValidationIssues([
            YAMLValidationIssue(
                source: .syntax,
                severity: .error,
                message: "Tabs are not allowed in YAML indentation."
            )
        ])
        state.beginResourceYAMLValidation()

        state.revertResourceYAMLToClusterSnapshot()

        XCTAssertEqual(state.resourceYAML, state.resourceYAMLBaseline)
        XCTAssertTrue(state.resourceYAMLValidationIssues.isEmpty)
        XCTAssertFalse(state.isValidatingResourceYAML)
    }
}
