import AppKit
import RuneCore
import RuneDiagnostics
import RuneStore
import SwiftUI

private struct SettingsHelpButton: View {
    let text: String
    @State private var isPopoverPresented = false

    var body: some View {
        Button {
            isPopoverPresented.toggle()
        } label: {
            Image(systemName: "questionmark.circle")
                .font(.caption)
                .foregroundStyle(.secondary)
                .padding(2)
                .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .help(text)
        .popover(isPresented: $isPopoverPresented, arrowEdge: .top) {
            Text(text)
                .font(.footnote)
                .foregroundStyle(.primary)
                .frame(width: 280, alignment: .leading)
                .padding(10)
        }
        .accessibilityLabel("More info")
        .accessibilityHint(text)
    }
}

/// Settings window content.
public struct RunePreferencesView: View {
    private enum PreferencesPane: String, CaseIterable, Identifiable {
        case general
        case keyBindings
        case logs
        case diagnostics
        case performance

        var id: String { rawValue }

        var title: String {
            switch self {
            case .general: return "General"
            case .keyBindings: return "Key Bindings"
            case .logs: return "Logs"
            case .diagnostics: return "Diagnostics"
            case .performance: return "Performance"
            }
        }

        var symbol: String {
            switch self {
            case .general: return "gearshape"
            case .keyBindings: return "keyboard"
            case .logs: return "text.alignleft"
            case .diagnostics: return "stethoscope"
            case .performance: return "speedometer"
            }
        }
    }

    @State private var selectedPane: PreferencesPane = .general
    @AppStorage(RuneSettingsKeys.persistNamespaceListCache) private var persistNamespaceListCache = true
    @AppStorage(RuneSettingsKeys.diagnosticsLogging) private var diagnosticsLogging = true
    @AppStorage(RuneSettingsKeys.verboseDebugTrace) private var verboseDebugTrace = false
    @AppStorage(RuneSettingsKeys.backgroundPrefetchOtherContexts) private var backgroundPrefetchOtherContexts = false
    @AppStorage(RuneSettingsKeys.logsCustomPresetOneMode) private var customOneModeRaw = RuneCustomLogPresetMode.lines.rawValue
    @AppStorage(RuneSettingsKeys.logsCustomPresetOneLines) private var customOneLinesRaw = "5000"
    @AppStorage(RuneSettingsKeys.logsCustomPresetOneTimeValue) private var customOneTimeValueRaw = "15"
    @AppStorage(RuneSettingsKeys.logsCustomPresetOneTimeUnit) private var customOneTimeUnitRaw = RuneCustomLogPresetTimeUnit.minutes.rawValue
    @AppStorage(RuneSettingsKeys.logsCustomPresetTwoMode) private var customTwoModeRaw = RuneCustomLogPresetMode.time.rawValue
    @AppStorage(RuneSettingsKeys.logsCustomPresetTwoLines) private var customTwoLinesRaw = "99999"
    @AppStorage(RuneSettingsKeys.logsCustomPresetTwoTimeValue) private var customTwoTimeValueRaw = "6"
    @AppStorage(RuneSettingsKeys.logsCustomPresetTwoTimeUnit) private var customTwoTimeUnitRaw = RuneCustomLogPresetTimeUnit.hours.rawValue
    @State private var cacheClearStatus: String?
    @State private var keyBindingShortcuts = Self.loadKeyBindingShortcuts()

    public init() {}

    public var body: some View {
        TabView(selection: $selectedPane) {
            generalSettingsForm
                .tag(PreferencesPane.general)
                .tabItem {
                    Label(PreferencesPane.general.title, systemImage: PreferencesPane.general.symbol)
                }

            keyBindingsSettingsForm
                .tag(PreferencesPane.keyBindings)
                .tabItem {
                    Label(PreferencesPane.keyBindings.title, systemImage: PreferencesPane.keyBindings.symbol)
                }

            logsSettingsForm
                .tag(PreferencesPane.logs)
                .tabItem {
                    Label(PreferencesPane.logs.title, systemImage: PreferencesPane.logs.symbol)
                }

            diagnosticsSettingsForm
                .tag(PreferencesPane.diagnostics)
                .tabItem {
                    Label(PreferencesPane.diagnostics.title, systemImage: PreferencesPane.diagnostics.symbol)
                }

            performanceSettingsForm
                .tag(PreferencesPane.performance)
                .tabItem {
                    Label(PreferencesPane.performance.title, systemImage: PreferencesPane.performance.symbol)
                }
        }
        .controlSize(.small)
        .frame(minWidth: 700, idealWidth: 780, minHeight: 520)
    }

    private var generalSettingsForm: some View {
        settingsPane(
            title: "General",
            subtitle: "Runtime behavior and local data."
        ) {
            settingsSection("Cache") {
                settingsToggleRow(
                    "Persist namespace list cache",
                    help: "Saves namespace names per context under Application Support/Rune/namespace-lists and restores them at startup while a fresh list loads. Does not persist logs or full resource payloads.",
                    isOn: $persistNamespaceListCache
                )

                Divider()

                VStack(alignment: .leading, spacing: 10) {
                    Text("Maintenance")
                        .font(.subheadline.weight(.semibold))
                    HStack(spacing: 10) {
                        Button("Clear cached cluster data", role: .destructive) {
                            clearDiskCaches()
                        }
                        .buttonStyle(.bordered)

                        if let cacheClearStatus {
                            Text(cacheClearStatus)
                                .font(.footnote)
                                .foregroundStyle(.secondary)
                                .fixedSize(horizontal: false, vertical: true)
                        }
                    }
                }
            }
        }
    }

    private var keyBindingsSettingsForm: some View {
        settingsPane(
            title: "Key Bindings",
            subtitle: "k9s-inspired action keys for the selected resource or release."
        ) {
            settingsSection("Defaults") {
                Text("These defaults mirror common k9s mnemonics where Rune has an equivalent action. `d`, `l`, `s`, and `Shift-F` follow k9s conventions; YAML and history actions are Rune mappings built around the same workflow.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)

                Button("Reset to k9s-style defaults") {
                    resetKeyBindingShortcuts()
                }
                .buttonStyle(.bordered)
            }

            settingsSection("Actions") {
                ForEach(RuneKeyBindingAction.allCases) { action in
                    VStack(alignment: .leading, spacing: 8) {
                        HStack(alignment: .top, spacing: 12) {
                            VStack(alignment: .leading, spacing: 3) {
                                Text(action.title)
                                    .font(.subheadline.weight(.semibold))
                                Text(action.detail)
                                    .font(.footnote)
                                    .foregroundStyle(.secondary)
                                    .fixedSize(horizontal: false, vertical: true)
                            }

                            Spacer(minLength: 12)

                            Toggle("⌘", isOn: shortcutCommandBinding(for: action))
                                .toggleStyle(.button)
                                .controlSize(.small)
                                .help("Require Command for this action")

                            Toggle("⌥", isOn: shortcutOptionBinding(for: action))
                                .toggleStyle(.button)
                                .controlSize(.small)
                                .help("Require Option for this action")

                            Toggle("⇧", isOn: shortcutShiftBinding(for: action))
                                .toggleStyle(.button)
                                .controlSize(.small)
                                .help("Require Shift for this action")

                            Picker("Key", selection: shortcutKeyBinding(for: action)) {
                                ForEach(Self.availableShortcutKeys, id: \.self) { key in
                                    Text(Self.displayShortcutKey(key)).tag(key)
                                }
                            }
                            .frame(width: 96)
                        }

                        HStack(spacing: 8) {
                            Text("Current: \(shortcut(for: action).displayValue)")
                                .font(.footnote)
                                .foregroundStyle(.secondary)

                            if let conflict = conflictingAction(for: action) {
                                Text("Conflicts with \(conflict.title)")
                                    .font(.footnote)
                                    .foregroundStyle(.red)
                            }
                        }
                    }

                    if action != RuneKeyBindingAction.allCases.last {
                        Divider()
                    }
                }
            }
        }
    }

    private var logsSettingsForm: some View {
        settingsPane(
            title: "Logs",
            subtitle: "Custom presets for log windows and dropdown defaults."
        ) {
            settingsSection("Custom log windows") {
                Text("Configure two custom presets shown in log dropdowns.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            customLogPresetSection(
                slot: .one,
                modeRaw: $customOneModeRaw,
                linesRaw: $customOneLinesRaw,
                timeValueRaw: $customOneTimeValueRaw,
                timeUnitRaw: $customOneTimeUnitRaw
            )

            customLogPresetSection(
                slot: .two,
                modeRaw: $customTwoModeRaw,
                linesRaw: $customTwoLinesRaw,
                timeValueRaw: $customTwoTimeValueRaw,
                timeUnitRaw: $customTwoTimeUnitRaw
            )
        }
    }

    private var diagnosticsSettingsForm: some View {
        settingsPane(
            title: "Diagnostics",
            subtitle: "Logging and debug trace controls."
        ) {
            settingsSection("Diagnostics logging") {
                settingsToggleRow(
                    "Diagnostics logging",
                    help: "Writes diagnostics to system log and command runner log categories.",
                    isOn: $diagnosticsLogging
                )
            }

            settingsSection("Verbose trace") {
                settingsToggleRow(
                    "Verbose debug trace (file)",
                    help: "Appends detailed Kubernetes load traces to Application Support/Rune/Logs/debug-trace.log.",
                    isOn: $verboseDebugTrace
                )

                Divider()

                VStack(alignment: .leading, spacing: 8) {
                    Text("Log file path")
                        .font(.subheadline.weight(.semibold))
                    Text(DebugTraceWriter.logFileURL.path)
                        .font(.caption.monospaced())
                        .foregroundStyle(.secondary)
                        .textSelection(.enabled)

                    HStack(spacing: 10) {
                        Button("Reveal debug trace in Finder") {
                            revealDebugTraceLogInFinder()
                        }
                        .buttonStyle(.bordered)

                        Button("Clear debug trace log", role: .destructive) {
                            DebugTraceWriter.clear()
                        }
                        .buttonStyle(.bordered)
                    }
                }
            }
        }
    }

    private var performanceSettingsForm: some View {
        settingsPane(
            title: "Performance",
            subtitle: "Background loading and responsiveness tradeoffs."
        ) {
            settingsSection("Background prefetch") {
                settingsToggleRow(
                    "Background prefetch other contexts",
                    help: "Allows bounded background warming of overview cache for non-selected contexts.",
                    isOn: $backgroundPrefetchOtherContexts
                )
            }
        }
    }

    @ViewBuilder
    private func customLogPresetSection(
        slot: RuneCustomLogPresetSlot,
        modeRaw: Binding<String>,
        linesRaw: Binding<String>,
        timeValueRaw: Binding<String>,
        timeUnitRaw: Binding<String>
    ) -> some View {
        let mode = enumBinding(modeRaw, default: RuneCustomLogPresetMode.lines)
        let unit = enumBinding(timeUnitRaw, default: RuneCustomLogPresetTimeUnit.minutes)
        let lines = digitsOnlyBinding(linesRaw)
        let timeValue = digitsOnlyBinding(timeValueRaw)

        settingsSection("Custom \(slot.ordinal)") {
            Picker("Type", selection: mode) {
                ForEach(RuneCustomLogPresetMode.allCases, id: \.rawValue) { mode in
                    Text(mode.title).tag(mode)
                }
            }
            .pickerStyle(.segmented)
            .frame(maxWidth: 260)

            if mode.wrappedValue == .lines {
                LabeledContent("Lines") {
                    TextField("5000", text: lines)
                        .textFieldStyle(.roundedBorder)
                        .frame(maxWidth: 140)
                }
                Text("Digits only. `99999` = since beginning.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            } else {
                LabeledContent("Window") {
                    HStack(spacing: 8) {
                        TextField("15", text: timeValue)
                            .textFieldStyle(.roundedBorder)
                            .frame(maxWidth: 80)
                        Picker("Unit", selection: unit) {
                            ForEach(RuneCustomLogPresetTimeUnit.allCases, id: \.rawValue) { unit in
                                Text(unit.title).tag(unit)
                            }
                        }
                        .labelsHidden()
                        .pickerStyle(.menu)
                    }
                }
                Text("Example: 15 minutes, 6 hours, 2 days.")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            Text("Dropdown preview: \(UserDefaults.standard.runeCustomLogPresetConfig(slot: slot).title(slot: slot))")
                .font(.footnote)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            helpInline("Shown in all log-window dropdowns.")
        }
    }

    @ViewBuilder
    private func settingsPane<Content: View>(
        title: String,
        subtitle: String,
        @ViewBuilder content: () -> Content
    ) -> some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 14) {
                VStack(alignment: .leading, spacing: 4) {
                    Text(title)
                        .font(.title3.weight(.semibold))
                    Text(subtitle)
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                }

                content()
            }
            .frame(maxWidth: 760, alignment: .leading)
            .padding(.horizontal, 20)
            .padding(.vertical, 16)
        }
    }

    @ViewBuilder
    private func settingsSection<Content: View>(
        _ title: String,
        @ViewBuilder content: () -> Content
    ) -> some View {
        VStack(alignment: .leading, spacing: 7) {
            Text(title)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.secondary)

            VStack(alignment: .leading, spacing: 10) {
                content()
            }
            .runeInsetCard(padding: 12)
        }
    }

    @ViewBuilder
    private func settingsToggleRow(_ title: String, help: String, isOn: Binding<Bool>) -> some View {
        Toggle(isOn: isOn) {
            settingLabel(title, help: help)
        }
        .toggleStyle(.switch)
        .frame(minHeight: 24)
    }

    @ViewBuilder
    private func helpInline(_ text: String) -> some View {
        HStack(spacing: 6) {
            Image(systemName: "questionmark.circle")
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(text)
                .font(.footnote)
                .foregroundStyle(.tertiary)
                .fixedSize(horizontal: false, vertical: true)
        }
    }

    @ViewBuilder
    private func settingLabel(_ title: String, help: String) -> some View {
        HStack(spacing: 6) {
            Text(title)
            helpIcon(help)
        }
    }

    @ViewBuilder
    private func helpIcon(_ text: String) -> some View {
        SettingsHelpButton(text: text)
    }

    private func enumBinding<T: RawRepresentable>(
        _ rawBinding: Binding<String>,
        default defaultValue: T
    ) -> Binding<T> where T.RawValue == String {
        Binding<T>(
            get: {
                T(rawValue: rawBinding.wrappedValue) ?? defaultValue
            },
            set: { newValue in
                rawBinding.wrappedValue = newValue.rawValue
            }
        )
    }

    private func digitsOnlyBinding(_ rawBinding: Binding<String>) -> Binding<String> {
        Binding<String>(
            get: { rawBinding.wrappedValue },
            set: { newValue in
                rawBinding.wrappedValue = newValue.filter(\.isNumber)
            }
        )
    }

    private func revealDebugTraceLogInFinder() {
        let url = DebugTraceWriter.logFileURL
        let parent = url.deletingLastPathComponent()
        if FileManager.default.fileExists(atPath: url.path) {
            NSWorkspace.shared.activateFileViewerSelecting([url])
        } else if FileManager.default.fileExists(atPath: parent.path) {
            NSWorkspace.shared.open(parent)
        } else {
            try? FileManager.default.createDirectory(at: parent, withIntermediateDirectories: true)
            NSWorkspace.shared.open(parent)
        }
    }

    private func clearDiskCaches() {
        let result = RuneCacheMaintenance.clearDiskCaches()
        NotificationCenter.default.post(name: .runeCachesDidClear, object: nil)
        if result.failedCount == 0 {
            cacheClearStatus = "Cleared \(result.removedCount) cache path\(result.removedCount == 1 ? "" : "s"). Reload view data to repopulate."
        } else {
            cacheClearStatus = "Cleared \(result.removedCount), failed \(result.failedCount). Close and reopen Rune if paths stay locked."
        }
    }

    private func shortcut(for action: RuneKeyBindingAction) -> RuneKeyboardShortcut {
        keyBindingShortcuts[action] ?? action.defaultShortcut
    }

    private func shortcutKeyBinding(for action: RuneKeyBindingAction) -> Binding<String> {
        Binding(
            get: { shortcut(for: action).key },
            set: { newValue in
                updateShortcut(for: action) { current in
                    RuneKeyboardShortcut(
                        key: newValue,
                        requiresShift: current.requiresShift,
                        requiresCommand: current.requiresCommand,
                        requiresOption: current.requiresOption
                    ) ?? current
                }
            }
        )
    }

    private func shortcutCommandBinding(for action: RuneKeyBindingAction) -> Binding<Bool> {
        Binding(
            get: { shortcut(for: action).requiresCommand },
            set: { newValue in
                updateShortcut(for: action) { current in
                    RuneKeyboardShortcut(
                        key: current.key,
                        requiresShift: current.requiresShift,
                        requiresCommand: newValue,
                        requiresOption: current.requiresOption
                    ) ?? current
                }
            }
        )
    }

    private func shortcutOptionBinding(for action: RuneKeyBindingAction) -> Binding<Bool> {
        Binding(
            get: { shortcut(for: action).requiresOption },
            set: { newValue in
                updateShortcut(for: action) { current in
                    RuneKeyboardShortcut(
                        key: current.key,
                        requiresShift: current.requiresShift,
                        requiresCommand: current.requiresCommand,
                        requiresOption: newValue
                    ) ?? current
                }
            }
        )
    }

    private func shortcutShiftBinding(for action: RuneKeyBindingAction) -> Binding<Bool> {
        Binding(
            get: { shortcut(for: action).requiresShift },
            set: { newValue in
                updateShortcut(for: action) { current in
                    RuneKeyboardShortcut(
                        key: current.key,
                        requiresShift: newValue,
                        requiresCommand: current.requiresCommand,
                        requiresOption: current.requiresOption
                    ) ?? current
                }
            }
        )
    }

    private func updateShortcut(
        for action: RuneKeyBindingAction,
        transform: (RuneKeyboardShortcut) -> RuneKeyboardShortcut
    ) {
        let updated = transform(shortcut(for: action))
        keyBindingShortcuts[action] = updated
        UserDefaults.standard.setRuneKeyBindingShortcut(updated, for: action)
    }

    private func conflictingAction(for action: RuneKeyBindingAction) -> RuneKeyBindingAction? {
        let currentShortcut = shortcut(for: action)
        return RuneKeyBindingAction.allCases.first {
            $0 != action && shortcut(for: $0) == currentShortcut
        }
    }

    private func resetKeyBindingShortcuts() {
        UserDefaults.standard.resetRuneKeyBindingShortcuts()
        keyBindingShortcuts = Self.loadKeyBindingShortcuts()
    }

    private static var availableShortcutKeys: [String] {
        Array("abcdefghijklmnopqrstuvwxyz0123456789").map(String.init) + ["[", "]", "left", "right"]
    }

    private static func displayShortcutKey(_ key: String) -> String {
        key.rangeOfCharacter(from: .alphanumerics) != nil ? key.uppercased() : key
    }

    private static func loadKeyBindingShortcuts() -> [RuneKeyBindingAction: RuneKeyboardShortcut] {
        Dictionary(uniqueKeysWithValues: RuneKeyBindingAction.allCases.map {
            ($0, UserDefaults.standard.runeKeyBindingShortcut(for: $0))
        })
    }
}
