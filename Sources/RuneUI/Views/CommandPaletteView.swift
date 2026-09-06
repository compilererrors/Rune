import AppKit
import SwiftUI

struct CommandPalettePrefixShortcut: Identifiable, Equatable, Sendable {
    let queryPrefix: String
    let title: String

    var id: String { queryPrefix }
}

enum CommandPalettePresentation {
    static let commonPrefixShortcuts = [
        CommandPalettePrefixShortcut(queryPrefix: ":po", title: "Pods"),
        CommandPalettePrefixShortcut(queryPrefix: ":deploy", title: "Deployments"),
        CommandPalettePrefixShortcut(queryPrefix: ":svc", title: "Services"),
        CommandPalettePrefixShortcut(queryPrefix: ":logs", title: "Open Logs"),
        CommandPalettePrefixShortcut(queryPrefix: ":so", title: "Save & Open"),
        CommandPalettePrefixShortcut(queryPrefix: ":ns", title: "Namespaces")
    ]

    static func prefillQuery(for prefix: String) -> String {
        let trimmed = prefix.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "" : "\(trimmed) "
    }

    static func prefillQuery(fromCheatSheetTitle title: String) -> String? {
        let trimmed = title.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.hasPrefix(":") else { return nil }
        let prefix = trimmed.prefix { character in
            !character.isWhitespace && character != "/"
        }
        guard prefix.count > 1 else { return nil }
        return prefillQuery(for: String(prefix))
    }
}

@MainActor
final class CommandPaletteActivationGate {
    private(set) var isExecuting = false

    @discardableResult
    func perform(
        isPalettePresented: () -> Bool,
        action: () -> Void
    ) -> Bool {
        guard !isExecuting else { return false }
        isExecuting = true
        action()

        // A rejected/no-op command leaves the palette open and must not lock it.
        // A successful command closes it synchronously; keep the gate closed until
        // this view disappears so queued Return events or a double-click cannot
        // execute the same command a second time.
        if isPalettePresented() {
            isExecuting = false
        }
        return true
    }

    func reset() {
        isExecuting = false
    }
}

struct CommandPaletteView: View {
    private enum FocusTarget: Hashable {
        case input
        case results
    }

    @ObservedObject var viewModel: RuneAppViewModel
    @State private var query: String = ""
    @State private var selectedItemID: String?
    @State private var localKeyMonitor: Any?
    @State private var isPrefixHelpPresented = false
    @State private var activationGate = CommandPaletteActivationGate()
    @FocusState private var focusedTarget: FocusTarget?
    @FocusState private var prefixHelpFocusedItemID: String?
    @Environment(\.runeThemePalette) private var runeThemePalette
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        let items = viewModel.commandPaletteItems(query: query)

        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogSectionSpacing) {
            HStack(alignment: .top, spacing: 12) {
                VStack(alignment: .leading, spacing: 6) {
                    Text("Command Palette")
                        .font(.title3.weight(.semibold))
                        .accessibilityAddTraits(.isHeader)
                    Text("Search by name, or type : to browse command prefixes.")
                        .font(.subheadline)
                        .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                }
                Spacer(minLength: 0)
                RuneDialogCloseButton("Close Command Palette") {
                    dismiss()
                }
            }

            HStack(spacing: 10) {
                Image(systemName: query.trimmingCharacters(in: .whitespacesAndNewlines).hasPrefix(":") ? "terminal" : "magnifyingglass")
                    .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                TextField("Search commands and resources", text: $query)
                    .textFieldStyle(.plain)
                    .accessibilityIdentifier("rune.command-palette.input")
                    .focused($focusedTarget, equals: .input)
                    .runeTextInputCursor()
                    .onMoveCommand { direction in
                        handleMoveCommand(direction: direction, items: items)
                    }
                    .onSubmit {
                        executePrimaryAction(items: items)
                    }
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 8)
            .frame(minHeight: 44)
            .background(
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous)
                    .fill(runeThemePalette?.inset.opacity(0.94) ?? Color(nsColor: .controlBackgroundColor).opacity(0.76))
            )
            .overlay(
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous)
                    .strokeBorder(runeThemePalette?.stroke.opacity(0.42) ?? Color(nsColor: .separatorColor).opacity(0.20), lineWidth: 1)
            )

            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 8) {
                    ForEach(CommandPalettePresentation.commonPrefixShortcuts) { shortcut in
                        prefixShortcut(shortcut)
                    }
                    fullPrefixHelpButton
                    keyboardHint()
                }
            }

            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 2) {
                        if items.isEmpty {
                            RuneContentStateView(
                                query.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                                    ? .empty(
                                        title: "No Commands Found",
                                        message: "Commands will appear when their features are available."
                                    )
                                    : .filteredEmpty(
                                        title: "No Commands Found",
                                        message: "Try another name or prefix."
                                    ),
                                variant: .centered,
                                action: query.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                                    ? nil
                                    : RuneContentStateAction("Clear Search", systemImage: "xmark.circle") {
                                        query = ""
                                        focusedTarget = .input
                                    }
                            )
                            .frame(maxWidth: .infinity, minHeight: 240)
                        } else {
                            ForEach(items) { item in
                                Button {
                                    selectedItemID = item.id
                                    activate {
                                        viewModel.executeCommandPaletteItem(item)
                                    }
                                } label: {
                                    HStack(spacing: 10) {
                                        Image(systemName: item.symbolName)
                                            .frame(width: 18)
                                            .foregroundStyle(runeThemePalette?.accent ?? Color.accentColor)
                                        VStack(alignment: .leading, spacing: 2) {
                                            Text(item.title)
                                                .font(.headline)
                                            Text(item.subtitle)
                                                .font(.subheadline)
                                                .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                                        }
                                        Spacer()
                                    }
                                    .padding(.horizontal, 8)
                                    .padding(.vertical, 6)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .background(
                                        RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                                            .fill(selectedItemID == item.id ? selectionFill : Color.clear)
                                    )
                                    .overlay(
                                        RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                                            .stroke(selectedItemID == item.id ? selectionStroke : Color.clear, lineWidth: 1)
                                    )
                                    .contentShape(Rectangle())
                                }
                                .buttonStyle(.plain)
                                .disabled(activationGate.isExecuting)
                                .id(item.id)
                            }
                        }
                    }
                    .padding(6)
                }
                .focusable(true)
                .focused($focusedTarget, equals: .results)
                .background(
                    RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous)
                        .fill(runeThemePalette?.panel.opacity(0.94) ?? Color(nsColor: .controlBackgroundColor).opacity(0.72))
                )
                .overlay(
                    RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous)
                        .strokeBorder(runeThemePalette?.stroke.opacity(0.36) ?? Color(nsColor: .separatorColor).opacity(0.18), lineWidth: 1)
                )
                .onChange(of: selectedItemID) { _, newID in
                    guard let newID else { return }
                    DispatchQueue.main.async {
                        withAnimation(.easeOut(duration: 0.12)) {
                            proxy.scrollTo(newID, anchor: .center)
                        }
                    }
                }
            }

            keyboardActionBridge(items: items)
        }
        .padding(RuneUILayoutMetrics.dialogContentPadding)
        .frame(
            minWidth: RuneUILayoutMetrics.commandPaletteMinWidth,
            idealWidth: RuneUILayoutMetrics.commandPaletteIdealWidth,
            maxWidth: RuneUILayoutMetrics.commandPaletteMaxWidth,
            minHeight: RuneUILayoutMetrics.commandPaletteMinHeight,
            idealHeight: RuneUILayoutMetrics.commandPaletteIdealHeight,
            maxHeight: RuneUILayoutMetrics.commandPaletteMaxHeight
        )
        .background(runeThemePalette?.content.opacity(0.98) ?? Color(nsColor: .windowBackgroundColor).opacity(0.82))
        .runePointerCursor()
        .onAppear {
            activationGate.reset()
            let prefill = viewModel.consumeCommandPalettePrefillQuery()
            if !prefill.isEmpty {
                query = prefill
            }
            focusedTarget = .input
            selectedItemID = viewModel.commandPaletteItems(query: query).first?.id
            installLocalKeyMonitor()
        }
        .onDisappear {
            removeLocalKeyMonitor()
        }
        .onChange(of: query) { _, _ in
            let refreshedItems = viewModel.commandPaletteItems(query: query)
            selectedItemID = refreshedItems.first?.id
        }
        .onChange(of: items.map(\.id)) { _, newIDs in
            if selectedItemID == nil || !newIDs.contains(selectedItemID ?? "") {
                selectedItemID = newIDs.first
            }
        }
        .onMoveCommand { direction in
            handleMoveCommand(direction: direction, items: items)
        }
    }

    private func prefixShortcut(_ shortcut: CommandPalettePrefixShortcut) -> some View {
        Button {
            applyPrefix(shortcut.queryPrefix)
        } label: {
            HStack(spacing: 5) {
                Text(shortcut.queryPrefix)
                    .font(.footnote.weight(.bold).monospaced())
                Text(shortcut.title)
                    .font(.footnote.weight(.medium))
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(runeThemePalette?.chipFill ?? Color.secondary.opacity(0.12), in: Capsule())
            .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
            .contentShape(Capsule())
        }
        .buttonStyle(.plain)
        .help("Use \(shortcut.queryPrefix) to find \(shortcut.title.lowercased())")
        .accessibilityLabel("Use \(shortcut.queryPrefix) for \(shortcut.title)")
    }

    private var fullPrefixHelpButton: some View {
        Button {
            if !isPrefixHelpPresented {
                focusedTarget = nil
            }
            isPrefixHelpPresented.toggle()
        } label: {
            Label("All Prefixes", systemImage: "questionmark.circle")
                .runeMinimumInteractiveTarget()
        }
        .buttonStyle(RuneToolbarButtonStyle())
        .controlSize(.small)
        .help("Browse all command prefixes")
        .popover(isPresented: $isPrefixHelpPresented, arrowEdge: .bottom) {
            fullPrefixHelp
        }
    }

    private var fullPrefixHelp: some View {
        let prefixItems = viewModel.commandPaletteItems(query: ":")

        return VStack(alignment: .leading, spacing: 10) {
            Text("Command Prefixes")
                .font(.headline)
                .accessibilityAddTraits(.isHeader)

            ScrollView {
                LazyVStack(alignment: .leading, spacing: 2) {
                    ForEach(prefixItems) { item in
                        Button {
                            guard let prefill = CommandPalettePresentation.prefillQuery(
                                fromCheatSheetTitle: item.title
                            ) else { return }
                            query = prefill
                            focusedTarget = .input
                            isPrefixHelpPresented = false
                        } label: {
                            HStack(spacing: 10) {
                                Image(systemName: item.symbolName)
                                    .frame(width: 18)
                                    .foregroundStyle(runeThemePalette?.accent ?? Color.accentColor)
                                VStack(alignment: .leading, spacing: 2) {
                                    Text(item.title)
                                        .font(.subheadline.weight(.semibold))
                                    Text(item.subtitle)
                                        .font(.caption)
                                        .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
                                }
                                Spacer(minLength: 0)
                            }
                            .padding(.horizontal, 8)
                            .padding(.vertical, 6)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .contentShape(Rectangle())
                        }
                        .buttonStyle(.plain)
                        .focused($prefixHelpFocusedItemID, equals: item.id)
                    }
                }
            }
            .frame(minHeight: 220, idealHeight: 320, maxHeight: 420)
        }
        .padding(14)
        .frame(minWidth: 320, idealWidth: 380, maxWidth: 440)
        .runePointerCursor()
        .onAppear {
            focusedTarget = nil
            DispatchQueue.main.async {
                prefixHelpFocusedItemID = prefixItems.first?.id
            }
        }
        .onDisappear {
            prefixHelpFocusedItemID = nil
            guard viewModel.state.isCommandPalettePresented else { return }
            DispatchQueue.main.async {
                focusedTarget = .input
            }
        }
    }

    private func applyPrefix(_ prefix: String) {
        query = CommandPalettePresentation.prefillQuery(for: prefix)
        focusedTarget = .input
    }

    private func keyboardHint() -> some View {
        HStack(spacing: 6) {
            Text("Tab")
                .font(.caption2.weight(.bold))
                .padding(.horizontal, 7)
                .padding(.vertical, 4)
                .background(runeThemePalette?.chipFill ?? Color.secondary.opacity(0.12), in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous))
            Text("focus results, arrows select, Enter runs")
                .font(.footnote.weight(.medium))
                .foregroundStyle(runeThemePalette?.secondaryText ?? Color.secondary)
        }
        .padding(.horizontal, 4)
    }

    private var selectionFill: Color {
        runeThemePalette?.selectionFill ?? Color.accentColor.opacity(0.22)
    }

    private var selectionStroke: Color {
        runeThemePalette?.selectionStroke ?? Color.accentColor.opacity(0.45)
    }

    private func executePrimaryAction(items: [CommandPaletteItem]) {
        if focusedTarget == .input {
            if let selectedItemID,
               let selectedItem = items.first(where: { $0.id == selectedItemID }) {
                activate {
                    viewModel.executeCommandPaletteItem(selectedItem)
                }
                return
            }
            activate {
                viewModel.executeCommandPaletteQuery(query)
            }
            return
        }

        if let selectedItemID,
           let selectedItem = items.first(where: { $0.id == selectedItemID }) {
            activate {
                viewModel.executeCommandPaletteItem(selectedItem)
            }
            return
        }

        activate {
            viewModel.executeCommandPaletteQuery(query)
        }
    }

    private func activate(_ action: () -> Void) {
        activationGate.perform(
            isPalettePresented: { viewModel.state.isCommandPalettePresented },
            action: action
        )
    }

    @ViewBuilder
    private func keyboardActionBridge(items: [CommandPaletteItem]) -> some View {
        if !isPrefixHelpPresented {
            VStack(spacing: 0) {
                Button("") {
                    focusResults(items: items)
                }
                .keyboardShortcut(.tab, modifiers: [])

                Button("") {
                    focusedTarget = .input
                }
                .keyboardShortcut(.tab, modifiers: [.shift])

                Button("") {
                    guard focusedTarget == .results else { return }
                    executePrimaryAction(items: items)
                }
                .keyboardShortcut(.return, modifiers: [])
            }
            .frame(width: 0, height: 0)
            .opacity(0)
            .accessibilityHidden(true)
        }
    }

    private func focusResults(items: [CommandPaletteItem]) {
        focusedTarget = .results
        if selectedItemID == nil || !items.contains(where: { $0.id == selectedItemID }) {
            selectedItemID = items.first?.id
        }
    }

    private func moveSelection(direction: MoveCommandDirection, items: [CommandPaletteItem]) {
        guard !items.isEmpty else {
            selectedItemID = nil
            return
        }

        guard let currentID = selectedItemID,
              let currentIndex = items.firstIndex(where: { $0.id == currentID }) else {
            selectedItemID = items.first?.id
            return
        }

        let nextIndex: Int
        switch direction {
        case .down, .right:
            nextIndex = min(currentIndex + 1, items.count - 1)
        case .up, .left:
            nextIndex = max(currentIndex - 1, 0)
        @unknown default:
            nextIndex = currentIndex
        }

        selectedItemID = items[nextIndex].id
    }

    private func handleMoveCommand(direction: MoveCommandDirection, items: [CommandPaletteItem]) {
        guard !isPrefixHelpPresented else { return }
        switch direction {
        case .down, .up:
            if focusedTarget == .input {
                focusResults(items: items)
                return
            }
            if focusedTarget == .results {
                moveSelection(direction: direction, items: items)
                return
            }
        default:
            return
        }
    }

    private func installLocalKeyMonitor() {
        removeLocalKeyMonitor()
        localKeyMonitor = NSEvent.addLocalMonitorForEvents(matching: .keyDown) { event in
            let items = viewModel.commandPaletteItems(query: query)
            if handleLocalKeyEvent(event, items: items) {
                return nil
            }
            return event
        }
    }

    private func removeLocalKeyMonitor() {
        guard let localKeyMonitor else { return }
        NSEvent.removeMonitor(localKeyMonitor)
        self.localKeyMonitor = nil
    }

    private func handleLocalKeyEvent(_ event: NSEvent, items: [CommandPaletteItem]) -> Bool {
        guard !isPrefixHelpPresented else { return false }
        guard !activationGate.isExecuting else {
            return event.keyCode == 36 || event.keyCode == 76
        }
        let flags = event.modifierFlags.intersection(.deviceIndependentFlagsMask)
        let hasBlockingModifiers = flags.contains(.command) || flags.contains(.option) || flags.contains(.control)
        if hasBlockingModifiers {
            return false
        }

        switch event.keyCode {
        case 125: // down arrow
            handleMoveCommand(direction: .down, items: items)
            return focusedTarget == .input || focusedTarget == .results
        case 126: // up arrow
            handleMoveCommand(direction: .up, items: items)
            return focusedTarget == .input || focusedTarget == .results
        case 36, 76: // return / keypad enter
            if focusedTarget == .input || focusedTarget == .results {
                executePrimaryAction(items: items)
                return true
            }
            return false
        default:
            return false
        }
    }
}
