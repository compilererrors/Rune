import AppKit
import SwiftUI

struct CommandPaletteView: View {
    private enum FocusTarget: Hashable {
        case input
        case results
    }

    @ObservedObject var viewModel: RuneAppViewModel
    @State private var query: String = ""
    @State private var selectedItemID: String?
    @State private var localKeyMonitor: Any?
    @FocusState private var focusedTarget: FocusTarget?

    var body: some View {
        let items = viewModel.commandPaletteItems(query: query)

        VStack(alignment: .leading, spacing: 14) {
            VStack(alignment: .leading, spacing: 6) {
                Text("Command Palette")
                    .font(.title3.weight(.semibold))
                Text("Search or use a prefix: `:po`, `:deploy`, `:svc` / `:service`, `:no`, `:sts`, `:ing`, `:cm`, `:ctx`, `:ns`, `:ov`, `:rbac`, `:cr`, `:helm`, `:cj` …")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }

            HStack(spacing: 10) {
                Image(systemName: query.trimmingCharacters(in: .whitespacesAndNewlines).hasPrefix(":") ? "terminal" : "magnifyingglass")
                    .foregroundStyle(.secondary)
                TextField("Search or type e.g. :po api, :service billing, :no node1, :ns kube-system, :cj", text: $query)
                    .textFieldStyle(.plain)
                    .focused($focusedTarget, equals: .input)
                    .onMoveCommand { direction in
                        handleMoveCommand(direction: direction, items: items)
                    }
                    .onSubmit {
                        executePrimaryAction(items: items)
                    }
            }
            .padding(.horizontal, 12)
            .frame(height: 44)
            .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 12, style: .continuous))

            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 8) {
                    paletteHint(":po")
                    paletteHint(":deploy")
                    paletteHint(":sts")
                    paletteHint(":svc")
                    paletteHint(":ing")
                    paletteHint(":cm")
                    paletteHint(":ctx")
                    paletteHint(":ns")
                    paletteHint(":ov")
                    paletteHint(":rbac")
                    paletteHint(":helm")
                    paletteHint(":reload")
                    keyboardHint()
                }
            }

            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 2) {
                        ForEach(items) { item in
                            Button {
                                selectedItemID = item.id
                                viewModel.executeCommandPaletteItem(item)
                            } label: {
                                HStack(spacing: 10) {
                                    Image(systemName: item.symbolName)
                                        .frame(width: 18)
                                        .foregroundStyle(Color.accentColor)
                                    VStack(alignment: .leading, spacing: 2) {
                                        Text(item.title)
                                            .font(.headline)
                                        Text(item.subtitle)
                                            .font(.subheadline)
                                            .foregroundStyle(.secondary)
                                    }
                                    Spacer()
                                }
                                .padding(.horizontal, 8)
                                .padding(.vertical, 6)
                                .frame(maxWidth: .infinity, alignment: .leading)
                                .background(
                                    RoundedRectangle(cornerRadius: 8, style: .continuous)
                                        .fill(selectedItemID == item.id ? Color.accentColor.opacity(0.22) : Color.clear)
                                )
                                .overlay(
                                    RoundedRectangle(cornerRadius: 8, style: .continuous)
                                        .stroke(selectedItemID == item.id ? Color.accentColor.opacity(0.45) : Color.clear, lineWidth: 1)
                                )
                                .contentShape(Rectangle())
                            }
                            .buttonStyle(.plain)
                            .id(item.id)
                        }
                    }
                    .padding(6)
                }
                .focusable(true)
                .focused($focusedTarget, equals: .results)
                .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 10, style: .continuous))
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
        .padding(16)
        .frame(minWidth: 680, minHeight: 440)
        .background(.thinMaterial)
        .onAppear {
            focusedTarget = .input
            selectedItemID = items.first?.id
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

    private func paletteHint(_ command: String) -> some View {
        Text(command)
            .font(.footnote.weight(.semibold))
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(.regularMaterial, in: Capsule())
            .foregroundStyle(.secondary)
    }

    private func keyboardHint() -> some View {
        HStack(spacing: 6) {
            Text("Tab")
                .font(.caption2.weight(.bold))
                .padding(.horizontal, 7)
                .padding(.vertical, 4)
                .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 6, style: .continuous))
            Text("focus results, arrows select, Enter runs")
                .font(.footnote.weight(.medium))
                .foregroundStyle(.secondary)
        }
        .padding(.horizontal, 4)
    }

    private func executePrimaryAction(items: [CommandPaletteItem]) {
        if focusedTarget == .input {
            if let selectedItemID,
               let selectedItem = items.first(where: { $0.id == selectedItemID }) {
                viewModel.executeCommandPaletteItem(selectedItem)
                return
            }
            viewModel.executeCommandPaletteQuery(query)
            return
        }

        if let selectedItemID,
           let selectedItem = items.first(where: { $0.id == selectedItemID }) {
            viewModel.executeCommandPaletteItem(selectedItem)
            return
        }

        viewModel.executeCommandPaletteQuery(query)
    }

    @ViewBuilder
    private func keyboardActionBridge(items: [CommandPaletteItem]) -> some View {
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
