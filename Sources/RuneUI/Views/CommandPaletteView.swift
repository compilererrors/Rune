import SwiftUI

struct CommandPaletteView: View {
    private enum FocusTarget: Hashable {
        case input
        case results
    }

    @ObservedObject var viewModel: RuneAppViewModel
    @State private var query: String = ""
    @State private var selectedItemID: String?
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

            List(items, selection: $selectedItemID) { item in
                Button {
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
                    .padding(.vertical, 4)
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .tag(item.id)
            }
            .focusable(true)
            .focused($focusedTarget, equals: .results)
            .scrollContentBackground(.hidden)
            .background(Color.clear)
            .onMoveCommand { direction in
                guard focusedTarget == .results else { return }
                moveSelection(direction: direction, items: items)
            }

            keyboardActionBridge(items: items)
        }
        .padding(16)
        .frame(minWidth: 680, minHeight: 440)
        .background(.thinMaterial)
        .onAppear {
            focusedTarget = .input
            selectedItemID = items.first?.id
        }
        .onChange(of: query) { _, _ in
            selectedItemID = items.first?.id
        }
        .onChange(of: items.map(\.id)) { _, newIDs in
            if selectedItemID == nil || !newIDs.contains(selectedItemID ?? "") {
                selectedItemID = newIDs.first
            }
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
        if focusedTarget == .results,
           let selectedItemID,
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
}
