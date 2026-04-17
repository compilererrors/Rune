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
                Text("Sök eller k9s-stil: `:po`, `:deploy`, `:sts`, `:svc`, `:ing`, `:cm`, `:sec`, `:ctx`, `:ns`, `:rbac`, `:cr`, `:helm` …")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            }

            HStack(spacing: 10) {
                Image(systemName: query.trimmingCharacters(in: .whitespacesAndNewlines).hasPrefix(":") ? "terminal" : "magnifyingglass")
                    .foregroundStyle(.secondary)
                TextField("Sök eller skriv t.ex. :po api, :svc billing, :ns kube-system, :rbac", text: $query)
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
            Text("focus results")
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
}
