import SwiftUI

struct CommandPaletteView: View {
    @ObservedObject var viewModel: RuneAppViewModel
    @State private var query: String = ""

    var body: some View {
        VStack(spacing: 12) {
            HStack {
                Image(systemName: "magnifyingglass")
                    .foregroundStyle(.secondary)
                TextField("Hoppa till section, context, namespace eller pod", text: $query)
                    .textFieldStyle(.plain)
            }
            .padding(10)
            .background(
                RoundedRectangle(cornerRadius: 10, style: .continuous)
                    .fill(.regularMaterial)
            )

            List(viewModel.commandPaletteItems(query: query)) { item in
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
                    .padding(.vertical, 2)
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
            }
            .scrollContentBackground(.hidden)
            .background(Color.clear)
        }
        .padding(16)
        .frame(minWidth: 680, minHeight: 440)
        .background(.thinMaterial)
    }
}
