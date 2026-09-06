import SwiftUI

/// The menu and table headers share the same sort state. Choosing the checked
/// column leaves its direction alone; direction is an explicit menu choice.
struct RuneResourceSortMenu<Column: Hashable>: View {
    let options: [(Column, String)]
    let column: Column
    let ascending: Bool
    var ascendingTitle = "Ascending"
    var descendingTitle = "Descending"
    let onSelectColumn: (Column) -> Void
    let onReverseDirection: () -> Void

    var body: some View {
        RuneToolbarMenu {
            Picker("Sort By", selection: Binding(
                get: { column },
                set: { if $0 != column { onSelectColumn($0) } }
            )) {
                ForEach(options, id: \.0) { option in
                    Text(option.1).tag(option.0)
                }
            }
            .pickerStyle(.inline)

            Divider()

            Picker("Order", selection: Binding(
                get: { ascending },
                set: { if $0 != ascending { onReverseDirection() } }
            )) {
                Text(ascendingTitle).tag(true)
                Text(descendingTitle).tag(false)
            }
            .pickerStyle(.inline)
        } label: {
            Label("Sort", systemImage: "arrow.up.arrow.down")
        }
        .help("Sort by \(selectedTitle), \(ascending ? ascendingTitle.lowercased() : descendingTitle.lowercased())")
        .accessibilityLabel("Sort resources")
        .accessibilityValue("\(selectedTitle), \(ascending ? ascendingTitle : descendingTitle)")
        .accessibilityIdentifier("resource-sort-menu")
    }

    private var selectedTitle: String { options.first { $0.0 == column }?.1 ?? "" }
}
