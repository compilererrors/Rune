import SwiftUI

/// Wraps `Picker` + `.segmented` in a horizontal `ScrollView` so `NSSegmentedControl` intrinsic width
/// never draws past the split column (sidebar / neighbor panes).
struct RuneSegmentedPickerInScroll<SelectionValue: Hashable, Content: View>: View {
    private let title: LocalizedStringKey
    private let selection: Binding<SelectionValue>
    private let labelsHidden: Bool
    private let content: Content

    init(
        _ title: LocalizedStringKey,
        selection: Binding<SelectionValue>,
        labelsHidden: Bool = false,
        @ViewBuilder content: () -> Content
    ) {
        self.title = title
        self.selection = selection
        self.labelsHidden = labelsHidden
        self.content = content()
    }

    var body: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            Group {
                if labelsHidden {
                    Picker(title, selection: selection) { content }
                        .pickerStyle(.segmented)
                        .labelsHidden()
                        .fixedSize(horizontal: true, vertical: false)
                } else {
                    Picker(title, selection: selection) { content }
                        .pickerStyle(.segmented)
                        .fixedSize(horizontal: true, vertical: false)
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        // Extra inset so the first segment does not sit under the split divider / rounded pane edge.
        .padding(.leading, 6)
    }
}
