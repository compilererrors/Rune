import SwiftUI

/// Wraps `Picker` + `.segmented` in a horizontal `ScrollView` so `NSSegmentedControl` intrinsic width
/// never draws past the navigation column edge.
struct RuneSegmentedPickerInScroll<SelectionValue: Hashable, Content: View>: View {
    private let title: String
    private let selection: Binding<SelectionValue>
    private let labelsHidden: Bool
    private let content: Content

    init(
        _ title: String,
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
        ScrollView(.horizontal, showsIndicators: true) {
            Group {
                if labelsHidden {
                    Picker(selection: selection) { content } label: {
                        Text(title)
                    }
                        .pickerStyle(.segmented)
                        .labelsHidden()
                        .fixedSize(horizontal: true, vertical: false)
                } else {
                    Picker(selection: selection) { content } label: {
                        Text(title)
                    }
                        .pickerStyle(.segmented)
                        .fixedSize(horizontal: true, vertical: false)
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityHint("Scroll horizontally to reveal additional choices")
    }
}
