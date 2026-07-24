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
            segmentedPicker
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityIdentifier("segmented-picker-tabs")
        .accessibilityHint("Scroll horizontally to reveal additional choices")
    }

    @ViewBuilder
    private var segmentedPicker: some View {
        if labelsHidden {
            Picker(selection: selection) { content } label: {
                Text(title)
            }
            .pickerStyle(.segmented)
            .runeInterfaceFont(weight: .medium)
            .runeInterfaceControlSize(compactBaseline: true)
            .labelsHidden()
            .fixedSize(horizontal: true, vertical: false)
        } else {
            Picker(selection: selection) { content } label: {
                Text(title)
            }
            .pickerStyle(.segmented)
            .runeInterfaceFont(weight: .medium)
            .runeInterfaceControlSize(compactBaseline: true)
            .fixedSize(horizontal: true, vertical: false)
        }
    }
}
