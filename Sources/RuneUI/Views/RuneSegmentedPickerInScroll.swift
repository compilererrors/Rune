import SwiftUI

enum RuneSegmentedPickerOverflowBehavior: Sendable, Equatable {
    case horizontalScroll
    case intrinsic
}

/// Wraps `Picker` + `.segmented` in a horizontal `ScrollView` so `NSSegmentedControl` intrinsic width
/// never draws past the navigation column edge.
struct RuneSegmentedPickerInScroll<SelectionValue: Hashable, Content: View>: View {
    private let title: String
    private let selection: Binding<SelectionValue>
    private let labelsHidden: Bool
    private let overflowBehavior: RuneSegmentedPickerOverflowBehavior
    private let content: Content

    init(
        _ title: String,
        selection: Binding<SelectionValue>,
        labelsHidden: Bool = false,
        overflowBehavior: RuneSegmentedPickerOverflowBehavior = .horizontalScroll,
        @ViewBuilder content: () -> Content
    ) {
        self.title = title
        self.selection = selection
        self.labelsHidden = labelsHidden
        self.overflowBehavior = overflowBehavior
        self.content = content()
    }

    @ViewBuilder
    var body: some View {
        switch overflowBehavior {
        case .horizontalScroll:
            ScrollView(.horizontal, showsIndicators: true) {
                segmentedPicker
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .accessibilityHint("Scroll horizontally to reveal additional choices")
        case .intrinsic:
            segmentedPicker
        }
    }

    @ViewBuilder
    private var segmentedPicker: some View {
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

/// Uses the full segmented control while it fits and switches to a native menu
/// at compact widths. Keeping the decision in one component gives every
/// resource-family selector the same breakpoint and interaction model.
struct RuneAdaptiveSegmentedPicker<SelectionValue: Hashable, Content: View>: View {
    private let title: String
    private let selection: Binding<SelectionValue>
    private let labelsHidden: Bool
    private let compactMaximumWidth: CGFloat?
    private let content: Content

    init(
        _ title: String,
        selection: Binding<SelectionValue>,
        labelsHidden: Bool = false,
        compactMaximumWidth: CGFloat? = nil,
        @ViewBuilder content: () -> Content
    ) {
        self.title = title
        self.selection = selection
        self.labelsHidden = labelsHidden
        self.compactMaximumWidth = compactMaximumWidth
        self.content = content()
    }

    var body: some View {
        ViewThatFits(in: .horizontal) {
            RuneSegmentedPickerInScroll(
                title,
                selection: selection,
                labelsHidden: labelsHidden,
                overflowBehavior: .intrinsic
            ) {
                content
            }
            .accessibilityIdentifier("adaptive-segmented-picker-segments")

            compactPicker
                .frame(maxWidth: compactMaximumWidth, alignment: .leading)
                .accessibilityIdentifier("adaptive-segmented-picker-menu")
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityLabel(title)
    }

    @ViewBuilder
    private var compactPicker: some View {
        if labelsHidden {
            Picker(title, selection: selection) {
                content
            }
            .pickerStyle(.menu)
            .labelsHidden()
        } else {
            Picker(title, selection: selection) {
                content
            }
            .pickerStyle(.menu)
        }
    }
}
