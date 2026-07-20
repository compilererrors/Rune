import SwiftUI

enum RuneAdaptiveToolbarCompactBehavior: Sendable, Equatable {
    case stacked
    case horizontalScroll

    var usesHorizontalScrolling: Bool {
        self == .horizontalScroll
    }
}

enum RuneAdaptiveToolbarMetrics {
    static let supportedInspectorWidth: CGFloat = 320
    static let minimumRowHeight: CGFloat = 30
    static let accessibilityMinimumRowHeight: CGFloat = 44
    static let groupSpacing: CGFloat = 8
    static let rowSpacing: CGFloat = 8
}

/// Two semantic control groups that remain inline when they fit and use an
/// explicit compact policy otherwise. Scrolling is never enabled implicitly.
struct RuneAdaptiveToolbar<Primary: View, Secondary: View>: View {
    let accessibilityLabel: String
    let compactBehavior: RuneAdaptiveToolbarCompactBehavior
    @ViewBuilder let primary: Primary
    @ViewBuilder let secondary: Secondary
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    init(
        _ accessibilityLabel: String,
        compactBehavior: RuneAdaptiveToolbarCompactBehavior = .stacked,
        @ViewBuilder primary: () -> Primary,
        @ViewBuilder secondary: () -> Secondary
    ) {
        self.accessibilityLabel = accessibilityLabel
        self.compactBehavior = compactBehavior
        self.primary = primary()
        self.secondary = secondary()
    }

    var body: some View {
        ViewThatFits(in: .horizontal) {
            inlineLayout
            compactLayout
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityElement(children: .contain)
        .accessibilityLabel(accessibilityLabel)
    }

    private var inlineLayout: some View {
        HStack(alignment: .center, spacing: RuneAdaptiveToolbarMetrics.groupSpacing) {
            primary
                .fixedSize(horizontal: true, vertical: false)
            Spacer(minLength: RuneAdaptiveToolbarMetrics.groupSpacing)
            secondary
                .fixedSize(horizontal: true, vertical: false)
        }
        .frame(
            maxWidth: .infinity,
            minHeight: minimumRowHeight,
            alignment: .leading
        )
    }

    @ViewBuilder
    private var compactLayout: some View {
        switch compactBehavior {
        case .stacked:
            VStack(alignment: .leading, spacing: RuneAdaptiveToolbarMetrics.rowSpacing) {
                primary
                    .frame(
                        maxWidth: .infinity,
                        minHeight: minimumRowHeight,
                        alignment: .leading
                    )
                secondary
                    .frame(
                        maxWidth: .infinity,
                        minHeight: minimumRowHeight,
                        alignment: .leading
                    )
            }
            .fixedSize(horizontal: false, vertical: true)
        case .horizontalScroll:
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(alignment: .center, spacing: RuneAdaptiveToolbarMetrics.groupSpacing) {
                    primary
                    secondary
                }
                .frame(minHeight: minimumRowHeight)
                .fixedSize(horizontal: true, vertical: false)
            }
        }
    }

    private var minimumRowHeight: CGFloat {
        dynamicTypeSize.isAccessibilitySize
            ? RuneAdaptiveToolbarMetrics.accessibilityMinimumRowHeight
            : RuneAdaptiveToolbarMetrics.minimumRowHeight
    }
}
