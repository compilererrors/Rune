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

private struct RuneAdaptiveToolbarStackedLayout: Layout {
    let minimumRowHeight: CGFloat

    func sizeThatFits(
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) -> CGSize {
        guard !subviews.isEmpty else {
            return CGSize(width: proposal.width ?? 0, height: 0)
        }
        if subviews.count == 1 {
            let idealSize = subviews[0].sizeThatFits(.unspecified)
            let width = proposal.width ?? idealSize.width
            let fittedSize = subviews[0].sizeThatFits(
                ProposedViewSize(width: width, height: nil)
            )
            return CGSize(
                width: width,
                height: max(minimumRowHeight, fittedSize.height)
            )
        }
        guard subviews.count == 2 else { return .zero }
        let idealSizes = subviews.map { $0.sizeThatFits(.unspecified) }
        let inlineMinimumWidth = idealSizes[0].width
            + RuneAdaptiveToolbarMetrics.groupSpacing
            + idealSizes[1].width
        let width = proposal.width ?? inlineMinimumWidth

        if width >= inlineMinimumWidth {
            return CGSize(
                width: width,
                height: max(minimumRowHeight, idealSizes.map(\.height).max() ?? 0)
            )
        }

        let rowSizes = subviews.map {
            $0.sizeThatFits(ProposedViewSize(width: width, height: nil))
        }
        return CGSize(
            width: width,
            height: max(minimumRowHeight, rowSizes[0].height)
                + RuneAdaptiveToolbarMetrics.rowSpacing
                + max(minimumRowHeight, rowSizes[1].height)
        )
    }

    func placeSubviews(
        in bounds: CGRect,
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) {
        guard !subviews.isEmpty else { return }
        if subviews.count == 1 {
            let fittedSize = subviews[0].sizeThatFits(
                ProposedViewSize(width: bounds.width, height: nil)
            )
            subviews[0].place(
                at: CGPoint(
                    x: bounds.minX,
                    y: bounds.minY + max(0, (bounds.height - fittedSize.height) / 2)
                ),
                anchor: .topLeading,
                proposal: ProposedViewSize(
                    width: min(bounds.width, fittedSize.width),
                    height: fittedSize.height
                )
            )
            return
        }
        guard subviews.count == 2 else { return }
        let idealSizes = subviews.map { $0.sizeThatFits(.unspecified) }
        let inlineMinimumWidth = idealSizes[0].width
            + RuneAdaptiveToolbarMetrics.groupSpacing
            + idealSizes[1].width

        if bounds.width >= inlineMinimumWidth {
            let rowHeight = max(minimumRowHeight, idealSizes.map(\.height).max() ?? 0)
            subviews[0].place(
                at: CGPoint(
                    x: bounds.minX,
                    y: bounds.minY + (rowHeight - idealSizes[0].height) / 2
                ),
                anchor: .topLeading,
                proposal: ProposedViewSize(
                    width: idealSizes[0].width,
                    height: idealSizes[0].height
                )
            )
            subviews[1].place(
                at: CGPoint(
                    x: bounds.maxX - idealSizes[1].width,
                    y: bounds.minY + (rowHeight - idealSizes[1].height) / 2
                ),
                anchor: .topLeading,
                proposal: ProposedViewSize(
                    width: idealSizes[1].width,
                    height: idealSizes[1].height
                )
            )
            return
        }

        let rowSizes = subviews.map {
            $0.sizeThatFits(ProposedViewSize(width: bounds.width, height: nil))
        }
        let primaryRowHeight = max(minimumRowHeight, rowSizes[0].height)
        let secondaryRowHeight = max(minimumRowHeight, rowSizes[1].height)
        subviews[0].place(
            at: CGPoint(
                x: bounds.minX,
                y: bounds.minY + (primaryRowHeight - rowSizes[0].height) / 2
            ),
            anchor: .topLeading,
            proposal: ProposedViewSize(width: bounds.width, height: rowSizes[0].height)
        )
        subviews[1].place(
            at: CGPoint(
                x: bounds.minX,
                y: bounds.minY
                    + primaryRowHeight
                    + RuneAdaptiveToolbarMetrics.rowSpacing
                    + (secondaryRowHeight - rowSizes[1].height) / 2
            ),
            anchor: .topLeading,
            proposal: ProposedViewSize(width: bounds.width, height: rowSizes[1].height)
        )
    }
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
        Group {
            switch compactBehavior {
            case .stacked:
                RuneAdaptiveToolbarStackedLayout(minimumRowHeight: minimumRowHeight) {
                    HStack(spacing: RuneAdaptiveToolbarMetrics.groupSpacing) {
                        primary
                    }
                    HStack(spacing: RuneAdaptiveToolbarMetrics.groupSpacing) {
                        secondary
                    }
                }
            case .horizontalScroll:
                ViewThatFits(in: .horizontal) {
                    inlineLayout
                    horizontalScrollLayout
                }
            }
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

    private var horizontalScrollLayout: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(alignment: .center, spacing: RuneAdaptiveToolbarMetrics.groupSpacing) {
                primary
                secondary
            }
            .frame(minHeight: minimumRowHeight)
            .fixedSize(horizontal: true, vertical: false)
        }
    }

    private var minimumRowHeight: CGFloat {
        dynamicTypeSize.isAccessibilitySize
            ? RuneAdaptiveToolbarMetrics.accessibilityMinimumRowHeight
            : RuneAdaptiveToolbarMetrics.minimumRowHeight
    }
}
