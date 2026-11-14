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
    static let minimumRowHeight: CGFloat = RuneControlHeight.toolbar
    static let accessibilityMinimumRowHeight: CGFloat = RuneControlHeight.accessibility
    static let groupSpacing: CGFloat = RuneSpacing.xs
    static let rowSpacing: CGFloat = RuneSpacing.xs
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

/// Places subviews left to right and starts a new row when the next one does
/// not fit. Used as the last fallback of `RuneToolbarOverflowRow`, so a
/// toolbar wraps instead of clipping.
struct RuneWrappingLayout: Layout {
    var spacing: CGFloat = RuneSpacing.xs
    var rowSpacing: CGFloat = RuneSpacing.xs

    func sizeThatFits(proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) -> CGSize {
        // Without a width the layout reports its narrowest form (one control
        // per row), so `ViewThatFits` can pick a wrapped variant.
        let rows = arrange(width: proposal.width ?? 0, subviews: subviews)
        let width = proposal.width ?? rows.map(\.width).max() ?? 0
        return CGSize(width: width, height: rows.reduce(0) { $0 + $1.height } + rowSpacing * CGFloat(max(0, rows.count - 1)))
    }

    func placeSubviews(in bounds: CGRect, proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) {
        var y = bounds.minY
        for row in arrange(width: bounds.width, subviews: subviews) {
            var x = bounds.minX
            for (index, size) in zip(row.indices, row.sizes) {
                subviews[index].place(
                    at: CGPoint(x: x, y: y + (row.height - size.height) / 2),
                    anchor: .topLeading,
                    proposal: ProposedViewSize(size)
                )
                x += size.width + spacing
            }
            y += row.height + rowSpacing
        }
    }

    private struct Row {
        var indices: [Int] = []
        var sizes: [CGSize] = []
        var width: CGFloat = 0
        var height: CGFloat = 0
    }

    private func arrange(width: CGFloat?, subviews: Subviews) -> [Row] {
        let limit = width ?? .infinity
        var rows: [Row] = []
        var current = Row()
        for index in subviews.indices {
            var size = subviews[index].sizeThatFits(.unspecified)
            let remaining = current.indices.isEmpty ? limit : limit - current.width - spacing
            if size.width > remaining, remaining > 0 {
                // Adaptive children (with their own compact variant) get the
                // remaining width first, so they compact before the row wraps.
                let fitted = subviews[index].sizeThatFits(ProposedViewSize(width: remaining, height: nil))
                if fitted.width <= remaining + 0.5 { size = fitted }
            }
            if !current.indices.isEmpty && current.width + spacing + size.width > limit + 0.5 {
                rows.append(current)
                current = Row()
                if size.width > limit, limit > 0 {
                    size = subviews[index].sizeThatFits(ProposedViewSize(width: limit, height: nil))
                }
            }
            current.width = current.indices.isEmpty ? size.width : current.width + spacing + size.width
            current.height = max(current.height, size.height)
            current.indices.append(index)
            current.sizes.append(size)
        }
        if !current.indices.isEmpty { rows.append(current) }
        return rows
    }
}

/// One toolbar row that never clips: full labels when they fit, then icons
/// only (labels stay as help and accessibility names), then wrapping onto
/// more rows, with labels while each control still fits on its own.
struct RuneToolbarOverflowRow<Content: View>: View {
    var spacing: CGFloat = RuneSpacing.xs
    @ViewBuilder let content: Content

    init(spacing: CGFloat = RuneSpacing.xs, @ViewBuilder content: () -> Content) {
        self.spacing = spacing
        self.content = content()
    }

    var body: some View {
        ViewThatFits(in: .horizontal) {
            HStack(alignment: .center, spacing: spacing) { content }
                .fixedSize()
                .labelStyle(.titleAndIcon)
            HStack(alignment: .center, spacing: spacing) { content }
                .fixedSize()
                .labelStyle(.iconOnly)
            RuneWrappingLayout(spacing: spacing) { content }
                .labelStyle(.titleAndIcon)
            RuneWrappingLayout(spacing: spacing) { content }
                .labelStyle(.iconOnly)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
