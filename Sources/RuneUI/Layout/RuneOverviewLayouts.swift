import SwiftUI

/// Keeps overview modules balanced across the final row while every row still
/// fills the same leading and trailing grid lines.
struct RuneBalancedOverviewGrid: Layout {
    let minimumItemWidth: CGFloat
    let spacing: CGFloat

    init(minimumItemWidth: CGFloat = 160, spacing: CGFloat = 10) {
        self.minimumItemWidth = minimumItemWidth
        self.spacing = spacing
    }

    func sizeThatFits(
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) -> CGSize {
        guard !subviews.isEmpty else {
            return CGSize(width: proposal.width ?? 0, height: 0)
        }

        let availableWidth = resolvedWidth(proposal: proposal, subviews: subviews)
        let rows = Self.rowRanges(
            itemCount: subviews.count,
            maximumColumns: maximumColumns(for: availableWidth)
        )
        let rowHeights = rows.map { range in
            measuredRowHeight(
                subviews: subviews,
                range: range,
                availableWidth: availableWidth
            )
        }
        return CGSize(
            width: availableWidth,
            height: rowHeights.reduce(0, +) + spacing * CGFloat(max(0, rows.count - 1))
        )
    }

    func placeSubviews(
        in bounds: CGRect,
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) {
        let rows = Self.rowRanges(
            itemCount: subviews.count,
            maximumColumns: maximumColumns(for: bounds.width)
        )
        var y = bounds.minY

        for range in rows {
            let itemWidth = widthPerItem(itemCount: range.count, availableWidth: bounds.width)
            let rowHeight = measuredRowHeight(
                subviews: subviews,
                range: range,
                availableWidth: bounds.width
            )
            var x = bounds.minX

            for index in range {
                subviews[index].place(
                    at: CGPoint(x: x, y: y),
                    anchor: .topLeading,
                    proposal: ProposedViewSize(width: itemWidth, height: rowHeight)
                )
                x += itemWidth + spacing
            }
            y += rowHeight + spacing
        }
    }

    static func balancedRowCounts(itemCount: Int, maximumColumns: Int) -> [Int] {
        guard itemCount > 0 else { return [] }
        let boundedMaximum = max(1, min(itemCount, maximumColumns))
        let rowCount = Int(ceil(Double(itemCount) / Double(boundedMaximum)))
        let minimumCount = itemCount / rowCount
        let widerRowCount = itemCount % rowCount
        return (0..<rowCount).map { index in
            minimumCount + (index < widerRowCount ? 1 : 0)
        }
    }

    private static func rowRanges(itemCount: Int, maximumColumns: Int) -> [Range<Int>] {
        var start = 0
        return balancedRowCounts(itemCount: itemCount, maximumColumns: maximumColumns).map { count in
            defer { start += count }
            return start..<(start + count)
        }
    }

    private func resolvedWidth(proposal: ProposedViewSize, subviews: Subviews) -> CGFloat {
        if let proposedWidth = proposal.width, proposedWidth.isFinite {
            return max(0, proposedWidth)
        }
        let idealWidths = subviews.map {
            max(minimumItemWidth, $0.sizeThatFits(.unspecified).width)
        }
        return idealWidths.reduce(0, +) + spacing * CGFloat(max(0, idealWidths.count - 1))
    }

    private func maximumColumns(for availableWidth: CGFloat) -> Int {
        max(1, Int((availableWidth + spacing) / (minimumItemWidth + spacing)))
    }

    private func widthPerItem(itemCount: Int, availableWidth: CGFloat) -> CGFloat {
        guard itemCount > 0 else { return 0 }
        return max(
            0,
            (availableWidth - spacing * CGFloat(itemCount - 1)) / CGFloat(itemCount)
        )
    }

    private func measuredRowHeight(
        subviews: Subviews,
        range: Range<Int>,
        availableWidth: CGFloat
    ) -> CGFloat {
        let itemWidth = widthPerItem(itemCount: range.count, availableWidth: availableWidth)
        return range.reduce(CGFloat.zero) { current, index in
            max(
                current,
                subviews[index].sizeThatFits(
                    ProposedViewSize(width: itemWidth, height: nil)
                ).height
            )
        }
    }
}

/// A compact flow rail for short status badges. Badges keep their intrinsic
/// width and wrap onto stable rows instead of clipping or forcing the pane wide.
struct RuneBadgeFlowLayout: Layout {
    let horizontalSpacing: CGFloat
    let verticalSpacing: CGFloat

    init(horizontalSpacing: CGFloat = 8, verticalSpacing: CGFloat = 8) {
        self.horizontalSpacing = horizontalSpacing
        self.verticalSpacing = verticalSpacing
    }

    struct Cache {
        var sizes: [CGSize]
    }

    func makeCache(subviews: Subviews) -> Cache {
        Cache(sizes: subviews.map { $0.sizeThatFits(.unspecified) })
    }

    func updateCache(_ cache: inout Cache, subviews: Subviews) {
        cache.sizes = subviews.map { $0.sizeThatFits(.unspecified) }
    }

    func sizeThatFits(
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout Cache
    ) -> CGSize {
        let availableWidth = proposal.width ?? cache.sizes.reduce(0) {
            $0 + $1.width
        } + horizontalSpacing * CGFloat(max(0, cache.sizes.count - 1))
        let result = Self.flowMetrics(
            sizes: cache.sizes,
            availableWidth: max(0, availableWidth),
            horizontalSpacing: horizontalSpacing,
            verticalSpacing: verticalSpacing
        )
        return CGSize(width: availableWidth, height: result.height)
    }

    func placeSubviews(
        in bounds: CGRect,
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout Cache
    ) {
        let result = Self.flowMetrics(
            sizes: cache.sizes,
            availableWidth: bounds.width,
            horizontalSpacing: horizontalSpacing,
            verticalSpacing: verticalSpacing
        )
        for (index, origin) in result.origins.enumerated() where index < subviews.count {
            subviews[index].place(
                at: CGPoint(x: bounds.minX + origin.x, y: bounds.minY + origin.y),
                anchor: .topLeading,
                proposal: ProposedViewSize(cache.sizes[index])
            )
        }
    }

    static func flowMetrics(
        sizes: [CGSize],
        availableWidth: CGFloat,
        horizontalSpacing: CGFloat,
        verticalSpacing: CGFloat
    ) -> (origins: [CGPoint], height: CGFloat) {
        guard !sizes.isEmpty else { return ([], 0) }
        var origins: [CGPoint] = []
        origins.reserveCapacity(sizes.count)
        var x: CGFloat = 0
        var y: CGFloat = 0
        var lineHeight: CGFloat = 0

        for size in sizes {
            if x > 0, x + size.width > availableWidth {
                x = 0
                y += lineHeight + verticalSpacing
                lineHeight = 0
            }
            origins.append(CGPoint(x: x, y: y))
            x += size.width + horizontalSpacing
            lineHeight = max(lineHeight, size.height)
        }
        return (origins, y + lineHeight)
    }
}

struct RuneSectionHeader<Trailing: View>: View {
    let title: String
    let systemImage: String
    let tint: Color
    @ViewBuilder let trailing: Trailing

    init(
        _ title: String,
        systemImage: String,
        tint: Color = .secondary,
        @ViewBuilder trailing: () -> Trailing
    ) {
        self.title = title
        self.systemImage = systemImage
        self.tint = tint
        self.trailing = trailing()
    }

    var body: some View {
        HStack(alignment: .center, spacing: 8) {
            Image(systemName: systemImage)
                .font(.system(size: 13, weight: .semibold))
                .foregroundStyle(tint)
                .frame(width: 18, height: 18, alignment: .center)
            Text(title)
                .font(.headline.weight(.semibold))
            Spacer(minLength: 8)
            trailing
        }
        .frame(maxWidth: .infinity, minHeight: 22, alignment: .leading)
        .accessibilityElement(children: .contain)
    }
}

extension RuneSectionHeader where Trailing == EmptyView {
    init(_ title: String, systemImage: String, tint: Color = .secondary) {
        self.init(title, systemImage: systemImage, tint: tint) {
            EmptyView()
        }
    }
}
