import SwiftUI

enum RuneFindBarMetrics {
    static let supportedInspectorWidth: CGFloat = 320
    static let minimumSearchFieldWidth: CGFloat = 160
    static let idealSearchFieldWidth: CGFloat = 240
    static let primaryMinimumWidth: CGFloat =
        RuneUILayoutMetrics.inspectorControlLeadingAccessoryWidth
        + RuneUILayoutMetrics.inspectorControlColumnSpacing
        + minimumSearchFieldWidth
    static let actionSpacing: CGFloat = RuneAdaptiveToolbarMetrics.groupSpacing
    static let rowSpacing: CGFloat = RuneAdaptiveToolbarMetrics.rowSpacing
}

/// Keeps the fixed-size navigation rail trailing while allowing the search
/// field to use its supported minimum width before the bar grows to two rows.
private struct RuneFindBarActionRailLayout: Layout {
    let minimumRowHeight: CGFloat
    let forcesStackedLayout: Bool

    func sizeThatFits(
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) -> CGSize {
        guard subviews.count == 2 else { return .zero }

        let secondarySize = subviews[1].sizeThatFits(.unspecified)
        let inlineMinimumWidth = RuneFindBarMetrics.primaryMinimumWidth
            + RuneFindBarMetrics.actionSpacing
            + secondarySize.width
        let width = proposal.width ?? inlineMinimumWidth

        if !forcesStackedLayout, width >= inlineMinimumWidth {
            let primaryWidth = width
                - RuneFindBarMetrics.actionSpacing
                - secondarySize.width
            let primarySize = subviews[0].sizeThatFits(
                ProposedViewSize(width: primaryWidth, height: nil)
            )
            return CGSize(
                width: width,
                height: max(minimumRowHeight, primarySize.height, secondarySize.height)
            )
        }

        let primarySize = subviews[0].sizeThatFits(
            ProposedViewSize(width: width, height: nil)
        )
        let fittedSecondarySize = subviews[1].sizeThatFits(
            ProposedViewSize(width: width, height: nil)
        )
        return CGSize(
            width: width,
            height: max(minimumRowHeight, primarySize.height)
                + RuneFindBarMetrics.rowSpacing
                + max(minimumRowHeight, fittedSecondarySize.height)
        )
    }

    func placeSubviews(
        in bounds: CGRect,
        proposal: ProposedViewSize,
        subviews: Subviews,
        cache: inout ()
    ) {
        guard subviews.count == 2 else { return }

        let secondarySize = subviews[1].sizeThatFits(.unspecified)
        let inlineMinimumWidth = RuneFindBarMetrics.primaryMinimumWidth
            + RuneFindBarMetrics.actionSpacing
            + secondarySize.width

        if !forcesStackedLayout, bounds.width >= inlineMinimumWidth {
            let primaryWidth = bounds.width
                - RuneFindBarMetrics.actionSpacing
                - secondarySize.width
            let primarySize = subviews[0].sizeThatFits(
                ProposedViewSize(width: primaryWidth, height: nil)
            )
            let rowHeight = max(minimumRowHeight, primarySize.height, secondarySize.height)

            subviews[0].place(
                at: CGPoint(
                    x: bounds.minX,
                    y: bounds.minY + (rowHeight - primarySize.height) / 2
                ),
                anchor: .topLeading,
                proposal: ProposedViewSize(width: primaryWidth, height: primarySize.height)
            )
            subviews[1].place(
                at: CGPoint(
                    x: bounds.maxX,
                    y: bounds.minY + (rowHeight - secondarySize.height) / 2
                ),
                anchor: .topTrailing,
                proposal: ProposedViewSize(
                    width: secondarySize.width,
                    height: secondarySize.height
                )
            )
            return
        }

        let primarySize = subviews[0].sizeThatFits(
            ProposedViewSize(width: bounds.width, height: nil)
        )
        let fittedSecondarySize = subviews[1].sizeThatFits(
            ProposedViewSize(width: bounds.width, height: nil)
        )
        let primaryRowHeight = max(minimumRowHeight, primarySize.height)
        let secondaryRowHeight = max(minimumRowHeight, fittedSecondarySize.height)

        subviews[0].place(
            at: CGPoint(
                x: bounds.minX,
                y: bounds.minY + (primaryRowHeight - primarySize.height) / 2
            ),
            anchor: .topLeading,
            proposal: ProposedViewSize(width: bounds.width, height: primarySize.height)
        )
        subviews[1].place(
            at: CGPoint(
                x: bounds.minX,
                y: bounds.minY
                    + primaryRowHeight
                    + RuneFindBarMetrics.rowSpacing
                    + (secondaryRowHeight - fittedSecondarySize.height) / 2
            ),
            anchor: .topLeading,
            proposal: ProposedViewSize(
                width: fittedSecondarySize.width,
                height: fittedSecondarySize.height
            )
        )
    }
}

/// Shared find-bar chrome. Indexing and navigation remain owned by each
/// surface, while the controls use the same wide and compact layout policy.
struct RuneFindBarChrome<Primary: View, Secondary: View>: View {
    let accessibilityLabel: String
    @ViewBuilder let primary: Primary
    @ViewBuilder let secondary: Secondary
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    init(
        _ accessibilityLabel: String,
        @ViewBuilder primary: () -> Primary,
        @ViewBuilder secondary: () -> Secondary
    ) {
        self.accessibilityLabel = accessibilityLabel
        self.primary = primary()
        self.secondary = secondary()
    }

    var body: some View {
        RuneFindBarActionRailLayout(
            minimumRowHeight: minimumRowHeight,
            forcesStackedLayout: dynamicTypeSize.isAccessibilitySize
        ) {
            primary
            secondary
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityElement(children: .contain)
        .accessibilityLabel(accessibilityLabel)
        .controlSize(.small)
        .padding(.horizontal, RuneUILayoutMetrics.inspectorControlContentInset)
        .padding(.vertical, RuneUILayoutMetrics.inspectorControlChromeVerticalPadding)
        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
        .overlay {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .stroke(Color.primary.opacity(0.16), lineWidth: 1)
        }
        .shadow(color: .black.opacity(0.16), radius: 10, x: 0, y: 5)
        .contentShape(Rectangle())
        .runePointerCursor()
    }

    private var minimumRowHeight: CGFloat {
        dynamicTypeSize.isAccessibilitySize
            ? RuneAdaptiveToolbarMetrics.accessibilityMinimumRowHeight
            : RuneAdaptiveToolbarMetrics.minimumRowHeight
    }
}

struct RuneMatchCaseButton: View {
    @Binding var isSelected: Bool
    let helpText: String
    @Environment(\.runeThemePalette) private var runeThemePalette

    init(isSelected: Binding<Bool>, help: String = "Match case") {
        _isSelected = isSelected
        helpText = help
    }

    var body: some View {
        Button {
            isSelected.toggle()
        } label: {
            Text("Aa")
                .font(.caption.weight(.semibold))
                .frame(
                    width: RuneUILayoutMetrics.iconButtonSize,
                    height: RuneUILayoutMetrics.iconButtonSize
                )
                .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .foregroundStyle(
            isSelected
                ? (runeThemePalette?.accent ?? Color.accentColor)
                : (runeThemePalette?.secondaryText ?? Color.secondary)
        )
        .background {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous)
                .fill(
                    isSelected
                        ? (runeThemePalette?.accent ?? Color.accentColor).opacity(0.22)
                        : Color.clear
                )
        }
        .help(helpText)
        .accessibilityLabel(helpText)
        .accessibilityValue(isSelected ? "Selected" : "Not selected")
        .accessibilityAddTraits(isSelected ? .isSelected : [])
    }
}
