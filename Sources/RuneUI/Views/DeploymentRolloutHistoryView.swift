import SwiftUI

enum DeploymentRolloutHistoryLayoutMetrics {
    static let supportedInspectorWidth: CGFloat = 320
    static let minimumTableWidth: CGFloat = 436
    static let revisionWidth: CGFloat = 72
    static let replicaSetMinimumWidth: CGFloat = 180
    static let changeCauseWidth: CGFloat = 160
    static let columnSpacing: CGFloat = 12
}

struct DeploymentRolloutHistoryView: View {
    let history: String

    var body: some View {
        let rows = DeploymentRolloutHistoryPresentation.rows(from: history)
        Group {
            if history.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                emptyState
            } else if rows.isEmpty {
                rawHistoryView
            } else {
                rowsView(rows)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var emptyState: some View {
        RuneContentStateView(
            .empty(
                title: "No rollout history loaded",
                message: "Revision history will appear after it is returned by the cluster."
            ),
            variant: .card
        )
    }

    private var rawHistoryView: some View {
        ScrollView {
            Text(history)
                .runeText(.code)
                .frame(maxWidth: .infinity, alignment: .leading)
                .textSelection(.enabled)
                .padding(RuneSpacing.inset)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(RuneSurfaceBackground(kind: .editor))
    }

    private func rowsView(_ rows: [DeploymentRolloutHistoryRow]) -> some View {
        ScrollView(.vertical) {
            DeploymentRolloutHistoryTable(rows: rows)
                .padding(RuneSpacing.inset)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(RuneSurfaceBackground(kind: .editor))
    }
}

struct DeploymentRolloutHistoryTable: View {
    let rows: [DeploymentRolloutHistoryRow]

    var body: some View {
        ViewThatFits(in: .horizontal) {
            wideTable
                .frame(minWidth: DeploymentRolloutHistoryLayoutMetrics.minimumTableWidth)
            compactTable
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var wideTable: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
            Divider()
                .opacity(0.45)
                .padding(.bottom, RuneSpacing.xxs)
            ForEach(rows) { row in
                wideRow(row)
            }
        }
    }

    private var compactTable: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.tight) {
            Text("Rollout revisions")
                .runeText(.microEmphasis)
                .foregroundStyle(.runeSecondary)

            ForEach(rows) { row in
                compactRow(row)
            }
        }
    }

    private var header: some View {
        HStack(spacing: DeploymentRolloutHistoryLayoutMetrics.columnSpacing) {
            columnText(
                "Revision",
                width: DeploymentRolloutHistoryLayoutMetrics.revisionWidth,
                weight: .semibold
            )
            Text("ReplicaSet")
                .runeText(.microEmphasis)
                .foregroundStyle(.runeSecondary)
                .frame(
                    minWidth: DeploymentRolloutHistoryLayoutMetrics.replicaSetMinimumWidth,
                    maxWidth: .infinity,
                    alignment: .leading
                )
            Text("Change Cause")
                .runeText(.microEmphasis)
                .foregroundStyle(.runeSecondary)
                .frame(width: DeploymentRolloutHistoryLayoutMetrics.changeCauseWidth, alignment: .leading)
        }
        .padding(.bottom, RuneSpacing.tight)
    }

    private func wideRow(_ row: DeploymentRolloutHistoryRow) -> some View {
        HStack(spacing: DeploymentRolloutHistoryLayoutMetrics.columnSpacing) {
            columnText(
                row.revision,
                width: DeploymentRolloutHistoryLayoutMetrics.revisionWidth,
                weight: .semibold
            )
            Text(row.replicaSet)
                .runeText(.microEmphasis)
                .lineLimit(1)
                .truncationMode(.middle)
                .textSelection(.enabled)
                .help(row.replicaSet)
                .frame(
                    minWidth: DeploymentRolloutHistoryLayoutMetrics.replicaSetMinimumWidth,
                    maxWidth: .infinity,
                    alignment: .leading
                )
            Text(row.changeCause.isEmpty ? "-" : row.changeCause)
                .runeText(.micro)
                .foregroundStyle(.runeSecondary)
                .lineLimit(1)
                .truncationMode(.middle)
                .textSelection(.enabled)
                .help(row.changeCause.isEmpty ? "No change cause" : row.changeCause)
                .frame(width: DeploymentRolloutHistoryLayoutMetrics.changeCauseWidth, alignment: .leading)
        }
        .padding(.vertical, RuneSpacing.xxs)
    }

    private func compactRow(_ row: DeploymentRolloutHistoryRow) -> some View {
        VStack(alignment: .leading, spacing: RuneSpacing.tight) {
            compactField("Revision", value: row.revision, weight: .semibold)
            compactField("ReplicaSet", value: row.replicaSet, weight: .semibold)
            compactField("Change Cause", value: row.changeCause.isEmpty ? "-" : row.changeCause)
        }
        .padding(.horizontal, RuneSpacing.xs)
        .padding(.vertical, RuneSpacing.tight)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(RuneSurfaceBackground(kind: .listRow(isSelected: false)))
    }

    private func compactField(
        _ title: String,
        value: String,
        weight: Font.Weight = .regular
    ) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: RuneSpacing.xs) {
            Text(title)
                .runeText(.microEmphasis)
                .foregroundStyle(.runeSecondary)
                .frame(width: 76, alignment: .leading)

            Text(value)
                .runeText(.micro, weight: weight)
                .foregroundStyle(.runeSecondary)
                .lineLimit(title == "Change Cause" ? 2 : 1)
                .truncationMode(.middle)
                .textSelection(.enabled)
                .help(value)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    private func columnText(
        _ text: String,
        width: CGFloat,
        weight: Font.Weight = .regular
    ) -> some View {
        Text(text)
            .runeText(.micro, weight: weight).monospacedDigit()
            .foregroundStyle(.runeSecondary)
            .lineLimit(1)
            .truncationMode(.middle)
            .textSelection(.enabled)
            .frame(width: width, alignment: .leading)
    }
}
