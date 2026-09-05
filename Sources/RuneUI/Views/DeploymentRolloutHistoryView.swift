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
                .font(.system(size: 12, weight: .regular, design: .monospaced))
                .frame(maxWidth: .infinity, alignment: .leading)
                .textSelection(.enabled)
                .padding(10)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(RuneSurfaceBackground(kind: .editor))
    }

    private func rowsView(_ rows: [DeploymentRolloutHistoryRow]) -> some View {
        ScrollView(.vertical) {
            DeploymentRolloutHistoryTable(rows: rows)
                .padding(10)
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
                .padding(.bottom, 4)
            ForEach(rows) { row in
                wideRow(row)
            }
        }
    }

    private var compactTable: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("Rollout revisions")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)

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
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
                .frame(
                    minWidth: DeploymentRolloutHistoryLayoutMetrics.replicaSetMinimumWidth,
                    maxWidth: .infinity,
                    alignment: .leading
                )
            Text("Change Cause")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
                .frame(width: DeploymentRolloutHistoryLayoutMetrics.changeCauseWidth, alignment: .leading)
        }
        .padding(.bottom, 6)
    }

    private func wideRow(_ row: DeploymentRolloutHistoryRow) -> some View {
        HStack(spacing: DeploymentRolloutHistoryLayoutMetrics.columnSpacing) {
            columnText(
                row.revision,
                width: DeploymentRolloutHistoryLayoutMetrics.revisionWidth,
                weight: .semibold
            )
            Text(row.replicaSet)
                .font(.caption.weight(.semibold))
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
                .font(.caption)
                .foregroundStyle(row.changeCause.isEmpty ? Color.secondary.opacity(0.55) : Color.secondary)
                .lineLimit(1)
                .truncationMode(.middle)
                .textSelection(.enabled)
                .help(row.changeCause.isEmpty ? "No change cause" : row.changeCause)
                .frame(width: DeploymentRolloutHistoryLayoutMetrics.changeCauseWidth, alignment: .leading)
        }
        .padding(.vertical, 5)
    }

    private func compactRow(_ row: DeploymentRolloutHistoryRow) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            compactField("Revision", value: row.revision, weight: .semibold)
            compactField("ReplicaSet", value: row.replicaSet, weight: .semibold)
            compactField("Change Cause", value: row.changeCause.isEmpty ? "-" : row.changeCause)
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 7)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(RuneSurfaceBackground(kind: .listRow(isSelected: false)))
    }

    private func compactField(
        _ title: String,
        value: String,
        weight: Font.Weight = .regular
    ) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 8) {
            Text(title)
                .font(.caption2.weight(.semibold))
                .foregroundStyle(.secondary)
                .frame(width: 76, alignment: .leading)

            Text(value)
                .font(.caption.weight(weight))
                .foregroundStyle(.secondary)
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
            .font(.caption.monospacedDigit().weight(weight))
            .foregroundStyle(.secondary)
            .lineLimit(1)
            .truncationMode(.middle)
            .textSelection(.enabled)
            .frame(width: width, alignment: .leading)
    }
}
