import SwiftUI

struct DeploymentRolloutHistoryView: View {
    let history: String

    var body: some View {
        let rows = DeploymentRolloutHistoryPresentation.rows(from: history)
        if history.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            emptyState
        } else if rows.isEmpty {
            rawHistoryView
        } else {
            rowsView(rows)
        }
    }

    private var emptyState: some View {
        VStack(spacing: 8) {
            Image(systemName: "clock.arrow.circlepath")
                .font(.title3)
                .foregroundStyle(.secondary)
            Text("No rollout history loaded")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, minHeight: 96)
    }

    private var rawHistoryView: some View {
        ScrollView {
            Text(history)
                .font(.system(size: 12, weight: .regular, design: .monospaced))
                .frame(maxWidth: .infinity, alignment: .leading)
                .textSelection(.enabled)
                .padding(10)
                .background(RuneSurfaceBackground(kind: .editor))
        }
    }

    private func rowsView(_ rows: [DeploymentRolloutHistoryRow]) -> some View {
        ScrollView([.vertical, .horizontal]) {
            VStack(alignment: .leading, spacing: 0) {
                header
                ForEach(rows) { row in
                    rowView(row)
                }
            }
            .padding(10)
            .frame(minWidth: 520, alignment: .leading)
            .background(RuneSurfaceBackground(kind: .editor))
        }
    }

    private var header: some View {
        HStack(spacing: 12) {
            columnText("Revision", width: 72, weight: .semibold)
            columnText("ReplicaSet", width: 230, weight: .semibold)
            Text("Change Cause")
                .font(.system(size: 12, weight: .semibold, design: .monospaced))
                .foregroundStyle(.secondary)
                .frame(minWidth: 160, maxWidth: .infinity, alignment: .leading)
        }
        .padding(.bottom, 6)
    }

    private func rowView(_ row: DeploymentRolloutHistoryRow) -> some View {
        HStack(spacing: 12) {
            columnText(row.revision, width: 72)
            columnText(row.replicaSet, width: 230)
            Text(row.changeCause.isEmpty ? "-" : row.changeCause)
                .font(.system(size: 12, weight: .regular, design: .monospaced))
                .lineLimit(1)
                .truncationMode(.middle)
                .textSelection(.enabled)
                .frame(minWidth: 160, maxWidth: .infinity, alignment: .leading)
        }
        .padding(.vertical, 3)
    }

    private func columnText(
        _ text: String,
        width: CGFloat,
        weight: Font.Weight = .regular
    ) -> some View {
        Text(text)
            .font(.system(size: 12, weight: weight, design: .monospaced))
            .lineLimit(1)
            .truncationMode(.middle)
            .textSelection(.enabled)
            .frame(width: width, alignment: .leading)
    }
}
