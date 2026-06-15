import SwiftUI

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
        HStack(spacing: 8) {
            Image(systemName: "clock.arrow.circlepath")
                .frame(width: 16)
                .foregroundStyle(.secondary)
            Text("No rollout history loaded")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            Spacer(minLength: 0)
        }
        .padding(10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(RuneSurfaceBackground(kind: .editor))
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
            VStack(alignment: .leading, spacing: 0) {
                header
                Divider()
                    .opacity(0.45)
                    .padding(.bottom, 4)
                ForEach(rows) { row in
                    rowView(row)
                }
            }
            .padding(10)
            .frame(maxWidth: .infinity, alignment: .leading)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background(RuneSurfaceBackground(kind: .editor))
    }

    private var header: some View {
        HStack(spacing: 12) {
            columnText("Revision", width: 72, weight: .semibold)
            Text("ReplicaSet")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
                .frame(minWidth: 180, maxWidth: .infinity, alignment: .leading)
            Text("Change Cause")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
                .frame(width: 160, alignment: .leading)
        }
        .padding(.bottom, 6)
    }

    private func rowView(_ row: DeploymentRolloutHistoryRow) -> some View {
        HStack(spacing: 12) {
            columnText(row.revision, width: 72, weight: .semibold)
            Text(row.replicaSet)
                .font(.caption.weight(.semibold))
                .lineLimit(1)
                .truncationMode(.middle)
                .textSelection(.enabled)
                .frame(minWidth: 180, maxWidth: .infinity, alignment: .leading)
            Text(row.changeCause.isEmpty ? "-" : row.changeCause)
                .font(.caption)
                .foregroundStyle(row.changeCause.isEmpty ? Color.secondary.opacity(0.55) : Color.secondary)
                .lineLimit(1)
                .truncationMode(.middle)
                .textSelection(.enabled)
                .frame(width: 160, alignment: .leading)
        }
        .padding(.vertical, 5)
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
