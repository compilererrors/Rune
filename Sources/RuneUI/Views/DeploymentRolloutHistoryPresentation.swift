import Foundation

struct DeploymentRolloutHistoryRow: Identifiable, Equatable {
    let revision: String
    let replicaSet: String
    let changeCause: String

    var id: String {
        "\(revision)|\(replicaSet)|\(changeCause)"
    }
}

enum DeploymentRolloutHistoryPresentation {
    static func rows(from rawHistory: String) -> [DeploymentRolloutHistoryRow] {
        rawHistory
            .split(whereSeparator: \.isNewline)
            .drop { line in
                line.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                    || line.trimmingCharacters(in: .whitespacesAndNewlines).uppercased().hasPrefix("REVISION")
            }
            .compactMap(row)
    }

    private static func row(from line: Substring) -> DeploymentRolloutHistoryRow? {
        let trimmed = line.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return nil }

        let parts = trimmed.split(maxSplits: 2, omittingEmptySubsequences: true) { $0 == " " || $0 == "\t" }
        guard parts.count >= 2 else { return nil }

        let parsedReplicaSet = String(parts[1])
        let replicaSet: String
        let changeCause: String
        if parts.count > 2 {
            replicaSet = parsedReplicaSet
            changeCause = String(parts[2]).trimmingCharacters(in: .whitespacesAndNewlines)
        } else if parsedReplicaSet.hasSuffix("<none>"), parsedReplicaSet.count > "<none>".count {
            replicaSet = String(parsedReplicaSet.dropLast("<none>".count))
            changeCause = "<none>"
        } else {
            replicaSet = parsedReplicaSet
            changeCause = ""
        }

        return DeploymentRolloutHistoryRow(
            revision: String(parts[0]),
            replicaSet: replicaSet,
            changeCause: changeCause
        )
    }
}
