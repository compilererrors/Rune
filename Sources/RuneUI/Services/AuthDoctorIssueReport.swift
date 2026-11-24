import Foundation
import RuneCore

/// The failed and warning Auth Doctor checks as plain text, to paste into a ticket or a chat.
/// Each check keeps Rune's line and, under it, the error word for word.
enum AuthDoctorIssueReport {
    static func text(checks: [RuneHealthCheck], context: String?, namespace: String?) -> String? {
        let issues = checks.filter { $0.status == .failed || $0.status == .warning }
        guard !issues.isEmpty else { return nil }

        var scope = ["Auth Doctor"]
        if let context, !context.isEmpty { scope.append("context \(context)") }
        if let namespace, !namespace.isEmpty { scope.append("namespace \(namespace)") }

        var lines = [scope.joined(separator: ", ")]
        for check in issues {
            lines.append("")
            lines.append("\(check.status == .failed ? "Failed" : "Warning"): \(check.title)")
            lines.append(check.message)
            if let detail = check.detail {
                lines.append(contentsOf: detail.split(separator: "\n", omittingEmptySubsequences: false).map { "    \($0)" })
            }
        }
        return lines.joined(separator: "\n")
    }
}
