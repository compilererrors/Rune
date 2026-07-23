import Foundation
import RuneCore

/// File-only lines (same gate as `DiagnosticsRecorder.trace`) for Kubernetes I/O — **never** logs bearer tokens.
public enum VerboseKubeTrace {
    private static let maxInputCharacters = 65_536

    public static func append(_ category: String, _ message: String) {
        guard UserDefaults.standard.runeVerboseDebugTrace else { return }
        DebugTraceWriter.append(
            category: privacySafeMessage(category),
            message: privacySafeMessage(message)
        )
    }

    /// Privacy-safe count of kubeconfig paths from `KUBECONFIG` (colon-separated).
    public static func kubeconfigSummary(_ environment: [String: String]) -> String {
        guard let raw = environment["KUBECONFIG"], !raw.isEmpty else {
            return "default-discovery"
        }
        let count = raw.split(separator: ":", omittingEmptySubsequences: true).count
        return count == 0 ? "default-discovery" : "configured(\(count))"
    }

    static func privacySafeMessage(_ message: String) -> String {
        var sanitized = String(message.prefix(maxInputCharacters))
            .replacingOccurrences(of: "\r", with: " ")
            .replacingOccurrences(of: "\n", with: " ")
            .replacingOccurrences(of: "\t", with: " ")

        sanitized = replacingFieldValues(
            in: sanitized,
            fieldNames: ["path", "apiPath"]
        ) { KubernetesAPIPathSanitizer.sanitizedPath($0) }

        sanitized = replacingIdentifierFieldValues(
            in: sanitized,
            fieldNames: ["context", "contextName"],
            followingFieldNames: [
                "namespace",
                "effectiveNamespace",
                "selectedNamespace",
                "method",
                "path",
                "apiPath",
                "server",
                "status",
                "attempt",
                "request",
                "triggerReload",
                "forceNamespaceMeta",
                "forceMeta",
                "pods",
                "deployments",
                "services",
                "section",
                "authorization",
                "token",
                "password",
                "error"
            ]
        ) { _ in "<redacted-context>" }

        sanitized = replacingIdentifierFieldValues(
            in: sanitized,
            fieldNames: [
                "namespace",
                "namespaceName",
                "effectiveNamespace",
                "selectedNamespace"
            ],
            followingFieldNames: [
                "context",
                "method",
                "path",
                "apiPath",
                "server",
                "status",
                "attempt",
                "request",
                "forceNamespaceMeta",
                "forceMeta",
                "pods",
                "deployments",
                "services",
                "section",
                "authorization",
                "token",
                "password",
                "error"
            ]
        ) { _ in "<redacted-namespace>" }

        sanitized = replacingFieldValues(
            in: sanitized,
            fieldNames: [
                "host",
                "server",
                "serverName",
                "serverTrust",
                "serverURL",
                "endpoint",
                "url",
                "failingURL"
            ]
        ) { _ in "<redacted-host>" }

        sanitized = replacingFieldValues(
            in: sanitized,
            fieldNames: ["query"]
        ) { _ in "<redacted-query>" }

        sanitized = replacingTailFieldValues(
            in: sanitized,
            fieldNames: ["error", "trustError", "underlying", "root"]
        )

        sanitized = replacingFieldValues(
            in: sanitized,
            fieldNames: [
                "authorization",
                "proxy-authorization",
                "token",
                "accessToken",
                "access_token",
                "id_token",
                "refresh_token",
                "password",
                "clientSecret",
                "client-secret",
                "client_secret",
                "secret",
                "credential",
                "credentials",
                "cookie",
                "set-cookie",
                "client-key-data",
                "client-certificate-data",
                "certificate-authority-data",
                "access-key",
                "secret-key",
                "headers"
            ]
        ) { _ in "<redacted>" }

        sanitized = replacingMatches(
            in: sanitized,
            pattern: #"\b(?:https?|wss?)://[^\s]+"#,
            template: "<redacted-url>",
            caseInsensitive: true
        )
        sanitized = replacingMatches(
            in: sanitized,
            pattern: #"\b(Bearer|Basic)\s+[A-Za-z0-9._~+/\-=]+"#,
            template: "$1 <redacted>",
            caseInsensitive: true
        )
        sanitized = replacingMatches(
            in: sanitized,
            pattern: #"\b(failed|failure|warnings?):\s+.*$"#,
            template: "$1: <redacted>",
            caseInsensitive: true
        )
        return sanitized
    }

    private static func replacingIdentifierFieldValues(
        in message: String,
        fieldNames: [String],
        followingFieldNames: [String],
        transform: (String) -> String
    ) -> String {
        let names = fieldNames
            .map(NSRegularExpression.escapedPattern(for:))
            .joined(separator: "|")
        let followingNames = followingFieldNames
            .map(NSRegularExpression.escapedPattern(for:))
            .joined(separator: "|")
        let pattern = #"(?<![A-Za-z0-9_.-])(?:\#(names))=(.*?)(?=\s+(?:\#(followingNames))=|$)"#
        guard let expression = try? NSRegularExpression(
            pattern: pattern,
            options: [.caseInsensitive]
        ) else {
            return message
        }

        var result = message
        let matches = expression.matches(
            in: message,
            range: NSRange(message.startIndex..., in: message)
        )
        for match in matches.reversed() {
            guard let valueRange = Range(match.range(at: 1), in: result) else { continue }
            result.replaceSubrange(valueRange, with: transform(String(result[valueRange])))
        }
        return result
    }

    private static func replacingTailFieldValues(
        in message: String,
        fieldNames: [String]
    ) -> String {
        let names = fieldNames
            .map(NSRegularExpression.escapedPattern(for:))
            .joined(separator: "|")
        return replacingMatches(
            in: message,
            pattern: #"(?<![A-Za-z0-9_.-])(\#(names))=.*$"#,
            template: "$1=<redacted>",
            caseInsensitive: true
        )
    }

    private static func replacingFieldValues(
        in message: String,
        fieldNames: [String],
        transform: (String) -> String
    ) -> String {
        let names = fieldNames
            .map(NSRegularExpression.escapedPattern(for:))
            .joined(separator: "|")
        let pattern = #"(?<![A-Za-z0-9_.-])(?:\#(names))=(.*?)(?=\s+[A-Za-z][A-Za-z0-9_.-]*=|$)"#
        guard let expression = try? NSRegularExpression(
            pattern: pattern,
            options: [.caseInsensitive]
        ) else {
            return message
        }

        var result = message
        let matches = expression.matches(
            in: message,
            range: NSRange(message.startIndex..., in: message)
        )
        for match in matches.reversed() {
            guard let valueRange = Range(match.range(at: 1), in: result) else { continue }
            let value = String(result[valueRange])
            result.replaceSubrange(valueRange, with: transform(value))
        }
        return result
    }

    private static func replacingMatches(
        in message: String,
        pattern: String,
        template: String,
        caseInsensitive: Bool
    ) -> String {
        let options: NSRegularExpression.Options = caseInsensitive ? [.caseInsensitive] : []
        guard let expression = try? NSRegularExpression(pattern: pattern, options: options) else {
            return message
        }
        return expression.stringByReplacingMatches(
            in: message,
            range: NSRange(message.startIndex..., in: message),
            withTemplate: template
        )
    }
}
