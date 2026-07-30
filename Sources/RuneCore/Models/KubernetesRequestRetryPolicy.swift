import Foundation

public enum KubernetesRequestRetryCategory: String, Codable, Sendable {
    case throttled
    case serverUnavailable
    case networkTransient
    case cancelled
    case notRetryable
}

public struct KubernetesRequestRetryDecision: Hashable, Codable, Sendable {
    public let isRetryable: Bool
    public let category: KubernetesRequestRetryCategory
    public let suggestedDelayNanoseconds: UInt64?

    public init(
        isRetryable: Bool,
        category: KubernetesRequestRetryCategory,
        suggestedDelayNanoseconds: UInt64? = nil
    ) {
        self.isRetryable = isRetryable
        self.category = category
        self.suggestedDelayNanoseconds = suggestedDelayNanoseconds
    }

    public var traceDescription: String {
        var parts = [
            "retryable=\(isRetryable)",
            "category=\(category.rawValue)"
        ]
        if let suggestedDelayNanoseconds {
            parts.append("suggestedDelayMs=\(suggestedDelayNanoseconds / 1_000_000)")
        }
        return parts.joined(separator: " ")
    }
}

public enum KubernetesRequestRetryPolicy {
    private static let defaultTransientDelayNanoseconds: UInt64 = 500_000_000
    private static let maximumRetryDelayNanoseconds: UInt64 = 2_000_000_000
    private static let maximumAttempts = 3

    public static func classifyHTTPStatus(
        _ statusCode: Int,
        retryAfterHeader: String? = nil
    ) -> KubernetesRequestRetryDecision {
        switch statusCode {
        case 429:
            return KubernetesRequestRetryDecision(
                isRetryable: true,
                category: .throttled,
                suggestedDelayNanoseconds: retryAfterDelayNanoseconds(from: retryAfterHeader)
            )
        case 500, 502, 503, 504:
            return KubernetesRequestRetryDecision(
                isRetryable: true,
                category: .serverUnavailable,
                suggestedDelayNanoseconds: retryAfterDelayNanoseconds(from: retryAfterHeader)
            )
        default:
            return KubernetesRequestRetryDecision(isRetryable: false, category: .notRetryable)
        }
    }

    public static func classifyNetworkError(_ error: Error) -> KubernetesRequestRetryDecision {
        let nsError = error as NSError
        guard nsError.domain == NSURLErrorDomain else {
            return KubernetesRequestRetryDecision(isRetryable: false, category: .notRetryable)
        }

        switch URLError.Code(rawValue: nsError.code) {
        case .cancelled:
            return KubernetesRequestRetryDecision(isRetryable: false, category: .cancelled)
        case .timedOut,
             .cannotFindHost,
             .cannotConnectToHost,
             .networkConnectionLost,
             .dnsLookupFailed,
             .notConnectedToInternet,
             .secureConnectionFailed,
             .serverCertificateHasBadDate,
             .serverCertificateUntrusted,
             .serverCertificateHasUnknownRoot,
             .serverCertificateNotYetValid,
             .clientCertificateRejected,
             .clientCertificateRequired:
            return KubernetesRequestRetryDecision(
                isRetryable: true,
                category: .networkTransient
            )
        default:
            return KubernetesRequestRetryDecision(isRetryable: false, category: .notRetryable)
        }
    }

    public static func shouldRetry(
        method: String,
        decision: KubernetesRequestRetryDecision,
        attempt: Int
    ) -> Bool {
        guard decision.isRetryable,
              isSafeRetryMethod(method),
              attempt > 0,
              attempt < maximumAttempts
        else {
            return false
        }

        return true
    }

    public static func isSafeRetryMethod(_ method: String) -> Bool {
        switch method.uppercased() {
        case "GET", "HEAD":
            return true
        default:
            return false
        }
    }

    public static func boundedDelayNanoseconds(
        for decision: KubernetesRequestRetryDecision,
        attempt: Int
    ) -> UInt64 {
        if let suggestedDelayNanoseconds = decision.suggestedDelayNanoseconds {
            return min(suggestedDelayNanoseconds, maximumRetryDelayNanoseconds)
        }
        let multiplierShift = max(0, min(attempt - 1, 2))
        let multiplier = UInt64(1 << multiplierShift)
        return min(defaultTransientDelayNanoseconds * multiplier, maximumRetryDelayNanoseconds)
    }

    private static func retryAfterDelayNanoseconds(from header: String?) -> UInt64? {
        guard let header else { return nil }
        let trimmed = header.trimmingCharacters(in: .whitespacesAndNewlines)
        if let seconds = Double(trimmed), seconds >= 0 {
            return UInt64(seconds * 1_000_000_000)
        }
        return nil
    }
}
