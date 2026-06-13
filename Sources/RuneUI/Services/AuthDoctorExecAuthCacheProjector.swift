import Foundation
import RuneCore
import RuneKube

public enum AuthDoctorExecAuthCacheProjector {
    public static func check(for diagnostic: KubernetesExecCredentialCacheDiagnostic) -> RuneHealthCheck {
        switch diagnostic.state {
        case .hit:
            return RuneHealthCheck(
                id: "exec-auth-cache",
                title: "Exec auth cache",
                status: .passed,
                message: hitMessage(expiresAt: diagnostic.expiresAt)
            )
        case .miss:
            return RuneHealthCheck(
                id: "exec-auth-cache",
                title: "Exec auth cache",
                status: .passed,
                message: "Exec auth is configured, but no reusable bearer token is cached. The plugin may return client certificate credentials or a non-cacheable response."
            )
        case .expired:
            return RuneHealthCheck(
                id: "exec-auth-cache",
                title: "Exec auth cache",
                status: .warning,
                message: expiredMessage(expiresAt: diagnostic.expiresAt)
            )
        }
    }

    private static func hitMessage(expiresAt: Date?) -> String {
        guard let expiresAt else {
            return "Exec credential token cache hit. Cached bearer token has no expiration timestamp."
        }
        return "Exec credential token cache hit. Cached bearer token is valid until \(timestamp(expiresAt))."
    }

    private static func expiredMessage(expiresAt: Date?) -> String {
        guard let expiresAt else {
            return "Cached exec credential token is expired or within the refresh window; the next API request will rerun the exec auth plugin."
        }
        return "Cached exec credential token expired or is within the refresh window at \(timestamp(expiresAt)); the next API request will rerun the exec auth plugin."
    }

    private static func timestamp(_ date: Date) -> String {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.string(from: date)
    }
}
