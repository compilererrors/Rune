import Foundation
import RuneCore
import RuneKube

public enum AuthDoctorFailureProjector {
    public static func checks(for errorMessage: String) -> [RuneHealthCheck] {
        let trimmed = errorMessage.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return [] }
        let lower = trimmed.lowercased()

        if isExecAuthFailure(lower) {
            return withEndpointDiagnostic(trimmed, checks: [
                RuneHealthCheck(
                    id: "exec-auth",
                    title: "Exec auth",
                    status: .failed,
                    message: execAuthMessage(lower)
                )
            ])
        }

        if isAuthorizationFailure(lower) {
            return withEndpointDiagnostic(trimmed, checks: [
                RuneHealthCheck(
                    id: "api-authorization",
                    title: "API authorization",
                    status: .warning,
                    message: "The Kubernetes API returned an authorization failure. The context is reachable, but the active identity is missing permission for this request."
                )
            ])
        }

        if isAuthenticationFailure(lower) {
            return withEndpointDiagnostic(trimmed, checks: [
                RuneHealthCheck(
                    id: "api-auth",
                    title: "API auth",
                    status: .failed,
                    message: "The Kubernetes API rejected the configured credentials. Refresh login credentials or review the selected kubeconfig user."
                )
            ])
        }

        if isClientCertificateAuthFailure(lower) {
            return withEndpointDiagnostic(trimmed, checks: [
                RuneHealthCheck(
                    id: "client-certificate-auth",
                    title: "Client certificate auth",
                    status: .failed,
                    message: "Kubeconfig client certificate authentication failed. Review the client certificate and key pair for the selected user, or refresh the kubeconfig credentials."
                )
            ])
        }

        if isTLSFailure(lower) {
            return withEndpointDiagnostic(trimmed, checks: [
                RuneHealthCheck(
                    id: "transport",
                    title: "API transport",
                    status: .failed,
                    message: "TLS or custom CA verification failed while connecting to the Kubernetes API server."
                )
            ])
        }

        if isProxyFailure(lower) {
            return withEndpointDiagnostic(trimmed, checks: [
                RuneHealthCheck(
                    id: "transport",
                    title: "API transport",
                    status: .failed,
                    message: "Proxy routing failed while connecting to the Kubernetes API server."
                )
            ])
        }

        if isConnectivityFailure(lower) {
            return withEndpointDiagnostic(trimmed, checks: [
                RuneHealthCheck(
                    id: "transport",
                    title: "API transport",
                    status: .failed,
                    message: "Rune could not reach the Kubernetes API server. Check DNS, network connectivity, VPN, or cluster endpoint availability."
                )
            ])
        }

        return withEndpointDiagnostic(trimmed, checks: [])
    }

    private static func isExecAuthFailure(_ lower: String) -> Bool {
        lower.contains("execcredential")
            || lower.contains("exec credential")
            || lower.contains("exec auth")
            || lower.contains("credential plugin")
            || lower.contains("kubeconfig exec")
            || lower.contains("executable file not found") && lower.contains("exec")
            || lower.contains("no such file or directory") && lower.contains("exec")
    }

    private static func execAuthMessage(_ lower: String) -> String {
        if lower.contains("timed out") || lower.contains("timeout") {
            return "Kubeconfig exec authentication timed out. Refresh provider login state or check whether the credential plugin is waiting on network or interactive input."
        }

        if lower.contains("not a valid execcredential json")
            || lower.contains("invalid json")
            || lower.contains("valid execcredential json") {
            return "Kubeconfig exec authentication returned invalid ExecCredential JSON. Update or reconfigure the credential plugin."
        }

        if lower.contains("returned apiversion") && lower.contains("expected") {
            return "Kubeconfig exec authentication returned an ExecCredential API version that does not match the kubeconfig entry."
        }

        if lower.contains("missing status") {
            return "Kubeconfig exec authentication returned an ExecCredential without status credentials."
        }

        if lower.contains("incomplete client certificate")
            || lower.contains("missing token")
            || lower.contains("missing client certificate")
            || lower.contains("missing client key") {
            return "Kubeconfig exec authentication returned incomplete credentials. The plugin must return a token or a complete client certificate and key."
        }

        if lower.contains("executable file not found")
            || lower.contains("no such file or directory")
            || lower.contains("not found") && lower.contains("kubeconfig exec auth") {
            return "Kubeconfig exec authentication command could not be started. Install the provider CLI or fix the exec command path in kubeconfig."
        }

        if lower.contains("command failed: kubeconfig exec auth") {
            return "Kubeconfig exec authentication command returned an error. Refresh provider login state or review the credential plugin configuration."
        }

        return "Kubeconfig exec authentication failed before Rune could complete the API request. Check the provider login or credential plugin."
    }

    private static func isAuthenticationFailure(_ lower: String) -> Bool {
        lower.contains(" 401")
            || lower.contains("status 401")
            || lower.contains("unauthorized")
            || lower.contains("invalid bearer")
            || lower.contains("bearer token")
            || lower.contains("basic auth")
            || lower.contains("authentication")
    }

    private static func isClientCertificateAuthFailure(_ lower: String) -> Bool {
        (lower.contains("client certificate") || lower.contains("client-certificate"))
            && (
                lower.contains("client certificate and key")
                    || lower.contains("client-certificate challenge")
                    || lower.contains("client tls")
                    || lower.contains("tls identity")
                    || lower.contains("no-client-identity")
                    || lower.contains("mtls")
            )
    }

    private static func isAuthorizationFailure(_ lower: String) -> Bool {
        lower.contains(" 403")
            || lower.contains("status 403")
            || lower.contains("forbidden")
            || lower.contains("cannot list resource")
            || lower.contains("cannot get resource")
            || lower.contains("rbac")
    }

    private static func isTLSFailure(_ lower: String) -> Bool {
        lower.contains("tls")
            || lower.contains("ssl")
            || lower.contains("certificate")
            || lower.contains("cert authority")
            || lower.contains("custom ca")
            || lower.contains("sec trust")
            || lower.contains("server trust")
            || lower.contains("x509")
    }

    private static func isProxyFailure(_ lower: String) -> Bool {
        lower.contains("proxy")
            || lower.contains("407")
            || lower.contains("tunnel")
    }

    private static func isConnectivityFailure(_ lower: String) -> Bool {
        lower.contains("could not connect")
            || lower.contains("connection refused")
            || lower.contains("connection reset")
            || lower.contains("network connection was lost")
            || lower.contains("not connected to the internet")
            || lower.contains("dns")
            || lower.contains("could not resolve")
            || lower.contains("name or service not known")
            || lower.contains("timed out")
            || lower.contains("timeout")
            || lower.contains("host is down")
            || lower.contains("no route to host")
    }

    private static func withEndpointDiagnostic(_ message: String, checks: [RuneHealthCheck]) -> [RuneHealthCheck] {
        guard let endpoint = sanitizedAPIEndpoint(in: message) else { return checks }
        return checks + [
            RuneHealthCheck(
                id: "api-request-endpoint",
                title: "Failed API request",
                status: .warning,
                message: "Failed Kubernetes API request: \(endpoint)."
            )
        ]
    }

    private static func sanitizedAPIEndpoint(in message: String) -> String? {
        guard message.contains("/api") || message.contains("http://") || message.contains("https://") else {
            return nil
        }

        for rawToken in message.split(whereSeparator: \.isWhitespace) {
            let token = String(rawToken)
                .trimmingCharacters(in: CharacterSet(charactersIn: "\"'`()[]{}<>,."))

            if let endpoint = endpointFromURLToken(token) {
                return KubernetesRESTRequestMetric.sanitizedAPIPath(endpoint)
            }

            if let endpoint = endpointFromPathToken(token) {
                return KubernetesRESTRequestMetric.sanitizedAPIPath(endpoint)
            }
        }
        return nil
    }

    private static func endpointFromURLToken(_ token: String) -> String? {
        guard token.hasPrefix("http://") || token.hasPrefix("https://"),
              let components = URLComponents(string: token),
              components.path.hasPrefix("/api") else {
            return nil
        }
        return components.path + (components.percentEncodedQuery.map { "?\($0)" } ?? "")
    }

    private static func endpointFromPathToken(_ token: String) -> String? {
        guard let range = token.range(of: "/api") else { return nil }
        let path = String(token[range.lowerBound...])
        guard path.hasPrefix("/api/") || path.hasPrefix("/apis/") else { return nil }
        return path
    }
}
