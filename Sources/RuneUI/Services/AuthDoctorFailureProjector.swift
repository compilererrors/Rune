import Foundation
import RuneCore
import RuneKube

public enum AuthDoctorFailureProjector {
    private static let maximumDiagnosticCharacters = 192
    private static let nativeAuthChecks = [
        RuneHealthCheck(
            id: "native-auth-profile",
            title: "Native authentication",
            status: .failed,
            message: "The selected context needs a connected native provider profile. Open Add Cluster, connect the matching provider credentials, and retry."
        )
    ]
    private static let authorizationChecks = [
        RuneHealthCheck(
            id: "api-authorization",
            title: "API authorization",
            status: .warning,
            message: "The Kubernetes API returned an authorization failure. The context is reachable, but the active identity is missing permission for this request."
        )
    ]
    private static let authenticationChecks = [
        RuneHealthCheck(
            id: "api-auth",
            title: "API auth",
            status: .failed,
            message: "The Kubernetes API rejected the configured credentials. Refresh login credentials or review the selected kubeconfig user."
        )
    ]
    private static let clientCertificateChecks = [
        RuneHealthCheck(
            id: "client-certificate-auth",
            title: "Client certificate auth",
            status: .failed,
            message: "Kubeconfig client certificate authentication failed. Review the client certificate and key pair for the selected user, or refresh the kubeconfig credentials."
        )
    ]
    private static let tlsChecks = [
        RuneHealthCheck(
            id: "transport",
            title: "API transport",
            status: .failed,
            message: "TLS or custom CA verification failed while connecting to the Kubernetes API server."
        )
    ]
    private static let proxyChecks = [
        RuneHealthCheck(
            id: "transport",
            title: "API transport",
            status: .failed,
            message: "Proxy routing failed while connecting to the Kubernetes API server."
        )
    ]
    private static let connectivityChecks = [
        RuneHealthCheck(
            id: "transport",
            title: "API transport",
            status: .failed,
            message: "Rune could not reach the Kubernetes API server. Check DNS, network connectivity, VPN, or cluster endpoint availability."
        )
    ]

    public static func checks(for errorMessage: String) -> [RuneHealthCheck] {
        let trimmed = boundedTrimmedErrorMessage(errorMessage)
        guard !trimmed.isEmpty else { return [] }
        let lower = trimmed.lowercased()
        if isNativeAuthFailure(lower) {
            return withEndpointDiagnostic(trimmed, checks: nativeAuthChecks)
        }

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
            return withEndpointDiagnostic(trimmed, checks: authorizationChecks)
        }

        if isAuthenticationFailure(lower) {
            return withEndpointDiagnostic(trimmed, checks: authenticationChecks)
        }

        if isClientCertificateAuthFailure(lower) {
            return withEndpointDiagnostic(trimmed, checks: clientCertificateChecks)
        }

        if isTLSFailure(lower) {
            return withEndpointDiagnostic(trimmed, checks: tlsChecks)
        }

        if isProxyFailure(lower) {
            return withEndpointDiagnostic(trimmed, checks: proxyChecks)
        }

        if isConnectivityFailure(lower) {
            return withEndpointDiagnostic(trimmed, checks: connectivityChecks)
        }

        return withEndpointDiagnostic(trimmed, checks: [])
    }

    private static func boundedTrimmedErrorMessage(_ message: String) -> String {
        let bounded: String
        if let limit = message.index(
            message.startIndex,
            offsetBy: maximumDiagnosticCharacters,
            limitedBy: message.endIndex
        ), limit != message.endIndex {
            bounded = String(message[..<limit])
        } else {
            bounded = message
        }
        guard let first = bounded.first, let last = bounded.last else { return "" }
        guard first.isWhitespace || last.isWhitespace else { return bounded }
        return bounded.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func isExecAuthFailure(_ text: String) -> Bool {
        guard contains(text, "exec") || contains(text, "credential plugin") else {
            return false
        }
        return contains(text, "execcredential")
            || contains(text, "exec credential")
            || contains(text, "exec auth")
            || contains(text, "exec plugin")
            || contains(text, "cli-backed auth")
            || contains(text, "credential plugin")
            || contains(text, "kubeconfig exec")
            || contains(text, "executable file not found") && contains(text, "exec")
            || contains(text, "no such file or directory") && contains(text, "exec")
    }

    private static func isNativeAuthFailure(_ text: String) -> Bool {
        guard contains(text, "native ") || contains(text, "connect ") else {
            return false
        }
        return contains(text, "native kubernetes authentication")
            || contains(text, "native authentication profile")
            || contains(text, "connect amazon eks credentials")
            || contains(text, "connect microsoft aks credentials")
            || contains(text, "connect google gke credentials")
            || contains(text, "connect oidc credentials")
    }

    private static func execAuthMessage(_ text: String) -> String {
        if contains(text, "cli-backed auth") || contains(text, "disabled in this rune build") {
            return RuneExternalCommandPolicy.disabledMessage
        }

        if contains(text, "timed out") || contains(text, "timeout") {
            return "Kubeconfig exec authentication timed out. Refresh provider login state or check whether the credential plugin is waiting on network or interactive input."
        }

        if contains(text, "not a valid execcredential json")
            || contains(text, "invalid json")
            || contains(text, "valid execcredential json") {
            return "Kubeconfig exec authentication returned invalid ExecCredential JSON. Update or reconfigure the credential plugin."
        }

        if contains(text, "returned apiversion") && contains(text, "expected") {
            return "Kubeconfig exec authentication returned an ExecCredential API version that does not match the kubeconfig entry."
        }

        if contains(text, "missing status") {
            return "Kubeconfig exec authentication returned an ExecCredential without status credentials."
        }

        if contains(text, "incomplete client certificate")
            || contains(text, "missing token")
            || contains(text, "missing client certificate")
            || contains(text, "missing client key") {
            return "Kubeconfig exec authentication returned incomplete credentials. The plugin must return a token or a complete client certificate and key."
        }

        if contains(text, "executable file not found")
            || contains(text, "no such file or directory")
            || contains(text, "not found") && contains(text, "kubeconfig exec auth") {
            return "Kubeconfig exec authentication command could not be started. Install the provider CLI or fix the exec command path in kubeconfig."
        }

        if contains(text, "command failed: kubeconfig exec auth") {
            return "Kubeconfig exec authentication command returned an error. Refresh provider login state or review the credential plugin configuration."
        }

        return "Kubeconfig exec authentication failed before Rune could complete the API request. Check the provider login or credential plugin."
    }

    private static func isAuthenticationFailure(_ text: String) -> Bool {
        contains(text, " 401")
            || contains(text, "status 401")
            || contains(text, "unauthorized")
            || contains(text, "invalid bearer")
            || contains(text, "bearer token")
            || contains(text, "basic auth")
            || contains(text, "authentication")
    }

    private static func isClientCertificateAuthFailure(_ text: String) -> Bool {
        guard contains(text, "client certificate") || contains(text, "client-certificate") else {
            return false
        }
        return contains(text, "client certificate and key")
            || contains(text, "client-certificate challenge")
            || contains(text, "client tls")
            || contains(text, "tls identity")
            || contains(text, "no-client-identity")
            || contains(text, "mtls")
    }

    private static func isAuthorizationFailure(_ text: String) -> Bool {
        contains(text, " 403")
            || contains(text, "status 403")
            || contains(text, "forbidden")
            || contains(text, "cannot list resource")
            || contains(text, "cannot get resource")
            || contains(text, "rbac")
    }

    private static func isTLSFailure(_ text: String) -> Bool {
        contains(text, "tls")
            || contains(text, "ssl")
            || contains(text, "certificate")
            || contains(text, "cert authority")
            || contains(text, "custom ca")
            || contains(text, "sec trust")
            || contains(text, "server trust")
            || contains(text, "x509")
    }

    private static func isProxyFailure(_ text: String) -> Bool {
        contains(text, "proxy")
            || contains(text, "407")
            || contains(text, "tunnel")
    }

    private static func isConnectivityFailure(_ text: String) -> Bool {
        contains(text, "timed out")
            || contains(text, "timeout")
            || contains(text, "dns")
            || contains(text, "could not connect")
            || contains(text, "connection refused")
            || contains(text, "connection reset")
            || contains(text, "network connection was lost")
            || contains(text, "not connected to the internet")
            || contains(text, "could not resolve")
            || contains(text, "name or service not known")
            || contains(text, "host is down")
            || contains(text, "no route to host")
    }

    @inline(__always)
    private static func contains(_ lower: String, _ needle: String) -> Bool {
        (lower as NSString).range(of: needle).location != NSNotFound
    }

    private static func withEndpointDiagnostic(_ message: String, checks: [RuneHealthCheck]) -> [RuneHealthCheck] {
        // API paths and URLs always contain a slash. The byte preflight avoids several
        // comparatively expensive substring searches for ordinary auth/transport errors.
        guard message.utf8.contains(0x2F) else { return checks }
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
