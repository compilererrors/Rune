import Foundation
import os
import RuneCore
import Security

/// Performs HTTPS GETs against the Kubernetes API server (same paths as `kubectl get --raw`).
final class KubernetesAPISessionTransport: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    private let credentials: KubeRESTCredentials
    private let execRunner: CommandRunning?
    private let baseEnvironment: [String: String]
    private let execTimeout: TimeInterval

    private let authState: OSAllocatedUnfairLock<(bearer: String?, expiry: Date?)>

    private lazy var clientIdentity: SecIdentity? = {
        guard let c = credentials.clientCertificatePEM,
              let k = credentials.clientPrivateKeyPEM else { return nil }
        return KubeTLSIdentity.makeIdentity(certPEM: c, keyPEM: k)
    }()

    private lazy var session: URLSession = {
        let config = URLSessionConfiguration.ephemeral
        config.timeoutIntervalForRequest = 90
        config.timeoutIntervalForResource = 120
        return URLSession(configuration: config, delegate: self, delegateQueue: nil)
    }()

    init(
        credentials: KubeRESTCredentials,
        execRunner: CommandRunning?,
        baseEnvironment: [String: String],
        execTimeout: TimeInterval
    ) {
        self.credentials = credentials
        self.execRunner = execRunner
        self.baseEnvironment = baseEnvironment
        self.execTimeout = execTimeout
        self.authState = OSAllocatedUnfairLock(initialState: (credentials.bearerToken, credentials.tokenExpiry))
    }

    func getString(pathAndQuery: String) async throws -> String {
        try await ensureFreshTokenIfNeeded()

        guard let url = Self.absoluteURL(server: credentials.serverURL, pathAndQuery: pathAndQuery) else {
            throw RuneError.parseError(message: "Ogiltig API-URL")
        }
        var request = URLRequest(url: url)
        request.httpMethod = "GET"
        request.setValue("application/json", forHTTPHeaderField: "Accept")

        let bearer = authState.withLock { $0.bearer ?? credentials.bearerToken }

        if let token = bearer, !token.isEmpty {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }

        let (data, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse else {
            throw RuneError.parseError(message: "API-svar saknar HTTP-svar")
        }
        guard (200 ... 299).contains(http.statusCode) else {
            let body = String(data: data, encoding: .utf8) ?? ""
            throw RuneError.commandFailed(
                command: "GET \(url.path)",
                message: "HTTP \(http.statusCode) \(body.prefix(500))"
            )
        }
        return String(data: data, encoding: .utf8) ?? ""
    }

    private func ensureFreshTokenIfNeeded() async throws {
        guard let exec = credentials.execPluginForRefresh, let runner = execRunner else { return }

        let (bearer, expiry) = authState.withLock { ($0.bearer, $0.expiry) }

        if let b = bearer, !b.isEmpty {
            if let e = expiry {
                if e > Date().addingTimeInterval(60) { return }
            } else {
                return
            }
        }

        let fetched = try await KubeExecCredentialRunner.fetchToken(
            exec: exec,
            baseEnvironment: baseEnvironment,
            runner: runner,
            timeout: execTimeout
        )
        authState.withLock {
            $0.bearer = fetched.token
            $0.expiry = fetched.expiry
        }
    }

    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        didReceive challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        handleAuthenticationChallenge(challenge, completionHandler: completionHandler)
    }

    func urlSession(
        _ session: URLSession,
        didReceive challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        handleAuthenticationChallenge(challenge, completionHandler: completionHandler)
    }

    private func handleAuthenticationChallenge(
        _ challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        if challenge.protectionSpace.authenticationMethod == NSURLAuthenticationMethodClientCertificate {
            if let identity = clientIdentity {
                completionHandler(.useCredential, URLCredential(identity: identity, certificates: nil, persistence: .none))
            } else {
                completionHandler(.performDefaultHandling, nil)
            }
            return
        }

        guard challenge.protectionSpace.authenticationMethod == NSURLAuthenticationMethodServerTrust,
              let serverTrust = challenge.protectionSpace.serverTrust else {
            completionHandler(.performDefaultHandling, nil)
            return
        }

        if credentials.insecureSkipTLSVerify {
            completionHandler(.useCredential, URLCredential(trust: serverTrust))
            return
        }

        if credentials.anchorCertificateDER == nil {
            completionHandler(.performDefaultHandling, nil)
            return
        }

        if let der = credentials.anchorCertificateDER,
           let anchor = SecCertificateCreateWithData(nil, der as CFData) {
            SecTrustSetAnchorCertificates(serverTrust, [anchor] as CFArray)
            SecTrustSetAnchorCertificatesOnly(serverTrust, true)
        }

        var error: CFError?
        if SecTrustEvaluateWithError(serverTrust, &error) {
            completionHandler(.useCredential, URLCredential(trust: serverTrust))
        } else {
            completionHandler(.cancelAuthenticationChallenge, nil)
        }
    }

    private static func absoluteURL(server: URL, pathAndQuery: String) -> URL? {
        let trimmed = pathAndQuery.hasPrefix("/") ? pathAndQuery : "/\(pathAndQuery)"
        return URL(string: trimmed, relativeTo: server)?.absoluteURL
    }
}

enum KubeRESTConfigViewLoader {
    static func loadJSON(
        contextName: String,
        environment: [String: String],
        runner: CommandRunning,
        kubectlExecutable: String,
        timeout: TimeInterval
    ) async throws -> KubeConfigViewJSON {
        let builder = KubectlCommandBuilder()
        let result = try await runner.run(
            executable: kubectlExecutable,
            arguments: ["kubectl"] + builder.configViewJSONArguments(context: contextName),
            environment: environment,
            timeout: timeout
        )
        guard result.exitCode == 0 else {
            throw RuneError.commandFailed(
                command: "kubectl config view",
                message: result.stderr.isEmpty ? "exit \(result.exitCode)" : result.stderr
            )
        }
        let data = Data(result.stdout.utf8)
        return try JSONDecoder().decode(KubeConfigViewJSON.self, from: data)
    }
}
