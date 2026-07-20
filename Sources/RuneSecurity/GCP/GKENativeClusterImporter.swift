import Foundation

public struct GKENativeClusterImportRequest: Equatable, Sendable {
    public let projectID: String
    public let location: String
    public let clusterName: String

    public init(projectID: String, location: String, clusterName: String) {
        self.projectID = projectID
        self.location = location
        self.clusterName = clusterName
    }
}

public struct GKENativeClusterImportResult: Equatable, Sendable {
    public let rawKubeConfig: String
    public let sourceName: String

    public init(rawKubeConfig: String, sourceName: String) {
        self.rawKubeConfig = rawKubeConfig
        self.sourceName = sourceName
    }
}

public struct GKEClusterHTTPRequest: Equatable, Sendable, CustomStringConvertible, CustomDebugStringConvertible {
    public let url: URL
    public let method: String
    public let headers: [String: String]

    public init(url: URL, method: String, headers: [String: String]) {
        self.url = url
        self.method = method
        self.headers = headers
    }

    public var description: String { "GKEClusterHTTPRequest(<redacted>)" }
    public var debugDescription: String { description }
}

public struct GKEClusterHTTPResponse: Equatable, Sendable {
    public let statusCode: Int
    public let headers: [String: String]
    public let body: Data

    public init(statusCode: Int, headers: [String: String] = [:], body: Data) {
        self.statusCode = statusCode
        self.headers = headers
        self.body = body
    }
}

public protocol GKEClusterHTTPClient: Sendable {
    func send(_ request: GKEClusterHTTPRequest) async throws -> GKEClusterHTTPResponse
}

public enum GKENativeClusterImportError: Error, LocalizedError, Equatable, Sendable {
    case missingRequiredField(String)
    case invalidResourceIdentifier(String)
    case authenticationFailed
    case networkFailure
    case invalidHTTPResponse
    case responseTooLarge
    case requestRejected(Int)
    case invalidClusterResponse
    case invalidClusterEndpoint
    case invalidCertificateAuthority

    public var errorDescription: String? {
        switch self {
        case let .missingRequiredField(field):
            return "Google GKE import requires \(Self.safeField(field))."
        case let .invalidResourceIdentifier(field):
            return "Google GKE import contains an invalid \(Self.safeField(field))."
        case .authenticationFailed:
            return "Rune could not authenticate the Google service account."
        case .networkFailure:
            return "Rune could not reach the Google Kubernetes Engine API."
        case .invalidHTTPResponse:
            return "Google Kubernetes Engine returned an invalid HTTP response."
        case .responseTooLarge:
            return "The Google Kubernetes Engine response exceeded the supported size."
        case let .requestRejected(statusCode):
            let safeStatus = (100...599).contains(statusCode) ? statusCode : 0
            return "Google Kubernetes Engine rejected the cluster request (HTTP \(safeStatus))."
        case .invalidClusterResponse:
            return "Google Kubernetes Engine returned incomplete cluster connection information."
        case .invalidClusterEndpoint:
            return "Google Kubernetes Engine returned an invalid cluster endpoint."
        case .invalidCertificateAuthority:
            return "Google Kubernetes Engine returned an invalid cluster certificate authority."
        }
    }

    private static func safeField(_ value: String) -> String {
        let normalized = value
            .unicodeScalars
            .filter { CharacterSet.alphanumerics.union(CharacterSet(charactersIn: " -_")).contains($0) }
        let result = String(String.UnicodeScalarView(normalized)).prefix(64)
        return result.isEmpty ? "field" : String(result)
    }
}

/// URLSession transport for the fixed Google Kubernetes Engine REST origin.
/// Redirects are rejected so a bearer token is never forwarded to another URL.
public final class GKEClusterURLSessionHTTPClient: GKEClusterHTTPClient, @unchecked Sendable {
    private static let maximumResponseBytes = 1_048_576
    private static let allowedHost = "container.googleapis.com"

    private let sessionConfiguration: URLSessionConfiguration

    public init() {
        sessionConfiguration = .ephemeral
    }

    /// Test-only transport seam. Production callers use `init()`.
    init(sessionConfiguration: URLSessionConfiguration) {
        self.sessionConfiguration = sessionConfiguration
    }

    public func send(_ request: GKEClusterHTTPRequest) async throws -> GKEClusterHTTPResponse {
        guard Self.isAllowedAPIURL(request.url), request.method == "GET" else {
            throw GKENativeClusterImportError.invalidHTTPResponse
        }

        var urlRequest = URLRequest(url: request.url)
        urlRequest.httpMethod = request.method
        urlRequest.timeoutInterval = 20
        for (name, value) in request.headers {
            guard !name.contains("\r"), !name.contains("\n"),
                  !value.contains("\r"), !value.contains("\n") else {
                throw GKENativeClusterImportError.invalidHTTPResponse
            }
            urlRequest.setValue(value, forHTTPHeaderField: name)
        }

        let configuration = sessionConfiguration.copy() as! URLSessionConfiguration
        configuration.waitsForConnectivity = false
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        configuration.urlCache = nil
        configuration.httpCookieStorage = nil
        configuration.httpShouldSetCookies = false
        let delegate = GKEClusterNoRedirectDelegate()
        let session = URLSession(configuration: configuration, delegate: delegate, delegateQueue: nil)
        defer { session.invalidateAndCancel() }

        do {
            let (bytes, response) = try await session.bytes(for: urlRequest)
            guard let httpResponse = response as? HTTPURLResponse else {
                throw GKENativeClusterImportError.invalidHTTPResponse
            }
            if let rawLength = httpResponse.value(forHTTPHeaderField: "Content-Length"),
               let contentLength = Int(rawLength),
               contentLength > Self.maximumResponseBytes {
                throw GKENativeClusterImportError.responseTooLarge
            }

            var data = Data()
            data.reserveCapacity(min(Self.maximumResponseBytes, 64 * 1_024))
            for try await byte in bytes {
                try Task.checkCancellation()
                guard data.count < Self.maximumResponseBytes else {
                    throw GKENativeClusterImportError.responseTooLarge
                }
                data.append(byte)
            }

            var headers: [String: String] = [:]
            for (key, value) in httpResponse.allHeaderFields {
                guard let key = key as? String, let value = value as? String else { continue }
                headers[key] = value
            }
            return GKEClusterHTTPResponse(
                statusCode: httpResponse.statusCode,
                headers: headers,
                body: data
            )
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as URLError where error.code == .cancelled {
            throw CancellationError()
        } catch let error as GKENativeClusterImportError {
            throw error
        } catch {
            throw GKENativeClusterImportError.networkFailure
        }
    }

    private static func isAllowedAPIURL(_ url: URL) -> Bool {
        guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
              components.scheme?.lowercased() == "https",
              components.host?.lowercased() == allowedHost,
              components.port == nil || components.port == 443,
              components.user == nil,
              components.password == nil,
              components.fragment == nil,
              components.path.hasPrefix("/v1/projects/") else {
            return false
        }
        return true
    }
}

private final class GKEClusterNoRedirectDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    func urlSession(
        _: URLSession,
        task _: URLSessionTask,
        willPerformHTTPRedirection _: HTTPURLResponse,
        newRequest _: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        completionHandler(nil)
    }
}

/// Fetches GKE connection metadata and creates a kubeconfig that Rune can
/// authenticate natively without executing gcloud or gke-gcloud-auth-plugin.
public struct GKENativeClusterImporter: Sendable {
    private static let maximumResponseBytes = 1_048_576
    private static let maximumCertificateAuthorityBytes = 262_144

    private let httpClient: any GKEClusterHTTPClient
    private let oauthHTTPClient: any GCPServiceAccountHTTPClient

    public init(
        httpClient: any GKEClusterHTTPClient = GKEClusterURLSessionHTTPClient(),
        oauthHTTPClient: any GCPServiceAccountHTTPClient = GCPServiceAccountURLSessionHTTPClient()
    ) {
        self.httpClient = httpClient
        self.oauthHTTPClient = oauthHTTPClient
    }

    public func importCluster(
        _ request: GKENativeClusterImportRequest,
        serviceAccountJSON: Data
    ) async throws -> GKENativeClusterImportResult {
        let validated = try ValidatedRequest(request)
        try Task.checkCancellation()

        let cancellationTrackingOAuthClient = GKECancellationTrackingOAuthHTTPClient(
            base: oauthHTTPClient
        )
        let credentialProvider: GCPServiceAccountCredentialProvider
        do {
            credentialProvider = try GCPServiceAccountCredentialProvider(
                serviceAccountJSON: serviceAccountJSON,
                httpClient: cancellationTrackingOAuthClient
            )
        } catch let error as GCPServiceAccountAuthError {
            throw error
        } catch {
            throw GKENativeClusterImportError.authenticationFailed
        }

        let token: GCPServiceAccountAccessToken
        do {
            token = try await withTaskCancellationHandler {
                try await credentialProvider.accessToken()
            } onCancel: {
                Task { await credentialProvider.invalidate() }
            }
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as GCPServiceAccountAuthError {
            let cancellationWasObserved = await cancellationTrackingOAuthClient.cancellationWasObserved()
            if Task.isCancelled || cancellationWasObserved {
                throw CancellationError()
            }
            throw error
        } catch {
            let cancellationWasObserved = await cancellationTrackingOAuthClient.cancellationWasObserved()
            if Task.isCancelled || cancellationWasObserved {
                throw CancellationError()
            }
            throw GKENativeClusterImportError.authenticationFailed
        }
        try Task.checkCancellation()

        let apiRequest = try Self.clusterRequest(for: validated, bearerToken: token.value)
        let response: GKEClusterHTTPResponse
        do {
            response = try await httpClient.send(apiRequest)
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as GKENativeClusterImportError {
            if Task.isCancelled { throw CancellationError() }
            throw error
        } catch {
            if Task.isCancelled { throw CancellationError() }
            throw GKENativeClusterImportError.networkFailure
        }
        try Task.checkCancellation()

        guard response.body.count <= Self.maximumResponseBytes else {
            throw GKENativeClusterImportError.responseTooLarge
        }
        guard (200...299).contains(response.statusCode) else {
            if response.statusCode == 401 || response.statusCode == 403 {
                throw GKENativeClusterImportError.authenticationFailed
            }
            throw GKENativeClusterImportError.requestRejected(response.statusCode)
        }

        let cluster: ClusterResponse
        do {
            cluster = try JSONDecoder().decode(ClusterResponse.self, from: response.body)
        } catch {
            throw GKENativeClusterImportError.invalidClusterResponse
        }
        let endpoint: String
        let certificateAuthority: String?
        if let rawIPEndpoint = Self.nonEmpty(cluster.endpoint) {
            endpoint = try Self.validatedClusterEndpoint(rawIPEndpoint, requireGKEDNSName: false)
            certificateAuthority = try Self.validatedCertificateAuthority(
                cluster.masterAuth?.clusterCaCertificate
            )
        } else if let rawDNSEndpoint = Self.nonEmpty(
            cluster.controlPlaneEndpointsConfig?.dnsEndpointConfig?.endpoint
        ) {
            endpoint = try Self.validatedClusterEndpoint(rawDNSEndpoint, requireGKEDNSName: true)
            certificateAuthority = nil
        } else {
            throw GKENativeClusterImportError.invalidClusterEndpoint
        }
        let contextName = "gke_\(validated.projectID)_\(validated.location)_\(validated.clusterName)"
        let rawKubeConfig = Self.kubeConfig(
            contextName: contextName,
            endpoint: endpoint,
            certificateAuthorityData: certificateAuthority
        )
        return GKENativeClusterImportResult(
            rawKubeConfig: rawKubeConfig,
            sourceName: Self.sourceName(for: contextName)
        )
    }

    private static func clusterRequest(
        for request: ValidatedRequest,
        bearerToken: String
    ) throws -> GKEClusterHTTPRequest {
        guard !bearerToken.isEmpty,
              bearerToken.utf8.count <= 131_072,
              bearerToken.unicodeScalars.allSatisfy({ !CharacterSet.controlCharacters.contains($0) }) else {
            throw GKENativeClusterImportError.authenticationFailed
        }
        var components = URLComponents()
        components.scheme = "https"
        components.host = "container.googleapis.com"
        components.path = "/v1/projects/\(request.projectID)/locations/\(request.location)/clusters/\(request.clusterName)"
        components.queryItems = [
            URLQueryItem(
                name: "fields",
                value: "name,location,endpoint,masterAuth/clusterCaCertificate,controlPlaneEndpointsConfig/dnsEndpointConfig/endpoint"
            )
        ]
        guard let url = components.url else {
            throw GKENativeClusterImportError.invalidHTTPResponse
        }
        return GKEClusterHTTPRequest(
            url: url,
            method: "GET",
            headers: [
                "Accept": "application/json",
                "Authorization": "Bearer \(bearerToken)"
            ]
        )
    }

    private static func validatedClusterEndpoint(
        _ rawValue: String,
        requireGKEDNSName: Bool
    ) throws -> String {
        let value = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !value.isEmpty, value.utf8.count <= 1_024,
              !value.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) }),
              let components = URLComponents(string: "https://\(value)"),
              components.scheme?.lowercased() == "https",
              let host = components.host?.lowercased(),
              !host.isEmpty,
              components.user == nil,
              components.password == nil,
              components.port == nil || components.port == 443,
              components.path.isEmpty || components.path == "/",
              components.query == nil,
              components.fragment == nil else {
            throw GKENativeClusterImportError.invalidClusterEndpoint
        }
        if requireGKEDNSName,
           host != "gke.goog",
           !host.hasSuffix(".gke.goog") {
            throw GKENativeClusterImportError.invalidClusterEndpoint
        }
        var normalized = components
        normalized.path = ""
        guard let url = normalized.url,
              url.scheme?.lowercased() == "https",
              url.host?.isEmpty == false else {
            throw GKENativeClusterImportError.invalidClusterEndpoint
        }
        return url.absoluteString
    }

    private static func validatedCertificateAuthority(_ rawValue: String?) throws -> String {
        let value = rawValue?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !value.isEmpty,
              value.utf8.count <= maximumCertificateAuthorityBytes * 2,
              let decoded = Data(base64Encoded: value),
              !decoded.isEmpty,
              decoded.count <= maximumCertificateAuthorityBytes else {
            throw GKENativeClusterImportError.invalidCertificateAuthority
        }
        return value
    }

    private static func kubeConfig(
        contextName: String,
        endpoint: String,
        certificateAuthorityData: String?
    ) -> String {
        let quotedName = yamlQuoted(contextName)
        let certificateAuthorityLine = certificateAuthorityData.map {
            "    certificate-authority-data: \(yamlQuoted($0))\n"
        } ?? ""
        return """
        apiVersion: v1
        kind: Config
        current-context: \(quotedName)
        clusters:
        - name: \(quotedName)
          cluster:
            server: \(yamlQuoted(endpoint))
        \(certificateAuthorityLine)contexts:
        - name: \(quotedName)
          context:
            cluster: \(quotedName)
            user: \(quotedName)
        users:
        - name: \(quotedName)
          user:
            exec:
              apiVersion: client.authentication.k8s.io/v1beta1
              command: gke-gcloud-auth-plugin
              args:
              - --use_application_default_credentials
              provideClusterInfo: true
              interactiveMode: Never
        """
    }

    private static func yamlQuoted(_ value: String) -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.withoutEscapingSlashes]
        guard let data = try? encoder.encode(value),
              let encoded = String(data: data, encoding: .utf8) else {
            return "\"\""
        }
        return encoded
    }

    private static func sourceName(for contextName: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_."))
        var sanitized = String(contextName.unicodeScalars.map { scalar in
            allowed.contains(scalar) ? Character(scalar) : "-"
        })
        while sanitized.contains("--") {
            sanitized = sanitized.replacingOccurrences(of: "--", with: "-")
        }
        sanitized = sanitized
            .trimmingCharacters(in: CharacterSet(charactersIn: "-_."))
        if sanitized.isEmpty {
            sanitized = "gke-cluster"
        }
        return String(sanitized.prefix(180)) + ".yaml"
    }

    private struct ClusterResponse: Decodable {
        struct MasterAuth: Decodable {
            let clusterCaCertificate: String?
        }

        struct ControlPlaneEndpointsConfig: Decodable {
            struct DNSEndpointConfig: Decodable {
                let endpoint: String?
            }

            let dnsEndpointConfig: DNSEndpointConfig?
        }

        let endpoint: String?
        let masterAuth: MasterAuth?
        let controlPlaneEndpointsConfig: ControlPlaneEndpointsConfig?
    }

    private static func nonEmpty(_ value: String?) -> String? {
        guard let value else { return nil }
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    private struct ValidatedRequest {
        let projectID: String
        let location: String
        let clusterName: String

        init(_ request: GKENativeClusterImportRequest) throws {
            projectID = try Self.resourceSegment(request.projectID, field: "project ID", maximumBytes: 256)
            location = try Self.resourceSegment(request.location, field: "location", maximumBytes: 128)
            clusterName = try Self.resourceSegment(request.clusterName, field: "cluster name", maximumBytes: 128)
        }

        private static func resourceSegment(
            _ rawValue: String,
            field: String,
            maximumBytes: Int
        ) throws -> String {
            let value = rawValue.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !value.isEmpty else {
                throw GKENativeClusterImportError.missingRequiredField(field)
            }
            let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_.:"))
            guard value.utf8.count <= maximumBytes,
                  value.unicodeScalars.allSatisfy({ $0.value < 128 && allowed.contains($0) }) else {
                throw GKENativeClusterImportError.invalidResourceIdentifier(field)
            }
            return value
        }
    }
}

private actor GKECancellationTrackingOAuthHTTPClient: GCPServiceAccountHTTPClient {
    private let base: any GCPServiceAccountHTTPClient
    private var observedCancellation = false

    init(base: any GCPServiceAccountHTTPClient) {
        self.base = base
    }

    func send(_ request: GCPServiceAccountHTTPRequest) async throws -> GCPServiceAccountHTTPResponse {
        do {
            return try await base.send(request)
        } catch is CancellationError {
            observedCancellation = true
            throw CancellationError()
        } catch let error as URLError where error.code == .cancelled {
            observedCancellation = true
            throw CancellationError()
        } catch {
            throw error
        }
    }

    func cancellationWasObserved() -> Bool {
        observedCancellation
    }
}
