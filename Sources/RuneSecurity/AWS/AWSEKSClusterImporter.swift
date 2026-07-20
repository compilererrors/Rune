import CryptoKit
import Foundation

public struct AWSEKSClusterImportRequest: Equatable, Sendable {
    public let clusterName: String
    public let region: String

    public init(clusterName: String, region: String) {
        self.clusterName = clusterName
        self.region = region
    }
}

public struct AWSEKSClusterImportResult: Equatable, Sendable {
    public let rawKubeConfig: String
    public let sourceName: String

    public init(rawKubeConfig: String, sourceName: String) {
        self.rawKubeConfig = rawKubeConfig
        self.sourceName = sourceName
    }
}

public struct AWSEKSClusterHTTPRequest: Equatable, Sendable, CustomStringConvertible, CustomDebugStringConvertible {
    public let url: URL
    public let method: String
    public let headers: [String: String]

    public init(url: URL, method: String, headers: [String: String]) {
        self.url = url
        self.method = method
        self.headers = headers
    }

    public var description: String { "AWSEKSClusterHTTPRequest(<redacted>)" }
    public var debugDescription: String { description }
}

public struct AWSEKSClusterHTTPResponse: Equatable, Sendable {
    public let statusCode: Int
    public let headers: [String: String]
    public let body: Data

    public init(statusCode: Int, headers: [String: String] = [:], body: Data) {
        self.statusCode = statusCode
        self.headers = headers
        self.body = body
    }
}

public protocol AWSEKSClusterHTTPClient: Sendable {
    func send(_ request: AWSEKSClusterHTTPRequest) async throws -> AWSEKSClusterHTTPResponse
}

public enum AWSEKSClusterImportError: Error, LocalizedError, Equatable, Sendable {
    case invalidClusterName
    case invalidRegion
    case unsupportedPartition
    case networkFailure
    case invalidHTTPResponse
    case responseTooLarge
    case authenticationFailed
    case accessDenied
    case clusterNotFound
    case requestRejected(Int)
    case invalidClusterResponse
    case clusterNotReady
    case invalidClusterEndpoint
    case invalidCertificateAuthority

    public var errorDescription: String? {
        switch self {
        case .invalidClusterName:
            return "Amazon EKS import requires a valid cluster name."
        case .invalidRegion:
            return "Amazon EKS import requires a valid AWS region."
        case .unsupportedPartition:
            return "Amazon EKS import does not support this AWS partition yet."
        case .networkFailure:
            return "Rune could not reach the Amazon EKS API."
        case .invalidHTTPResponse:
            return "Amazon EKS returned an invalid HTTP response."
        case .responseTooLarge:
            return "The Amazon EKS response exceeded the supported size."
        case .authenticationFailed:
            return "Rune could not authenticate the AWS credentials for Amazon EKS."
        case .accessDenied:
            return "AWS denied access to this Amazon EKS cluster. Check the credentials and eks:DescribeCluster permission."
        case .clusterNotFound:
            return "Amazon EKS could not find that cluster in the selected region."
        case let .requestRejected(statusCode):
            let safeStatus = (100...599).contains(statusCode) ? statusCode : 0
            return "Amazon EKS rejected the cluster request (HTTP \(safeStatus))."
        case .invalidClusterResponse:
            return "Amazon EKS returned incomplete cluster connection information."
        case .clusterNotReady:
            return "The Amazon EKS cluster is not ready to connect."
        case .invalidClusterEndpoint:
            return "Amazon EKS returned an invalid cluster endpoint."
        case .invalidCertificateAuthority:
            return "Amazon EKS returned an invalid cluster certificate authority."
        }
    }
}

/// URLSession transport restricted to official regional Amazon EKS origins.
/// Redirects are rejected so signed AWS headers cannot be forwarded elsewhere.
public final class AWSEKSClusterURLSessionHTTPClient: AWSEKSClusterHTTPClient, @unchecked Sendable {
    private static let maximumResponseBytes = 1_048_576

    private let sessionConfiguration: URLSessionConfiguration

    public init() {
        sessionConfiguration = .ephemeral
    }

    /// Test-only transport seam. Production callers use `init()`.
    init(sessionConfiguration: URLSessionConfiguration) {
        self.sessionConfiguration = sessionConfiguration
    }

    public func send(_ request: AWSEKSClusterHTTPRequest) async throws -> AWSEKSClusterHTTPResponse {
        guard request.method == "GET", Self.isAllowedAPIURL(request.url) else {
            throw AWSEKSClusterImportError.invalidHTTPResponse
        }

        var urlRequest = URLRequest(url: request.url)
        urlRequest.httpMethod = request.method
        urlRequest.timeoutInterval = 20
        for (name, value) in request.headers {
            guard !name.contains("\r"), !name.contains("\n"),
                  !value.contains("\r"), !value.contains("\n") else {
                throw AWSEKSClusterImportError.invalidHTTPResponse
            }
            urlRequest.setValue(value, forHTTPHeaderField: name)
        }

        let configuration = sessionConfiguration.copy() as! URLSessionConfiguration
        configuration.waitsForConnectivity = false
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        configuration.urlCache = nil
        configuration.httpCookieStorage = nil
        configuration.httpShouldSetCookies = false
        let delegate = AWSEKSClusterNoRedirectDelegate()
        let session = URLSession(configuration: configuration, delegate: delegate, delegateQueue: nil)
        defer { session.invalidateAndCancel() }

        do {
            let (bytes, response) = try await session.bytes(for: urlRequest)
            guard let httpResponse = response as? HTTPURLResponse else {
                throw AWSEKSClusterImportError.invalidHTTPResponse
            }
            if let contentLength = Self.contentLength(httpResponse),
               contentLength > Self.maximumResponseBytes {
                throw AWSEKSClusterImportError.responseTooLarge
            }

            var body = Data()
            body.reserveCapacity(min(Self.maximumResponseBytes, 64 * 1_024))
            for try await byte in bytes {
                try Task.checkCancellation()
                guard body.count < Self.maximumResponseBytes else {
                    throw AWSEKSClusterImportError.responseTooLarge
                }
                body.append(byte)
            }

            var headers: [String: String] = [:]
            for (key, value) in httpResponse.allHeaderFields {
                guard let key = key as? String, let value = value as? String else { continue }
                headers[key] = value
            }
            return AWSEKSClusterHTTPResponse(
                statusCode: httpResponse.statusCode,
                headers: headers,
                body: body
            )
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as URLError where error.code == .cancelled {
            throw CancellationError()
        } catch let error as AWSEKSClusterImportError {
            throw error
        } catch {
            throw AWSEKSClusterImportError.networkFailure
        }
    }

    private static func contentLength(_ response: HTTPURLResponse) -> Int? {
        let rawValue = response.value(forHTTPHeaderField: "Content-Length") ?? ""
        guard let value = Int(rawValue), value >= 0 else { return nil }
        return value
    }

    private static func isAllowedAPIURL(_ url: URL) -> Bool {
        guard let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
              components.scheme?.lowercased() == "https",
              let host = components.host?.lowercased(),
              components.port == nil || components.port == 443,
              components.user == nil,
              components.password == nil,
              components.query == nil,
              components.fragment == nil else {
            return false
        }
        let pathSegments = components.path.split(separator: "/", omittingEmptySubsequences: true)
        guard pathSegments.count == 2,
              pathSegments[0] == "clusters",
              let clusterName = pathSegments[1].removingPercentEncoding,
              ValidatedAWSEKSClusterImportRequest.isValidClusterName(clusterName),
              let region = AWSEKSEndpoint.region(fromEKSHost: host),
              let endpoint = try? AWSEKSEndpoint(region: region),
              endpoint.host == host else {
            return false
        }
        return true
    }
}

private final class AWSEKSClusterNoRedirectDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
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

/// Calls EKS DescribeCluster with SigV4 and creates an analyzer-compatible
/// kubeconfig without persisting AWS credentials or invoking the AWS CLI.
public struct AWSEKSClusterImporter: Sendable {
    private static let maximumResponseBytes = 1_048_576
    private static let maximumCertificateAuthorityBytes = 262_144
    private static let maximumEndpointBytes = 2_048

    private let httpClient: any AWSEKSClusterHTTPClient
    private let signingDate: @Sendable () -> Date

    public init(httpClient: any AWSEKSClusterHTTPClient = AWSEKSClusterURLSessionHTTPClient()) {
        self.httpClient = httpClient
        self.signingDate = Date.init
    }

    init(
        httpClient: any AWSEKSClusterHTTPClient,
        signingDate: @escaping @Sendable () -> Date
    ) {
        self.httpClient = httpClient
        self.signingDate = signingDate
    }

    public func importCluster(
        _ request: AWSEKSClusterImportRequest,
        credentials: AWSEKSCredentials
    ) async throws -> AWSEKSClusterImportResult {
        let validated = try ValidatedAWSEKSClusterImportRequest(request)
        let now = signingDate()
        if let expiration = credentials.expiration,
           expiration <= now.addingTimeInterval(30) {
            throw AWSEKSNativeAuthError.expiredCredentials
        }
        try Task.checkCancellation()

        let apiRequest = try AWSEKSDescribeClusterSigner.request(
            for: validated,
            credentials: credentials,
            signingDate: now
        )
        let response: AWSEKSClusterHTTPResponse
        do {
            response = try await httpClient.send(apiRequest)
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as AWSEKSClusterImportError {
            if Task.isCancelled { throw CancellationError() }
            throw error
        } catch {
            if Task.isCancelled { throw CancellationError() }
            throw AWSEKSClusterImportError.networkFailure
        }
        try Task.checkCancellation()

        guard response.body.count <= Self.maximumResponseBytes else {
            throw AWSEKSClusterImportError.responseTooLarge
        }
        guard (200...299).contains(response.statusCode) else {
            switch response.statusCode {
            case 401:
                throw AWSEKSClusterImportError.authenticationFailed
            case 403:
                throw AWSEKSClusterImportError.accessDenied
            case 404:
                throw AWSEKSClusterImportError.clusterNotFound
            default:
                throw AWSEKSClusterImportError.requestRejected(response.statusCode)
            }
        }

        let payload: DescribeClusterResponse
        do {
            payload = try JSONDecoder().decode(DescribeClusterResponse.self, from: response.body)
        } catch {
            throw AWSEKSClusterImportError.invalidClusterResponse
        }
        let cluster = try Self.validatedCluster(payload.cluster, request: validated)
        let identifier = try Self.execIdentifier(for: cluster)
        let rawKubeConfig = Self.kubeConfig(
            contextName: cluster.arn,
            endpoint: cluster.endpoint,
            certificateAuthorityData: cluster.certificateAuthorityData,
            region: validated.region,
            identifier: identifier
        )
        return AWSEKSClusterImportResult(
            rawKubeConfig: rawKubeConfig,
            sourceName: Self.sourceName(for: validated.clusterName)
        )
    }

    private static func validatedCluster(
        _ rawCluster: DescribeClusterResponse.Cluster?,
        request: ValidatedAWSEKSClusterImportRequest
    ) throws -> ValidatedCluster {
        guard let rawCluster,
              rawCluster.name == request.clusterName,
              let status = rawCluster.status else {
            throw AWSEKSClusterImportError.invalidClusterResponse
        }
        guard status == "ACTIVE" || status == "UPDATING" else {
            throw AWSEKSClusterImportError.clusterNotReady
        }
        let arn = try validatedARN(rawCluster.arn, request: request)
        let endpoint = try validatedClusterEndpoint(rawCluster.endpoint)
        let certificateAuthorityData = try validatedCertificateAuthority(
            rawCluster.certificateAuthority?.data
        )
        return ValidatedCluster(
            arn: arn,
            endpoint: endpoint,
            certificateAuthorityData: certificateAuthorityData,
            id: rawCluster.id,
            usesClusterID: rawCluster.outpostConfig != nil
                && rawCluster.outpostConfig?.etcdInstanceType == nil
        )
    }

    private static func validatedARN(
        _ rawValue: String?,
        request: ValidatedAWSEKSClusterImportRequest
    ) throws -> String {
        let value = rawValue?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !value.isEmpty,
              value.utf8.count <= 2_048,
              !value.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) }) else {
            throw AWSEKSClusterImportError.invalidClusterResponse
        }
        let components = value.split(separator: ":", maxSplits: 5, omittingEmptySubsequences: false)
        guard components.count == 6,
              components[0] == "arn",
              components[1] == Substring(request.endpoint.partition),
              components[2] == "eks",
              components[3] == Substring(request.region),
              isValidAccountID(String(components[4])),
              components[5] == Substring("cluster/\(request.clusterName)") else {
            throw AWSEKSClusterImportError.invalidClusterResponse
        }
        return value
    }

    private static func isValidAccountID(_ value: String) -> Bool {
        value.range(of: #"^[0-9]{12}$"#, options: .regularExpression) != nil
    }

    private static func validatedClusterEndpoint(_ rawValue: String?) throws -> String {
        let value = rawValue?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !value.isEmpty,
              value.utf8.count <= maximumEndpointBytes,
              !value.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) }),
              let components = URLComponents(string: value),
              components.scheme?.lowercased() == "https",
              components.host?.isEmpty == false,
              components.user == nil,
              components.password == nil,
              components.port == nil || components.port == 443,
              components.path.isEmpty || components.path == "/",
              components.query == nil,
              components.fragment == nil,
              let url = components.url else {
            throw AWSEKSClusterImportError.invalidClusterEndpoint
        }
        return url.absoluteString
    }

    private static func validatedCertificateAuthority(_ rawValue: String?) throws -> String {
        let value = rawValue?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !value.isEmpty,
              value.utf8.count <= maximumCertificateAuthorityBytes * 2,
              value.utf8.count.isMultiple(of: 4),
              value.range(
                  of: #"^[A-Za-z0-9+/]*={0,2}$"#,
                  options: .regularExpression
              ) != nil,
              let decoded = Data(base64Encoded: value),
              !decoded.isEmpty,
              decoded.count <= maximumCertificateAuthorityBytes else {
            throw AWSEKSClusterImportError.invalidCertificateAuthority
        }
        return value
    }

    private static func execIdentifier(for cluster: ValidatedCluster) throws -> AWSEKSClusterIdentifier {
        if cluster.usesClusterID {
            guard let id = cluster.id else {
                throw AWSEKSClusterImportError.invalidClusterResponse
            }
            do {
                return try AWSEKSClusterIdentifier(kind: .id, value: id)
            } catch {
                throw AWSEKSClusterImportError.invalidClusterResponse
            }
        }
        let resource = cluster.arn.split(separator: "/", maxSplits: 1, omittingEmptySubsequences: false)
        guard resource.count == 2 else {
            throw AWSEKSClusterImportError.invalidClusterResponse
        }
        do {
            return try AWSEKSClusterIdentifier(kind: .name, value: String(resource[1]))
        } catch {
            throw AWSEKSClusterImportError.invalidClusterResponse
        }
    }

    private static func kubeConfig(
        contextName: String,
        endpoint: String,
        certificateAuthorityData: String,
        region: String,
        identifier: AWSEKSClusterIdentifier
    ) -> String {
        let quotedName = yamlQuoted(contextName)
        let identifierOption = identifier.kind == .id ? "--cluster-id" : "--cluster-name"
        return """
        apiVersion: v1
        kind: Config
        current-context: \(quotedName)
        clusters:
        - name: \(quotedName)
          cluster:
            server: \(yamlQuoted(endpoint))
            certificate-authority-data: \(yamlQuoted(certificateAuthorityData))
        contexts:
        - name: \(quotedName)
          context:
            cluster: \(quotedName)
            user: \(quotedName)
        users:
        - name: \(quotedName)
          user:
            exec:
              apiVersion: client.authentication.k8s.io/v1
              command: aws
              args:
              - --region
              - \(yamlQuoted(region))
              - eks
              - get-token
              - \(identifierOption)
              - \(yamlQuoted(identifier.value))
              - --output
              - json
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

    private static func sourceName(for clusterName: String) -> String {
        "eks-\(clusterName).yaml"
    }

    private struct DescribeClusterResponse: Decodable {
        struct Cluster: Decodable {
            struct CertificateAuthority: Decodable {
                let data: String?
            }

            struct OutpostConfig: Decodable {
                let etcdInstanceType: String?
            }

            let arn: String?
            let certificateAuthority: CertificateAuthority?
            let endpoint: String?
            let id: String?
            let name: String?
            let outpostConfig: OutpostConfig?
            let status: String?
        }

        let cluster: Cluster?
    }

    private struct ValidatedCluster {
        let arn: String
        let endpoint: String
        let certificateAuthorityData: String
        let id: String?
        let usesClusterID: Bool
    }
}

private struct ValidatedAWSEKSClusterImportRequest: Sendable {
    let clusterName: String
    let region: String
    let endpoint: AWSEKSEndpoint

    init(_ request: AWSEKSClusterImportRequest) throws {
        let clusterName = request.clusterName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard Self.isValidClusterName(clusterName) else {
            throw AWSEKSClusterImportError.invalidClusterName
        }
        let region = request.region.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard region.utf8.count <= 64,
              region.range(
                  of: #"^[a-z0-9]+(?:-[a-z0-9]+)+-[0-9]+$"#,
                  options: .regularExpression
              ) != nil else {
            throw AWSEKSClusterImportError.invalidRegion
        }
        self.clusterName = clusterName
        self.region = region
        self.endpoint = try AWSEKSEndpoint(region: region)
    }

    static func isValidClusterName(_ value: String) -> Bool {
        !value.isEmpty
            && value.utf8.count <= 100
            && value.range(
                of: #"^[0-9A-Za-z][A-Za-z0-9_-]*$"#,
                options: .regularExpression
            ) != nil
    }
}

private struct AWSEKSEndpoint: Sendable {
    let host: String
    let partition: String

    init(region: String) throws {
        if region.hasPrefix("us-iso-")
            || region.hasPrefix("us-isob-")
            || region.hasPrefix("us-isof-")
            || region.hasPrefix("eu-isoe-")
            || region.hasPrefix("eusc-") {
            throw AWSEKSClusterImportError.unsupportedPartition
        }
        if region.hasPrefix("cn-") {
            host = "eks.\(region).amazonaws.com.cn"
            partition = "aws-cn"
            return
        }
        if region.hasPrefix("us-gov-") {
            host = "eks.\(region).amazonaws.com"
            partition = "aws-us-gov"
            return
        }
        let commercialPrefixes = [
            "af-", "ap-", "ca-", "eu-", "il-", "me-", "mx-", "sa-", "us-east-", "us-west-"
        ]
        guard commercialPrefixes.contains(where: region.hasPrefix) else {
            throw AWSEKSClusterImportError.unsupportedPartition
        }
        host = "eks.\(region).amazonaws.com"
        partition = "aws"
    }

    static func region(fromEKSHost host: String) -> String? {
        guard host.hasPrefix("eks.") else { return nil }
        if host.hasSuffix(".amazonaws.com.cn") {
            let start = host.index(host.startIndex, offsetBy: 4)
            let end = host.index(host.endIndex, offsetBy: -".amazonaws.com.cn".count)
            guard start < end else { return nil }
            return String(host[start..<end])
        }
        if host.hasSuffix(".amazonaws.com") {
            let start = host.index(host.startIndex, offsetBy: 4)
            let end = host.index(host.endIndex, offsetBy: -".amazonaws.com".count)
            guard start < end else { return nil }
            return String(host[start..<end])
        }
        return nil
    }
}

private enum AWSEKSDescribeClusterSigner {
    static func request(
        for request: ValidatedAWSEKSClusterImportRequest,
        credentials: AWSEKSCredentials,
        signingDate: Date
    ) throws -> AWSEKSClusterHTTPRequest {
        var components = URLComponents()
        components.scheme = "https"
        components.host = request.endpoint.host
        components.path = "/clusters/\(request.clusterName)"
        guard let url = components.url else {
            throw AWSEKSClusterImportError.invalidHTTPResponse
        }

        let timestamp = formatted(signingDate, format: "yyyyMMdd'T'HHmmss'Z'")
        let dateStamp = formatted(signingDate, format: "yyyyMMdd")
        let scope = "\(dateStamp)/\(request.region)/eks/aws4_request"
        var canonicalHeaderPairs: [(String, String)] = [
            ("host", request.endpoint.host),
            ("x-amz-date", timestamp)
        ]
        if let sessionToken = credentials.sessionToken {
            canonicalHeaderPairs.append(("x-amz-security-token", canonicalHeaderValue(sessionToken)))
        }
        canonicalHeaderPairs.sort { $0.0 < $1.0 }
        let canonicalHeaders = canonicalHeaderPairs
            .map { "\($0.0):\($0.1)\n" }
            .joined()
        let signedHeaders = canonicalHeaderPairs.map(\.0).joined(separator: ";")
        let canonicalRequest = [
            "GET",
            components.percentEncodedPath,
            "",
            canonicalHeaders,
            signedHeaders,
            sha256Hex(Data())
        ].joined(separator: "\n")
        let stringToSign = [
            "AWS4-HMAC-SHA256",
            timestamp,
            scope,
            sha256Hex(Data(canonicalRequest.utf8))
        ].joined(separator: "\n")

        let dateKey = hmacSHA256(
            key: Data("AWS4\(credentials.secretAccessKey)".utf8),
            message: Data(dateStamp.utf8)
        )
        let regionKey = hmacSHA256(key: dateKey, message: Data(request.region.utf8))
        let serviceKey = hmacSHA256(key: regionKey, message: Data("eks".utf8))
        let signingKey = hmacSHA256(key: serviceKey, message: Data("aws4_request".utf8))
        let signature = hex(hmacSHA256(key: signingKey, message: Data(stringToSign.utf8)))
        let authorization = "AWS4-HMAC-SHA256 Credential=\(credentials.accessKeyID)/\(scope), "
            + "SignedHeaders=\(signedHeaders), Signature=\(signature)"

        var headers = [
            "Accept": "application/json",
            "Authorization": authorization,
            "Host": request.endpoint.host,
            "X-Amz-Date": timestamp
        ]
        if let sessionToken = credentials.sessionToken {
            headers["X-Amz-Security-Token"] = sessionToken
        }
        return AWSEKSClusterHTTPRequest(url: url, method: "GET", headers: headers)
    }

    private static func canonicalHeaderValue(_ value: String) -> String {
        value.split(whereSeparator: { $0 == " " || $0 == "\t" }).joined(separator: " ")
    }

    private static func formatted(_ date: Date, format: String) -> String {
        let formatter = DateFormatter()
        formatter.calendar = Calendar(identifier: .gregorian)
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = format
        return formatter.string(from: date)
    }

    private static func sha256Hex(_ data: Data) -> String {
        hex(Data(SHA256.hash(data: data)))
    }

    private static func hmacSHA256(key: Data, message: Data) -> Data {
        let key = SymmetricKey(data: key)
        return Data(HMAC<SHA256>.authenticationCode(for: message, using: key))
    }

    private static func hex(_ data: Data) -> String {
        data.map { String(format: "%02x", $0) }.joined()
    }
}
