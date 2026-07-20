import Foundation
import Yams

public struct AKSNativeClusterImportRequest: Sendable, Equatable, CustomStringConvertible, CustomDebugStringConvertible {
    public let subscriptionID: String
    public let resourceGroup: String
    public let clusterName: String
    public let tenantID: String
    public let clientID: String

    public init(
        subscriptionID: String,
        resourceGroup: String,
        clusterName: String,
        tenantID: String,
        clientID: String
    ) throws {
        self.subscriptionID = try AKSNativeClusterValidation.uuid(subscriptionID, field: "subscription-id")
        self.resourceGroup = try AKSNativeClusterValidation.resourceGroup(resourceGroup)
        self.clusterName = try AKSNativeClusterValidation.clusterName(clusterName)
        self.tenantID = try AKSNativeClusterValidation.uuid(tenantID, field: "tenant-id")
        self.clientID = try AKSNativeClusterValidation.uuid(clientID, field: "client-id")
    }

    public var description: String { "AKSNativeClusterImportRequest(<redacted>)" }
    public var debugDescription: String { description }
}

public struct AKSNativeClusterImportResult: Sendable, Equatable, CustomStringConvertible, CustomDebugStringConvertible {
    public let rawKubeConfig: String
    public let sourceName: String

    public init(rawKubeConfig: String, sourceName: String) {
        self.rawKubeConfig = rawKubeConfig
        self.sourceName = sourceName
    }

    public var description: String {
        "AKSNativeClusterImportResult(rawKubeConfig: <redacted>, sourceName: \(sourceName))"
    }

    public var debugDescription: String { description }
}

public struct AKSClusterHTTPRequest: Sendable, Equatable, CustomStringConvertible, CustomDebugStringConvertible {
    public let url: URL
    public let method: String
    public let headers: [String: String]
    public let body: Data
    public let timeout: TimeInterval

    public init(
        url: URL,
        method: String = "POST",
        headers: [String: String],
        body: Data = Data(),
        timeout: TimeInterval = 20
    ) {
        self.url = url
        self.method = method
        self.headers = headers
        self.body = body
        self.timeout = timeout
    }

    public var description: String { "AKSClusterHTTPRequest(<redacted>)" }
    public var debugDescription: String { description }
}

public struct AKSClusterHTTPResponse: Sendable, Equatable, CustomStringConvertible, CustomDebugStringConvertible {
    public let statusCode: Int
    public let body: Data

    public init(statusCode: Int, body: Data) {
        self.statusCode = statusCode
        self.body = body
    }

    public var description: String { "AKSClusterHTTPResponse(statusCode: \(statusCode), body: <redacted>)" }
    public var debugDescription: String { description }
}

public protocol AKSClusterHTTPClient: Sendable {
    func send(_ request: AKSClusterHTTPRequest) async throws -> AKSClusterHTTPResponse
}

public final class URLSessionAKSClusterHTTPClient: AKSClusterHTTPClient, @unchecked Sendable {
    private let session: URLSession
    private let maximumResponseBytes: Int

    public convenience init(maximumResponseBytes: Int = 4_194_304) {
        self.init(
            maximumResponseBytes: maximumResponseBytes,
            sessionConfiguration: .ephemeral
        )
    }

    init(
        maximumResponseBytes: Int = 4_194_304,
        sessionConfiguration: URLSessionConfiguration
    ) {
        let configuration = sessionConfiguration
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        configuration.urlCache = nil
        configuration.httpCookieStorage = nil
        configuration.httpShouldSetCookies = false
        configuration.timeoutIntervalForRequest = 20
        configuration.timeoutIntervalForResource = 30
        self.session = URLSession(
            configuration: configuration,
            delegate: AKSClusterNoRedirectSessionDelegate(),
            delegateQueue: nil
        )
        self.maximumResponseBytes = max(1_024, min(maximumResponseBytes, 8_388_608))
    }

    public func send(_ request: AKSClusterHTTPRequest) async throws -> AKSClusterHTTPResponse {
        guard request.method == "POST",
              request.url.scheme?.lowercased() == "https",
              request.url.host?.lowercased() == AKSNativeClusterImporter.resourceManagerHost,
              request.url.port == nil || request.url.port == 443,
              request.url.user == nil,
              request.url.password == nil,
              request.url.fragment == nil else {
            throw AKSNativeClusterImportError.invalidEndpoint
        }

        var urlRequest = URLRequest(url: request.url)
        urlRequest.httpMethod = request.method
        urlRequest.httpBody = request.body
        urlRequest.timeoutInterval = request.timeout
        for (name, value) in request.headers {
            urlRequest.setValue(value, forHTTPHeaderField: name)
        }

        do {
            let (bytes, response) = try await session.bytes(for: urlRequest)
            guard let http = response as? HTTPURLResponse else {
                throw AKSNativeClusterImportError.transport
            }
            var body = Data()
            body.reserveCapacity(min(maximumResponseBytes, 128 * 1_024))
            for try await byte in bytes {
                try Task.checkCancellation()
                guard body.count < maximumResponseBytes else {
                    throw AKSNativeClusterImportError.responseTooLarge
                }
                body.append(byte)
            }
            return AKSClusterHTTPResponse(statusCode: http.statusCode, body: body)
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as URLError where error.code == .cancelled {
            throw CancellationError()
        } catch let error as AKSNativeClusterImportError {
            throw error
        } catch {
            throw AKSNativeClusterImportError.transport
        }
    }
}

private final class AKSClusterNoRedirectSessionDelegate: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
    func urlSession(
        _ session: URLSession,
        task: URLSessionTask,
        willPerformHTTPRedirection response: HTTPURLResponse,
        newRequest request: URLRequest,
        completionHandler: @escaping (URLRequest?) -> Void
    ) {
        completionHandler(nil)
    }
}

public struct AKSNativeClusterImporter: Sendable {
    static let authorityHost = "login.microsoftonline.com"
    static let resourceManagerHost = "management.azure.com"
    static let resourceManagerScope = "https://management.azure.com/.default"
    static let apiVersion = "2026-04-01"

    private static let maximumTokenResponseBytes = 1_048_576
    private static let maximumClusterResponseBytes = 4_194_304

    private let httpClient: any AKSClusterHTTPClient
    private let tokenHTTPClient: any AKSServicePrincipalTokenHTTPClient

    public init(
        httpClient: any AKSClusterHTTPClient = URLSessionAKSClusterHTTPClient(),
        tokenHTTPClient: any AKSServicePrincipalTokenHTTPClient = URLSessionAKSServicePrincipalTokenHTTPClient()
    ) {
        self.httpClient = httpClient
        self.tokenHTTPClient = tokenHTTPClient
    }

    public func importCluster(
        _ request: AKSNativeClusterImportRequest,
        clientSecret: String
    ) async throws -> AKSNativeClusterImportResult {
        try Task.checkCancellation()
        let credentials: AKSServicePrincipalCredentials
        do {
            credentials = try AKSServicePrincipalCredentials(
                clientID: request.clientID,
                clientSecret: clientSecret
            )
        } catch {
            throw AKSNativeClusterImportError.invalidCredentials
        }

        let accessToken = try await armAccessToken(
            request: request,
            credentials: credentials
        )
        try Task.checkCancellation()

        let response: AKSClusterHTTPResponse
        do {
            response = try await httpClient.send(AKSClusterHTTPRequest(
                url: try clusterCredentialURL(for: request),
                headers: [
                    "Accept": "application/json",
                    "Authorization": "Bearer \(accessToken)"
                ]
            ))
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as URLError where error.code == .cancelled {
            throw CancellationError()
        } catch let error as AKSNativeClusterImportError {
            throw error
        } catch {
            throw AKSNativeClusterImportError.transport
        }
        try Task.checkCancellation()

        guard response.body.count <= Self.maximumClusterResponseBytes else {
            throw AKSNativeClusterImportError.responseTooLarge
        }
        guard (200..<300).contains(response.statusCode) else {
            let code = Self.providerErrorCode(from: response.body)
            throw AKSNativeClusterImportError.clusterRequestFailed(
                statusCode: response.statusCode,
                code: code
            )
        }

        let raw = try decodedKubeConfig(from: response.body)
        let rewritten = try rewrittenKubeConfig(raw, for: request)
        try Task.checkCancellation()
        return AKSNativeClusterImportResult(
            rawKubeConfig: rewritten,
            sourceName: "\(request.clusterName)-kubeconfig.yaml"
        )
    }

    private func armAccessToken(
        request: AKSNativeClusterImportRequest,
        credentials: AKSServicePrincipalCredentials
    ) async throws -> String {
        let tokenURL = URL(string: "https://\(Self.authorityHost)/\(request.tenantID)/oauth2/v2.0/token")!
        let fields = [
            ("client_id", credentials.clientID),
            ("client_secret", credentials.clientSecret),
            ("grant_type", "client_credentials"),
            ("scope", Self.resourceManagerScope)
        ]
        let body = Data(Self.formBody(fields).utf8)
        let response: AKSServicePrincipalTokenHTTPResponse
        do {
            response = try await tokenHTTPClient.send(AKSServicePrincipalTokenHTTPRequest(
                url: tokenURL,
                headers: [
                    "Accept": "application/json",
                    "Content-Type": "application/x-www-form-urlencoded"
                ],
                body: body
            ))
        } catch is CancellationError {
            throw CancellationError()
        } catch let error as URLError where error.code == .cancelled {
            throw CancellationError()
        } catch {
            throw AKSNativeClusterImportError.transport
        }

        guard response.body.count <= Self.maximumTokenResponseBytes else {
            throw AKSNativeClusterImportError.responseTooLarge
        }
        let payload = try? JSONDecoder().decode(AKSTokenPayload.self, from: response.body)
        guard (200..<300).contains(response.statusCode) else {
            throw AKSNativeClusterImportError.authenticationFailed(
                statusCode: response.statusCode,
                code: payload?.error.map(Self.sanitizedProviderCode)
            )
        }
        guard let payload,
              payload.error == nil,
              let token = Self.nonEmpty(payload.accessToken),
              token.utf8.count <= 256 * 1_024,
              token.rangeOfCharacter(from: .newlines) == nil,
              !token.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) }),
              let expiresIn = payload.expiresIn?.seconds,
              (60...86_400).contains(expiresIn),
              payload.tokenType.map({ $0.caseInsensitiveCompare("Bearer") == .orderedSame }) ?? true else {
            throw AKSNativeClusterImportError.invalidProviderResponse
        }
        return token
    }

    private func clusterCredentialURL(for request: AKSNativeClusterImportRequest) throws -> URL {
        var components = URLComponents()
        components.scheme = "https"
        components.host = Self.resourceManagerHost
        components.path = "/subscriptions/\(request.subscriptionID)/resourceGroups/\(request.resourceGroup)/providers/Microsoft.ContainerService/managedClusters/\(request.clusterName)/listClusterUserCredential"
        components.queryItems = [
            URLQueryItem(name: "api-version", value: Self.apiVersion),
            URLQueryItem(name: "format", value: "exec")
        ]
        guard let url = components.url,
              url.scheme == "https",
              url.host == Self.resourceManagerHost else {
            throw AKSNativeClusterImportError.invalidEndpoint
        }
        return url
    }

    private func decodedKubeConfig(from body: Data) throws -> String {
        guard let payload = try? JSONDecoder().decode(AKSCredentialPayload.self, from: body),
              !payload.kubeconfigs.isEmpty,
              payload.kubeconfigs.count <= 16,
              let encoded = payload.kubeconfigs.first?.value,
              !encoded.isEmpty,
              encoded.utf8.count <= Self.maximumClusterResponseBytes * 2,
              let decoded = Data(base64Encoded: encoded),
              !decoded.isEmpty,
              decoded.count <= Self.maximumClusterResponseBytes,
              let raw = String(data: decoded, encoding: .utf8),
              !raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            throw AKSNativeClusterImportError.invalidProviderResponse
        }
        return raw
    }

    private func rewrittenKubeConfig(
        _ raw: String,
        for request: AKSNativeClusterImportRequest
    ) throws -> String {
        let root: Node
        do {
            guard let parsed = try Yams.compose(yaml: raw), parsed.mapping != nil else {
                throw AKSNativeClusterImportError.invalidKubeConfig
            }
            root = parsed
        } catch let error as AKSNativeClusterImportError {
            throw error
        } catch {
            throw AKSNativeClusterImportError.invalidKubeConfig
        }

        guard let currentContext = Self.nonEmpty(root["current-context"]?.string),
              let contextNodes = root["contexts"]?.sequence,
              let currentContextNode = contextNodes.first(where: {
                  $0["name"]?.string == currentContext
              }),
              let userName = Self.nonEmpty(currentContextNode["context"]?["user"]?.string),
              var users = root["users"]?.sequence,
              let userIndex = users.firstIndex(where: { $0["name"]?.string == userName }) else {
            throw AKSNativeClusterImportError.incompatibleKubeConfig
        }

        var namedUser = users[userIndex]
        guard var user = namedUser["user"],
              var exec = user["exec"],
              let command = Self.nonEmpty(exec["command"]?.string),
              Self.isKubelogin(command),
              let rawArguments = exec["args"]?.sequence else {
            throw AKSNativeClusterImportError.incompatibleKubeConfig
        }
        let arguments = try rawArguments.map { node -> String in
            guard let value = node.string else {
                throw AKSNativeClusterImportError.incompatibleKubeConfig
            }
            return value
        }
        let imported = try AKSImportedKubeloginArguments(arguments)
        guard imported.tenantID == request.tenantID else {
            throw AKSNativeClusterImportError.tenantMismatch
        }
        guard imported.environment == .publicCloud else {
            throw AKSNativeClusterImportError.incompatibleKubeConfig
        }

        let rewrittenArguments = [
            "get-token",
            "--environment", AKSAzureEnvironment.publicCloud.rawValue,
            "--server-id", imported.serverID,
            "--client-id", request.clientID,
            "--tenant-id", request.tenantID,
            "--login", "spn"
        ]
        exec["command"] = Node("kubelogin")
        exec["args"] = Node(rewrittenArguments.map(Node.init))
        exec["env"] = Node([] as [Node])
        exec["interactiveMode"] = Node("Never")
        user["exec"] = exec
        namedUser["user"] = user
        users[userIndex] = namedUser

        var rewrittenRoot = root
        rewrittenRoot["users"] = .sequence(users)
        let rewritten: String
        do {
            rewritten = try Yams.serialize(node: rewrittenRoot, allowUnicode: true)
        } catch {
            throw AKSNativeClusterImportError.invalidKubeConfig
        }

        let analysis: KubeConfigNativeAuthAnalysis
        do {
            analysis = try KubeConfigNativeAuthAnalyzer().analyze(raw: rewritten)
        } catch {
            throw AKSNativeClusterImportError.invalidKubeConfig
        }
        guard analysis.currentContext == currentContext,
              let context = analysis.contexts.first(where: { $0.contextName == currentContext }),
              context.provider == .azureKubelogin,
              let credentialRequest = context.credentialRequest,
              credentialRequest.provider == .azureKubelogin,
              let finalExec = context.exec,
              let descriptor = try? AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
                  command: finalExec.command,
                  arguments: finalExec.arguments
              ),
              descriptor.tenantID == request.tenantID,
              descriptor.clientID == request.clientID else {
            throw AKSNativeClusterImportError.incompatibleKubeConfig
        }
        return rewritten
    }

    private static func isKubelogin(_ command: String) -> Bool {
        let executable = URL(fileURLWithPath: command).lastPathComponent.lowercased()
        return executable == "kubelogin" || executable == "kubelogin.exe"
    }

    private static func providerErrorCode(from body: Data) -> String? {
        (try? JSONDecoder().decode(AKSProviderErrorEnvelope.self, from: body))?
            .error.code
            .map(sanitizedProviderCode)
    }

    private static func formBody(_ fields: [(String, String)]) -> String {
        fields.map { "\(formEncoded($0.0))=\(formEncoded($0.1))" }.joined(separator: "&")
    }

    private static func formEncoded(_ value: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-._~"))
        return value.addingPercentEncoding(withAllowedCharacters: allowed) ?? ""
    }

    private static func nonEmpty(_ value: String?) -> String? {
        guard let value else { return nil }
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    private static func sanitizedProviderCode(_ raw: String) -> String {
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_."))
        let scalars = raw.unicodeScalars.prefix(64).filter(allowed.contains)
        let value = String(String.UnicodeScalarView(scalars))
        return value.isEmpty ? "provider_error" : value
    }
}

private struct AKSImportedKubeloginArguments {
    private static let valueFlags: [String: String] = [
        "--tenant-id": "tenant-id",
        "--server-id": "server-id",
        "--client-id": "client-id",
        "--environment": "environment",
        "--login": "login",
        "-l": "login"
    ]

    let tenantID: String
    let serverID: String
    let environment: AKSAzureEnvironment

    init(_ arguments: [String]) throws {
        guard arguments.count <= 64 else {
            throw AKSNativeClusterImportError.incompatibleKubeConfig
        }
        var operation: String?
        var values: [String: String] = [:]
        var index = 0
        while index < arguments.count {
            let argument = arguments[index]
            guard argument.utf8.count <= 4_096 else {
                throw AKSNativeClusterImportError.incompatibleKubeConfig
            }
            if !argument.hasPrefix("-") {
                guard operation == nil else {
                    throw AKSNativeClusterImportError.incompatibleKubeConfig
                }
                operation = argument.lowercased()
                index += 1
                continue
            }

            let flag: String
            let inlineValue: String?
            if let equals = argument.firstIndex(of: "=") {
                flag = String(argument[..<equals])
                inlineValue = String(argument[argument.index(after: equals)...])
            } else {
                flag = argument
                inlineValue = nil
            }
            guard let canonical = Self.valueFlags[flag], values[canonical] == nil else {
                throw AKSNativeClusterImportError.incompatibleKubeConfig
            }
            let value: String
            if let inlineValue {
                value = inlineValue
            } else {
                index += 1
                guard index < arguments.count, !arguments[index].hasPrefix("-") else {
                    throw AKSNativeClusterImportError.incompatibleKubeConfig
                }
                value = arguments[index]
            }
            let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty else {
                throw AKSNativeClusterImportError.incompatibleKubeConfig
            }
            values[canonical] = trimmed
            index += 1
        }

        guard operation == "get-token",
              let tenant = values["tenant-id"],
              let serverID = values["server-id"] else {
            throw AKSNativeClusterImportError.incompatibleKubeConfig
        }
        do {
            self.tenantID = try AKSNativeClusterValidation.uuid(tenant, field: "tenant-id")
        } catch {
            throw AKSNativeClusterImportError.incompatibleKubeConfig
        }
        let validationClientID = values["client-id"] ?? "00000000-0000-4000-8000-000000000000"
        let descriptor: AKSKubeloginServicePrincipalDescriptor
        do {
            descriptor = try AKSKubeloginServicePrincipalDescriptor(
                tenantID: self.tenantID,
                serverID: serverID,
                clientID: validationClientID,
                environment: .publicCloud
            )
        } catch {
            throw AKSNativeClusterImportError.incompatibleKubeConfig
        }
        self.serverID = descriptor.serverID

        if let rawEnvironment = values["environment"] {
            guard let environment = AKSAzureEnvironment(rawValue: rawEnvironment) else {
                throw AKSNativeClusterImportError.incompatibleKubeConfig
            }
            self.environment = environment
        } else {
            self.environment = .publicCloud
        }
    }
}

private enum AKSNativeClusterValidation {
    static func uuid(_ raw: String, field: String) throws -> String {
        let value = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let uuid = UUID(uuidString: value) else {
            throw AKSNativeClusterImportError.invalidRequest(field: field)
        }
        return uuid.uuidString.lowercased()
    }

    static func resourceGroup(_ raw: String) throws -> String {
        let value = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_.()"))
        guard !value.isEmpty,
              value.utf8.count <= 90,
              !value.hasSuffix("."),
              value.unicodeScalars.allSatisfy(allowed.contains) else {
            throw AKSNativeClusterImportError.invalidRequest(field: "resource-group")
        }
        return value
    }

    static func clusterName(_ raw: String) throws -> String {
        let value = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        let allowed = CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_")
        let alphanumeric = CharacterSet.alphanumerics
        guard !value.isEmpty,
              value.utf8.count <= 63,
              value.unicodeScalars.allSatisfy(allowed.contains),
              let first = value.unicodeScalars.first,
              let last = value.unicodeScalars.last,
              alphanumeric.contains(first),
              alphanumeric.contains(last) else {
            throw AKSNativeClusterImportError.invalidRequest(field: "cluster-name")
        }
        return value
    }
}

private struct AKSTokenPayload: Decodable {
    let accessToken: String?
    let tokenType: String?
    let expiresIn: AKSFlexibleSeconds?
    let error: String?

    enum CodingKeys: String, CodingKey {
        case accessToken = "access_token"
        case tokenType = "token_type"
        case expiresIn = "expires_in"
        case error
    }
}

private enum AKSFlexibleSeconds: Decodable {
    case value(Int)

    var seconds: Int {
        switch self { case .value(let value): return value }
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if let value = try? container.decode(Int.self) {
            self = .value(value)
            return
        }
        if let string = try? container.decode(String.self), let value = Int(string) {
            self = .value(value)
            return
        }
        throw DecodingError.dataCorruptedError(in: container, debugDescription: "Invalid expires_in")
    }
}

private struct AKSCredentialPayload: Decodable {
    struct KubeConfig: Decodable {
        let value: String
    }

    let kubeconfigs: [KubeConfig]
}

private struct AKSProviderErrorEnvelope: Decodable {
    struct ProviderError: Decodable {
        let code: String?
    }

    let error: ProviderError
}

public enum AKSNativeClusterImportError: Error, LocalizedError, Sendable, Equatable {
    case invalidRequest(field: String)
    case invalidCredentials
    case invalidEndpoint
    case transport
    case authenticationFailed(statusCode: Int, code: String?)
    case clusterRequestFailed(statusCode: Int, code: String?)
    case responseTooLarge
    case invalidProviderResponse
    case invalidKubeConfig
    case tenantMismatch
    case incompatibleKubeConfig

    public var errorDescription: String? {
        switch self {
        case .invalidRequest(let field):
            return "Microsoft AKS import configuration is missing or invalid: \(field)."
        case .invalidCredentials:
            return "Microsoft AKS service-principal credentials are invalid."
        case .invalidEndpoint:
            return "Microsoft AKS import endpoint is invalid."
        case .transport:
            return "Microsoft AKS import request failed."
        case .authenticationFailed(let statusCode, let code):
            let suffix = code.map { " (\($0))" } ?? ""
            let safeStatus = (100...599).contains(statusCode) ? statusCode : 0
            return "Microsoft authentication returned HTTP \(safeStatus)\(suffix)."
        case .clusterRequestFailed(let statusCode, let code):
            let suffix = code.map { " (\($0))" } ?? ""
            let safeStatus = (100...599).contains(statusCode) ? statusCode : 0
            return "Microsoft AKS credential request returned HTTP \(safeStatus)\(suffix)."
        case .responseTooLarge:
            return "Microsoft AKS returned a response that exceeded the allowed size."
        case .invalidProviderResponse:
            return "Microsoft AKS returned an invalid credential response."
        case .invalidKubeConfig:
            return "Microsoft AKS returned an invalid kubeconfig."
        case .tenantMismatch:
            return "Microsoft AKS returned a kubeconfig for a different tenant."
        case .incompatibleKubeConfig:
            return "Microsoft AKS returned a kubeconfig that is not compatible with native authentication."
        }
    }
}
