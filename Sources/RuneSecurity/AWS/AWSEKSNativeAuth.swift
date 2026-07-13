import CryptoKit
import Foundation

/// A sanitized, provider-specific error produced while recognizing an AWS EKS
/// exec plugin or creating its native authentication token.
///
/// Error descriptions deliberately exclude credentials, tokens, cluster names,
/// profile names, endpoints supplied by the user, and command output.
public enum AWSEKSNativeAuthError: Error, LocalizedError, Sendable, Equatable {
    case missingOptionValue(String)
    case duplicateOption(String)
    case unsupportedOption(String)
    case unsupportedArgument
    case unsupportedRoleAssumption
    case customEndpointUnsupported
    case missingRegion
    case invalidRegion
    case unsupportedPartition
    case missingClusterIdentifier
    case conflictingClusterIdentifiers
    case invalidClusterIdentifier
    case invalidCredentials
    case expiredCredentials
    case tokenTooLarge

    public var errorDescription: String? {
        switch self {
        case let .missingOptionValue(option):
            return "AWS EKS exec auth is missing a value for \(Self.safeOption(option))."
        case let .duplicateOption(option):
            return "AWS EKS exec auth contains duplicate \(Self.safeOption(option)) options."
        case let .unsupportedOption(option):
            return "AWS EKS exec auth uses unsupported option \(Self.safeOption(option))."
        case .unsupportedArgument:
            return "AWS EKS exec auth contains an unsupported positional argument."
        case .unsupportedRoleAssumption:
            return "AWS EKS native auth does not support --role-arn yet. Configure credentials for the final role or use the direct Rune build."
        case .customEndpointUnsupported:
            return "AWS EKS native auth does not support custom AWS endpoints."
        case .missingRegion:
            return "AWS EKS native auth requires an explicit AWS region."
        case .invalidRegion:
            return "AWS EKS native auth contains an invalid AWS region."
        case .unsupportedPartition:
            return "AWS EKS native auth does not support this AWS partition yet."
        case .missingClusterIdentifier:
            return "AWS EKS exec auth requires --cluster-name or --cluster-id."
        case .conflictingClusterIdentifiers:
            return "AWS EKS exec auth cannot combine --cluster-name and --cluster-id."
        case .invalidClusterIdentifier:
            return "AWS EKS exec auth contains an invalid cluster identifier."
        case .invalidCredentials:
            return "AWS credentials are missing or malformed. Reconnect the AWS credential profile."
        case .expiredCredentials:
            return "AWS session credentials have expired. Reconnect the AWS credential profile."
        case .tokenTooLarge:
            return "The generated AWS EKS authentication token exceeds the supported size."
        }
    }

    private static func safeOption(_ value: String) -> String {
        let allowed = CharacterSet(charactersIn: "-abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
        let sanitized = value.unicodeScalars.filter { allowed.contains($0) }
        let result = String(String.UnicodeScalarView(sanitized)).prefix(64)
        return result.isEmpty ? "<unknown>" : String(result)
    }
}

public struct AWSEKSClusterIdentifier: Sendable, Equatable, Hashable {
    public enum Kind: String, Sendable, Equatable, Hashable {
        case name
        case id
    }

    public let kind: Kind
    public let value: String

    public init(kind: Kind, value: String) throws {
        let normalized = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalized.isEmpty,
              normalized.utf8.count <= 256,
              normalized.unicodeScalars.allSatisfy({ scalar in
                  !CharacterSet.controlCharacters.contains(scalar)
                      && !CharacterSet.whitespacesAndNewlines.contains(scalar)
              }) else {
            throw AWSEKSNativeAuthError.invalidClusterIdentifier
        }
        self.kind = kind
        self.value = normalized
    }
}

/// The non-secret subset of `aws eks get-token` needed to produce an EKS
/// authentication token without launching the AWS CLI.
public struct AWSEKSExecDescriptor: Sendable, Equatable, Hashable {
    public let region: String
    public let clusterIdentifier: AWSEKSClusterIdentifier
    public let profileHint: String?

    public init(
        region: String,
        clusterIdentifier: AWSEKSClusterIdentifier,
        profileHint: String? = nil
    ) throws {
        self.region = try Self.validatedRegion(region)
        self.clusterIdentifier = clusterIdentifier
        self.profileHint = Self.normalizedHint(profileHint)
    }

    /// Recognizes an AWS CLI EKS token command. Commands for other providers or
    /// other AWS operations return `nil`. Once an EKS token command is
    /// recognized, unsupported authentication semantics fail closed.
    public static func parseIfSupported(
        command: String,
        arguments: [String],
        environment: [String: String] = [:]
    ) throws -> AWSEKSExecDescriptor? {
        guard Self.isAWSExecutable(command) else { return nil }
        guard arguments.indices.contains(where: { index in
            let next = arguments.index(after: index)
            return next < arguments.endIndex
                && arguments[index].lowercased() == "eks"
                && arguments[next].lowercased() == "get-token"
        }) else {
            return nil
        }

        let parsed = try ParsedArguments(arguments)
        guard parsed.positionals.count >= 2,
              parsed.positionals[0].lowercased() == "eks",
              parsed.positionals[1].lowercased() == "get-token" else {
            return nil
        }
        guard parsed.positionals.count == 2 else {
            throw AWSEKSNativeAuthError.unsupportedArgument
        }

        if parsed.values["--role-arn"] != nil {
            throw AWSEKSNativeAuthError.unsupportedRoleAssumption
        }
        if parsed.values["--endpoint-url"] != nil {
            throw AWSEKSNativeAuthError.customEndpointUnsupported
        }
        if parsed.flags.contains("--no-sign-request") {
            throw AWSEKSNativeAuthError.unsupportedOption("--no-sign-request")
        }
        if let unsupported = parsed.unsupportedOptions.first {
            throw AWSEKSNativeAuthError.unsupportedOption(unsupported)
        }

        let clusterName = parsed.values["--cluster-name"]
        let clusterID = parsed.values["--cluster-id"]
        guard clusterName == nil || clusterID == nil else {
            throw AWSEKSNativeAuthError.conflictingClusterIdentifiers
        }
        let identifier: AWSEKSClusterIdentifier
        if let clusterName {
            identifier = try AWSEKSClusterIdentifier(kind: .name, value: clusterName)
        } else if let clusterID {
            identifier = try AWSEKSClusterIdentifier(kind: .id, value: clusterID)
        } else {
            throw AWSEKSNativeAuthError.missingClusterIdentifier
        }

        let region = parsed.values["--region"]
            ?? Self.nonEmpty(environment["AWS_REGION"])
            ?? Self.nonEmpty(environment["AWS_DEFAULT_REGION"])
        guard let region else {
            throw AWSEKSNativeAuthError.missingRegion
        }
        let profile = parsed.values["--profile"] ?? Self.nonEmpty(environment["AWS_PROFILE"])
        return try AWSEKSExecDescriptor(
            region: region,
            clusterIdentifier: identifier,
            profileHint: profile
        )
    }

    private static func isAWSExecutable(_ command: String) -> Bool {
        let trimmed = command.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return false }
        let basename = URL(fileURLWithPath: trimmed).lastPathComponent.lowercased()
        return basename == "aws" || basename == "aws.exe"
    }

    private static func validatedRegion(_ value: String) throws -> String {
        let region = value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard region.utf8.count <= 64,
              region.range(
                  of: #"^[a-z0-9]+(?:-[a-z0-9]+)+-[0-9]+$"#,
                  options: .regularExpression
              ) != nil else {
            throw AWSEKSNativeAuthError.invalidRegion
        }
        return region
    }

    private static func normalizedHint(_ value: String?) -> String? {
        guard let normalized = nonEmpty(value) else { return nil }
        return String(normalized.prefix(256))
    }

    private static func nonEmpty(_ value: String?) -> String? {
        let normalized = value?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return normalized.isEmpty ? nil : normalized
    }
}

private extension AWSEKSExecDescriptor {
    struct ParsedArguments {
        private static let valueOptions: Set<String> = [
            "--region",
            "--profile",
            "--cluster-name",
            "--cluster-id",
            "--role-arn",
            "--endpoint-url",
            "--output",
            "--query",
            "--color",
            "--ca-bundle",
            "--cli-read-timeout",
            "--cli-connect-timeout",
            "--cli-binary-format",
            "--cli-error-format"
        ]
        private static let flagOptions: Set<String> = [
            "--debug",
            "--no-verify-ssl",
            "--no-paginate",
            "--no-sign-request",
            "--no-cli-pager",
            "--cli-auto-prompt",
            "--no-cli-auto-prompt"
        ]
        private static let semanticValueOptions: Set<String> = [
            "--region",
            "--profile",
            "--cluster-name",
            "--cluster-id",
            "--role-arn",
            "--endpoint-url"
        ]

        var values: [String: String] = [:]
        var flags: Set<String> = []
        var positionals: [String] = []
        var unsupportedOptions: [String] = []

        init(_ arguments: [String]) throws {
            var index = 0
            while index < arguments.count {
                let argument = arguments[index]
                if argument.hasPrefix("--") {
                    let pair = Self.optionAndInlineValue(argument)
                    let option = pair.option
                    if Self.valueOptions.contains(option) {
                        let value: String
                        if let inline = pair.value {
                            value = inline
                        } else {
                            let nextIndex = arguments.index(after: index)
                            guard nextIndex < arguments.endIndex,
                                  !arguments[nextIndex].hasPrefix("--") else {
                                throw AWSEKSNativeAuthError.missingOptionValue(option)
                            }
                            value = arguments[nextIndex]
                            index = nextIndex
                        }
                        guard !value.isEmpty else {
                            throw AWSEKSNativeAuthError.missingOptionValue(option)
                        }
                        if Self.semanticValueOptions.contains(option), values[option] != nil {
                            throw AWSEKSNativeAuthError.duplicateOption(option)
                        }
                        values[option] = value
                    } else if Self.flagOptions.contains(option), pair.value == nil {
                        flags.insert(option)
                    } else {
                        unsupportedOptions.append(option)
                    }
                } else if argument.hasPrefix("-") {
                    unsupportedOptions.append(argument)
                } else {
                    positionals.append(argument)
                }
                index += 1
            }
        }

        private static func optionAndInlineValue(_ argument: String) -> (option: String, value: String?) {
            guard let separator = argument.firstIndex(of: "=") else {
                return (argument, nil)
            }
            return (
                String(argument[..<separator]),
                String(argument[argument.index(after: separator)...])
            )
        }
    }
}

/// Explicit AWS credentials used only to sign an STS GetCallerIdentity URL.
/// Persist this value only in a platform secret store such as Keychain.
public struct AWSEKSCredentials: Sendable, Equatable, CustomStringConvertible, CustomDebugStringConvertible {
    public let accessKeyID: String
    public let secretAccessKey: String
    public let sessionToken: String?
    public let expiration: Date?

    public init(
        accessKeyID: String,
        secretAccessKey: String,
        sessionToken: String? = nil,
        expiration: Date? = nil
    ) throws {
        let accessKeyID = accessKeyID.trimmingCharacters(in: .whitespacesAndNewlines)
        let secretAccessKey = secretAccessKey.trimmingCharacters(in: .whitespacesAndNewlines)
        let sessionToken = Self.nonEmpty(sessionToken)
        guard Self.isValidCredentialComponent(accessKeyID, maximumBytes: 256),
              Self.isValidCredentialComponent(secretAccessKey, maximumBytes: 4_096),
              sessionToken.map({ Self.isValidCredentialComponent($0, maximumBytes: 16_384) }) ?? true else {
            throw AWSEKSNativeAuthError.invalidCredentials
        }
        self.accessKeyID = accessKeyID
        self.secretAccessKey = secretAccessKey
        self.sessionToken = sessionToken
        self.expiration = expiration
    }

    public var description: String { "AWSEKSCredentials(<redacted>)" }
    public var debugDescription: String { description }

    private static func nonEmpty(_ value: String?) -> String? {
        let normalized = value?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return normalized.isEmpty ? nil : normalized
    }

    private static func isValidCredentialComponent(_ value: String, maximumBytes: Int) -> Bool {
        !value.isEmpty
            && value.utf8.count <= maximumBytes
            && value.unicodeScalars.allSatisfy { !CharacterSet.controlCharacters.contains($0) }
    }
}

public struct AWSEKSAuthenticationToken: Sendable, Equatable, CustomStringConvertible, CustomDebugStringConvertible {
    public let value: String
    public let expiration: Date

    public var description: String { "AWSEKSAuthenticationToken(<redacted>)" }
    public var debugDescription: String { description }
}

/// Creates the same bearer-token shape as `aws eks get-token` without invoking
/// an executable or contacting AWS. The Kubernetes EKS authenticator later
/// sends the signed GetCallerIdentity request to AWS STS.
public struct AWSEKSTokenSigner: Sendable {
    public init() {}

    public func token(
        for descriptor: AWSEKSExecDescriptor,
        credentials: AWSEKSCredentials,
        at signingDate: Date = Date()
    ) throws -> AWSEKSAuthenticationToken {
        let artifacts = try signingArtifacts(
            descriptor: descriptor,
            credentials: credentials,
            signingDate: signingDate
        )
        return AWSEKSAuthenticationToken(value: artifacts.token, expiration: artifacts.expiration)
    }

    func signingArtifacts(
        descriptor: AWSEKSExecDescriptor,
        credentials: AWSEKSCredentials,
        signingDate: Date
    ) throws -> AWSEKSSigningArtifacts {
        if let credentialExpiration = credentials.expiration,
           credentialExpiration <= signingDate.addingTimeInterval(30) {
            throw AWSEKSNativeAuthError.expiredCredentials
        }

        let endpoint = try Self.endpoint(for: descriptor.region)
        let timestamp = Self.timestamp(signingDate)
        let dateStamp = Self.dateStamp(signingDate)
        let scope = "\(dateStamp)/\(descriptor.region)/sts/aws4_request"
        let signedHeaders = "host;x-k8s-aws-id"

        var queryItems: [(String, String)] = [
            ("Action", "GetCallerIdentity"),
            ("Version", "2011-06-15"),
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
            ("X-Amz-Credential", "\(credentials.accessKeyID)/\(scope)"),
            ("X-Amz-Date", timestamp),
            ("X-Amz-Expires", "60"),
            ("X-Amz-SignedHeaders", signedHeaders)
        ]
        if let sessionToken = credentials.sessionToken {
            queryItems.append(("X-Amz-Security-Token", sessionToken))
        }
        let canonicalQuery = Self.canonicalQuery(queryItems)
        let canonicalHeaders = "host:\(endpoint.host)\nx-k8s-aws-id:\(descriptor.clusterIdentifier.value)\n"
        let payloadHash = Self.sha256Hex(Data())
        let canonicalRequest = [
            "GET",
            "/",
            canonicalQuery,
            canonicalHeaders,
            signedHeaders,
            payloadHash
        ].joined(separator: "\n")
        let stringToSign = [
            "AWS4-HMAC-SHA256",
            timestamp,
            scope,
            Self.sha256Hex(Data(canonicalRequest.utf8))
        ].joined(separator: "\n")

        let dateKey = Self.hmacSHA256(
            key: Data("AWS4\(credentials.secretAccessKey)".utf8),
            message: Data(dateStamp.utf8)
        )
        let regionKey = Self.hmacSHA256(key: dateKey, message: Data(descriptor.region.utf8))
        let serviceKey = Self.hmacSHA256(key: regionKey, message: Data("sts".utf8))
        let signingKey = Self.hmacSHA256(key: serviceKey, message: Data("aws4_request".utf8))
        let signature = Self.hex(Self.hmacSHA256(key: signingKey, message: Data(stringToSign.utf8)))
        let url = "https://\(endpoint.host)/?\(canonicalQuery)&X-Amz-Signature=\(signature)"
        let token = "k8s-aws-v1.\(Self.rawURLBase64(Data(url.utf8)))"
        guard token.utf8.count <= 4_096 else {
            throw AWSEKSNativeAuthError.tokenTooLarge
        }

        let normalExpiration = signingDate.addingTimeInterval(14 * 60)
        let expiration: Date
        if let credentialExpiration = credentials.expiration {
            expiration = min(normalExpiration, credentialExpiration.addingTimeInterval(-30))
        } else {
            expiration = normalExpiration
        }

        return AWSEKSSigningArtifacts(
            canonicalRequest: canonicalRequest,
            stringToSign: stringToSign,
            signature: signature,
            presignedURL: url,
            token: token,
            expiration: expiration
        )
    }

    private static func endpoint(for region: String) throws -> (host: String, partition: String) {
        if region.hasPrefix("us-iso-")
            || region.hasPrefix("us-isob-")
            || region.hasPrefix("us-isof-")
            || region.hasPrefix("eu-isoe-")
            || region.hasPrefix("eusc-") {
            throw AWSEKSNativeAuthError.unsupportedPartition
        }
        if region.hasPrefix("cn-") {
            return ("sts.\(region).amazonaws.com.cn", "aws-cn")
        }
        if region.hasPrefix("us-gov-") {
            return ("sts.\(region).amazonaws.com", "aws-us-gov")
        }
        let commercialPrefixes = [
            "af-", "ap-", "ca-", "eu-", "il-", "me-", "mx-", "sa-", "us-east-", "us-west-"
        ]
        guard commercialPrefixes.contains(where: region.hasPrefix) else {
            throw AWSEKSNativeAuthError.unsupportedPartition
        }
        return ("sts.\(region).amazonaws.com", "aws")
    }

    private static func canonicalQuery(_ items: [(String, String)]) -> String {
        let encodedItems: [(name: String, value: String)] = items.map { item in
            (name: percentEncode(item.0), value: percentEncode(item.1))
        }
        let sortedItems = encodedItems.sorted { lhs, rhs in
            lhs.name == rhs.name ? lhs.value < rhs.value : lhs.name < rhs.name
        }
        return sortedItems.map { item in
            item.name + "=" + item.value
        }.joined(separator: "&")
    }

    private static func percentEncode(_ value: String) -> String {
        var output = ""
        output.reserveCapacity(value.utf8.count)
        for byte in value.utf8 {
            switch byte {
            case 65...90, 97...122, 48...57, 45, 46, 95, 126:
                output.unicodeScalars.append(UnicodeScalar(byte))
            default:
                output.append("%")
                output.append(String(format: "%02X", byte))
            }
        }
        return output
    }

    private static func timestamp(_ date: Date) -> String {
        formatted(date, format: "yyyyMMdd'T'HHmmss'Z'")
    }

    private static func dateStamp(_ date: Date) -> String {
        formatted(date, format: "yyyyMMdd")
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
        let authenticationCode = HMAC<SHA256>.authenticationCode(
            for: message,
            using: SymmetricKey(data: key)
        )
        return Data(authenticationCode)
    }

    private static func hex(_ data: Data) -> String {
        data.map { String(format: "%02x", $0) }.joined()
    }

    private static func rawURLBase64(_ data: Data) -> String {
        data.base64EncodedString()
            .replacingOccurrences(of: "+", with: "-")
            .replacingOccurrences(of: "/", with: "_")
            .replacingOccurrences(of: "=", with: "")
    }
}

struct AWSEKSSigningArtifacts: Sendable, Equatable {
    let canonicalRequest: String
    let stringToSign: String
    let signature: String
    let presignedURL: String
    let token: String
    let expiration: Date
}
