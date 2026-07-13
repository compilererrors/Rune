import Foundation

public enum KubernetesNativeCredentialProviderError: Error, LocalizedError, Sendable, Equatable {
    case profileMissing(provider: KubernetesNativeAuthProviderKind)
    case profileCorrupted
    case unsupportedProvider(provider: KubernetesNativeAuthProviderKind)
    case invalidProviderConfiguration(provider: KubernetesNativeAuthProviderKind)

    public var errorDescription: String? {
        switch self {
        case .profileMissing(let provider):
            return "Connect \(provider.displayName) credentials in Rune before using this context."
        case .profileCorrupted:
            return "The native Kubernetes authentication profile in Keychain could not be decoded. Reconnect the profile."
        case .unsupportedProvider(let provider):
            return "Native \(provider.displayName) login is not configured in this Rune build."
        case .invalidProviderConfiguration(let provider):
            return "The kubeconfig does not contain enough information for native \(provider.displayName) authentication."
        }
    }
}

public struct KubernetesNativeAuthProfileStatus: Sendable, Equatable {
    public let bindingID: String
    public let provider: KubernetesNativeAuthProviderKind
    public let isConnected: Bool
    public let expiresAt: Date?

    public init(
        bindingID: String,
        provider: KubernetesNativeAuthProviderKind,
        isConnected: Bool,
        expiresAt: Date?
    ) {
        self.bindingID = bindingID
        self.provider = provider
        self.isConnected = isConnected
        self.expiresAt = expiresAt
    }
}

public protocol KubernetesNativeAuthConfiguring: Sendable {
    func status(for request: KubernetesNativeCredentialRequest) async throws -> KubernetesNativeAuthProfileStatus
    func bindAWSCredentials(
        to request: KubernetesNativeCredentialRequest,
        credentials: AWSEKSCredentials,
        displayName: String
    ) async throws
    func bindAKSServicePrincipal(
        to request: KubernetesNativeCredentialRequest,
        clientSecret: String,
        displayName: String
    ) async throws
    func bindGCPServiceAccount(
        to request: KubernetesNativeCredentialRequest,
        serviceAccountJSON: Data,
        displayName: String
    ) async throws
    func removeProfile(for bindingID: String) async throws
}

/// Resolves provider credentials entirely in-process. Long-lived secret material is stored in
/// Keychain through `KubernetesNativeAuthProfileStoring`; access tokens remain in memory.
public actor DefaultKubernetesNativeCredentialProvider: KubernetesNativeCredentialProviding, KubernetesNativeAuthConfiguring {
    public static let shared = DefaultKubernetesNativeCredentialProvider()

    private struct AWSCredentialSecret: Codable {
        let accessKeyID: String
        let secretAccessKey: String
        let sessionToken: String?
        let expiration: Date?
    }

    private struct OIDCCredentialSecret: Codable {
        let clientSecret: String?
        let refreshToken: String
    }

    private struct CachedCredential {
        let value: KubernetesNativeCredential

        func isUsable(at date: Date) -> Bool {
            guard let expiresAt = value.expiresAt else { return true }
            return expiresAt.timeIntervalSince(date) > 60
        }
    }

    private struct PendingProfileUpdate: Sendable {
        let profile: KubernetesNativeAuthProfile
        let secret: Data
    }

    private struct CredentialLoadResult: Sendable {
        let credential: KubernetesNativeCredential?
        let profileUpdate: PendingProfileUpdate?

        init(_ credential: KubernetesNativeCredential?, profileUpdate: PendingProfileUpdate? = nil) {
            self.credential = credential
            self.profileUpdate = profileUpdate
        }
    }

    private struct InFlightCredential {
        let id: UUID
        let generation: UInt64
        let task: Task<CredentialLoadResult, Error>
    }

    private let profileStore: any KubernetesNativeAuthProfileStoring
    private let oidcClient: OIDCNativeAuthClient
    private let aksHTTPClient: any AKSServicePrincipalTokenHTTPClient
    private let gcpHTTPClient: any GCPServiceAccountHTTPClient
    private let now: @Sendable () -> Date
    private var cache: [String: CachedCredential] = [:]
    private var inFlight: [String: InFlightCredential] = [:]
    private var bindingGenerations: [String: UInt64] = [:]
    private var forceRefreshBindings = Set<String>()

    public init(
        profileStore: any KubernetesNativeAuthProfileStoring = KeychainKubernetesNativeAuthProfileStore(),
        oidcClient: OIDCNativeAuthClient = OIDCNativeAuthClient(),
        aksHTTPClient: any AKSServicePrincipalTokenHTTPClient = URLSessionAKSServicePrincipalTokenHTTPClient(),
        gcpHTTPClient: any GCPServiceAccountHTTPClient = GCPServiceAccountURLSessionHTTPClient(),
        now: @escaping @Sendable () -> Date = Date.init
    ) {
        self.profileStore = profileStore
        self.oidcClient = oidcClient
        self.aksHTTPClient = aksHTTPClient
        self.gcpHTTPClient = gcpHTTPClient
        self.now = now
    }

    public func credential(for request: KubernetesNativeCredentialRequest) async throws -> KubernetesNativeCredential? {
        if let cached = cache[request.bindingID], cached.isUsable(at: now()), !forceRefreshBindings.contains(request.bindingID) {
            return cached.value
        }
        if let flight = inFlight[request.bindingID] {
            let result = try await flight.task.value
            guard bindingGenerations[request.bindingID, default: 0] == flight.generation else {
                throw CancellationError()
            }
            return result.credential
        }

        let shouldForceRefresh = forceRefreshBindings.contains(request.bindingID)
        let generation = bindingGenerations[request.bindingID, default: 0]
        let task = Task { [profileStore, oidcClient, aksHTTPClient, gcpHTTPClient, now] in
            try await Self.loadCredential(
                for: request,
                profileStore: profileStore,
                oidcClient: oidcClient,
                aksHTTPClient: aksHTTPClient,
                gcpHTTPClient: gcpHTTPClient,
                now: now(),
                forceRefresh: shouldForceRefresh
            )
        }
        let flightID = UUID()
        inFlight[request.bindingID] = InFlightCredential(
            id: flightID,
            generation: generation,
            task: task
        )
        do {
            let result = try await task.value
            if inFlight[request.bindingID]?.id == flightID {
                inFlight.removeValue(forKey: request.bindingID)
            }
            guard bindingGenerations[request.bindingID, default: 0] == generation else {
                throw CancellationError()
            }
            if let update = result.profileUpdate {
                try await profileStore.save(profile: update.profile, secret: update.secret)
                guard bindingGenerations[request.bindingID, default: 0] == generation else {
                    throw CancellationError()
                }
            }
            forceRefreshBindings.remove(request.bindingID)
            if let credential = result.credential {
                cache[request.bindingID] = CachedCredential(value: credential)
            }
            return result.credential
        } catch {
            if inFlight[request.bindingID]?.id == flightID {
                inFlight.removeValue(forKey: request.bindingID)
            }
            throw error
        }
    }

    public func invalidateCredential(for bindingID: String) async {
        resetTransientState(for: bindingID)
        forceRefreshBindings.insert(bindingID)
    }

    public func status(for request: KubernetesNativeCredentialRequest) async throws -> KubernetesNativeAuthProfileStatus {
        let stored = try await profileStore.storedProfile(for: request.bindingID)
        let currentDate = now()
        let hasStoredCredential = Self.hasUsableStoredCredential(
            stored,
            provider: request.provider,
            now: currentDate
        )
        let hasEmbeddedOIDCCredential: Bool
        if request.provider == .oidc,
           let token = request.authProvider?.configuration["id-token"],
           let metadata = try? OIDCJWTMetadata.parse(token),
           let expiration = metadata.expiration {
            hasEmbeddedOIDCCredential = expiration > currentDate
        } else {
            hasEmbeddedOIDCCredential = false
        }
        return KubernetesNativeAuthProfileStatus(
            bindingID: request.bindingID,
            provider: request.provider,
            isConnected: hasStoredCredential || hasEmbeddedOIDCCredential,
            expiresAt: cache[request.bindingID]?.value.expiresAt
        )
    }

    private static func hasUsableStoredCredential(
        _ stored: KubernetesNativeAuthStoredProfile?,
        provider: KubernetesNativeAuthProviderKind,
        now: Date
    ) -> Bool {
        guard let stored, stored.profile.provider == provider, let secret = stored.secret, !secret.isEmpty else {
            return false
        }
        switch provider {
        case .awsEKS:
            guard let value = try? JSONDecoder().decode(AWSCredentialSecret.self, from: secret),
                  (try? AWSEKSCredentials(
                      accessKeyID: value.accessKeyID,
                      secretAccessKey: value.secretAccessKey,
                      sessionToken: value.sessionToken,
                      expiration: value.expiration
                  )) != nil else { return false }
            return value.expiration.map { $0 > now } ?? true
        case .azureKubelogin:
            return (try? JSONDecoder().decode(AKSServicePrincipalCredentials.self, from: secret)) != nil
        case .googleGKE:
            return (try? GCPServiceAccountCredential(jsonData: secret)) != nil
        case .oidc:
            return (try? JSONDecoder().decode(OIDCCredentialSecret.self, from: secret)) != nil
        }
    }

    public func bindAWSCredentials(
        to request: KubernetesNativeCredentialRequest,
        credentials: AWSEKSCredentials,
        displayName: String = "AWS EKS"
    ) async throws {
        guard request.provider == .awsEKS,
              let exec = request.exec,
              try AWSEKSExecDescriptor.parseIfSupported(
                  command: exec.command,
                  arguments: exec.arguments,
                  environment: Self.environmentDictionary(exec.environment)
              ) != nil else {
            throw KubernetesNativeCredentialProviderError.invalidProviderConfiguration(provider: .awsEKS)
        }
        let payload = AWSCredentialSecret(
            accessKeyID: credentials.accessKeyID,
            secretAccessKey: credentials.secretAccessKey,
            sessionToken: credentials.sessionToken,
            expiration: credentials.expiration
        )
        let profile = KubernetesNativeAuthProfile(
            bindingID: request.bindingID,
            provider: .awsEKS,
            displayName: displayName
        )
        resetTransientState(for: request.bindingID)
        try await profileStore.save(profile: profile, secret: try JSONEncoder().encode(payload))
    }

    public func bindAKSServicePrincipal(
        to request: KubernetesNativeCredentialRequest,
        clientSecret: String,
        displayName: String = "Azure AKS"
    ) async throws {
        guard request.provider == .azureKubelogin,
              let exec = request.exec,
              let descriptor = try AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
                  command: exec.command,
                  arguments: exec.arguments
              ) else {
            throw KubernetesNativeCredentialProviderError.invalidProviderConfiguration(provider: .azureKubelogin)
        }
        let credentials = try AKSServicePrincipalCredentials(
            clientID: descriptor.clientID,
            clientSecret: clientSecret
        )
        let profile = KubernetesNativeAuthProfile(
            bindingID: request.bindingID,
            provider: .azureKubelogin,
            displayName: displayName
        )
        resetTransientState(for: request.bindingID)
        try await profileStore.save(profile: profile, secret: try JSONEncoder().encode(credentials))
    }

    public func bindGCPServiceAccount(
        to request: KubernetesNativeCredentialRequest,
        serviceAccountJSON: Data,
        displayName: String = "Google GKE"
    ) async throws {
        guard request.provider == .googleGKE, request.exec != nil else {
            throw KubernetesNativeCredentialProviderError.invalidProviderConfiguration(provider: .googleGKE)
        }
        _ = try GCPServiceAccountCredential(jsonData: serviceAccountJSON)
        let profile = KubernetesNativeAuthProfile(
            bindingID: request.bindingID,
            provider: .googleGKE,
            displayName: displayName
        )
        resetTransientState(for: request.bindingID)
        try await profileStore.save(profile: profile, secret: serviceAccountJSON)
    }

    public func removeProfile(for bindingID: String) async throws {
        resetTransientState(for: bindingID)
        try await profileStore.removeProfile(for: bindingID)
    }

    private func resetTransientState(for bindingID: String) {
        cache.removeValue(forKey: bindingID)
        inFlight.removeValue(forKey: bindingID)?.task.cancel()
        bindingGenerations[bindingID, default: 0] &+= 1
        forceRefreshBindings.remove(bindingID)
    }

    private static func loadCredential(
        for request: KubernetesNativeCredentialRequest,
        profileStore: any KubernetesNativeAuthProfileStoring,
        oidcClient: OIDCNativeAuthClient,
        aksHTTPClient: any AKSServicePrincipalTokenHTTPClient,
        gcpHTTPClient: any GCPServiceAccountHTTPClient,
        now: Date,
        forceRefresh: Bool
    ) async throws -> CredentialLoadResult {
        switch request.provider {
        case .awsEKS:
            return CredentialLoadResult(try await loadAWSCredential(for: request, profileStore: profileStore, now: now))
        case .oidc:
            return try await loadOIDCCredential(
                for: request,
                profileStore: profileStore,
                client: oidcClient,
                forceRefresh: forceRefresh
            )
        case .azureKubelogin:
            return CredentialLoadResult(try await loadAKSCredential(
                for: request,
                profileStore: profileStore,
                httpClient: aksHTTPClient
            ))
        case .googleGKE:
            return CredentialLoadResult(try await loadGCPCredential(
                for: request,
                profileStore: profileStore,
                httpClient: gcpHTTPClient,
                now: now
            ))
        }
    }

    private static func loadAKSCredential(
        for request: KubernetesNativeCredentialRequest,
        profileStore: any KubernetesNativeAuthProfileStoring,
        httpClient: any AKSServicePrincipalTokenHTTPClient
    ) async throws -> KubernetesNativeCredential? {
        guard let stored = try await profileStore.storedProfile(for: request.bindingID) else { return nil }
        guard stored.profile.provider == .azureKubelogin,
              let secret = stored.secret,
              let credentials = try? JSONDecoder().decode(AKSServicePrincipalCredentials.self, from: secret),
              let exec = request.exec else {
            throw KubernetesNativeCredentialProviderError.profileCorrupted
        }
        guard let descriptor = try AKSKubeloginServicePrincipalDescriptor.parseIfSupported(
            command: exec.command,
            arguments: exec.arguments
        ) else {
            throw KubernetesNativeCredentialProviderError.invalidProviderConfiguration(provider: .azureKubelogin)
        }
        let service = AKSServicePrincipalTokenService(httpClient: httpClient)
        let token = try await service.token(descriptor: descriptor, credentials: credentials)
        return KubernetesNativeCredential(bearerToken: token.value, expiresAt: token.expiration)
    }

    private static func loadGCPCredential(
        for request: KubernetesNativeCredentialRequest,
        profileStore: any KubernetesNativeAuthProfileStoring,
        httpClient: any GCPServiceAccountHTTPClient,
        now: Date
    ) async throws -> KubernetesNativeCredential? {
        guard let stored = try await profileStore.storedProfile(for: request.bindingID) else { return nil }
        guard stored.profile.provider == .googleGKE, let secret = stored.secret else {
            throw KubernetesNativeCredentialProviderError.profileCorrupted
        }
        let provider = try GCPServiceAccountCredentialProvider(
            serviceAccountJSON: secret,
            httpClient: httpClient,
            now: { now }
        )
        let token = try await provider.accessToken()
        return KubernetesNativeCredential(bearerToken: token.value, expiresAt: token.expiration)
    }

    private static func loadAWSCredential(
        for request: KubernetesNativeCredentialRequest,
        profileStore: any KubernetesNativeAuthProfileStoring,
        now: Date
    ) async throws -> KubernetesNativeCredential? {
        guard let stored = try await profileStore.storedProfile(for: request.bindingID) else { return nil }
        guard stored.profile.provider == .awsEKS,
              let data = stored.secret,
              let secret = try? JSONDecoder().decode(AWSCredentialSecret.self, from: data),
              let exec = request.exec else {
            throw KubernetesNativeCredentialProviderError.profileCorrupted
        }
        guard let descriptor = try AWSEKSExecDescriptor.parseIfSupported(
            command: exec.command,
            arguments: exec.arguments,
            environment: environmentDictionary(exec.environment)
        ) else {
            throw KubernetesNativeCredentialProviderError.invalidProviderConfiguration(provider: .awsEKS)
        }
        let credentials = try AWSEKSCredentials(
            accessKeyID: secret.accessKeyID,
            secretAccessKey: secret.secretAccessKey,
            sessionToken: secret.sessionToken,
            expiration: secret.expiration
        )
        let token = try AWSEKSTokenSigner().token(for: descriptor, credentials: credentials, at: now)
        return KubernetesNativeCredential(bearerToken: token.value, expiresAt: token.expiration)
    }

    private static func loadOIDCCredential(
        for request: KubernetesNativeCredentialRequest,
        profileStore: any KubernetesNativeAuthProfileStoring,
        client: OIDCNativeAuthClient,
        forceRefresh: Bool
    ) async throws -> CredentialLoadResult {
        guard let authProvider = request.authProvider,
              authProvider.name.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() == "oidc" else {
            return CredentialLoadResult(nil)
        }
        let stored = try await profileStore.storedProfile(for: request.bindingID)
        let storedSecret: OIDCCredentialSecret?
        if let data = stored?.secret {
            guard let decoded = try? JSONDecoder().decode(OIDCCredentialSecret.self, from: data) else {
                throw KubernetesNativeCredentialProviderError.profileCorrupted
            }
            storedSecret = decoded
        } else {
            storedSecret = nil
        }

        var configuration = authProvider.configuration
        if let clientSecret = storedSecret?.clientSecret { configuration["client-secret"] = clientSecret }
        if let refreshToken = storedSecret?.refreshToken { configuration["refresh-token"] = refreshToken }
        let oidcConfiguration = try OIDCAuthProviderConfiguration(
            authProviderName: authProvider.name,
            config: configuration
        )
        let session = OIDCNativeCredentialSession(
            configuration: oidcConfiguration,
            client: client
        )
        let credential = forceRefresh
            ? try await session.forceRefresh()
            : try await session.credential()
        let rotated = await session.consumeRefreshTokenUpdate()
        let effectiveRefreshToken = rotated ?? storedSecret?.refreshToken ?? oidcConfiguration.refreshToken
        if let effectiveRefreshToken {
            let secret = OIDCCredentialSecret(
                clientSecret: storedSecret?.clientSecret ?? oidcConfiguration.clientSecret,
                refreshToken: effectiveRefreshToken
            )
            let profile = KubernetesNativeAuthProfile(
                id: stored?.profile.id ?? UUID(),
                bindingID: request.bindingID,
                provider: .oidc,
                displayName: stored?.profile.displayName ?? "OIDC",
                configuration: [
                    "issuer": oidcConfiguration.issuerURL.absoluteString,
                    "client-id": oidcConfiguration.clientID
                ],
                createdAt: stored?.profile.createdAt ?? Date(),
                updatedAt: Date()
            )
            let update = PendingProfileUpdate(
                profile: profile,
                secret: try JSONEncoder().encode(secret)
            )
            return CredentialLoadResult(
                KubernetesNativeCredential(
                    bearerToken: credential.idToken,
                    expiresAt: credential.expiration
                ),
                profileUpdate: update
            )
        }
        return CredentialLoadResult(KubernetesNativeCredential(
            bearerToken: credential.idToken,
            expiresAt: credential.expiration
        ))
    }

    private static func environmentDictionary(
        _ entries: [KubernetesNativeAuthEnvironmentEntry]
    ) -> [String: String] {
        entries.reduce(into: [:]) { result, entry in
            result[entry.name] = entry.value
        }
    }
}
