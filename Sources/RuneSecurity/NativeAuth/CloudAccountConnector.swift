import Foundation

// MARK: - Opaque identities

public struct CloudAccountID: RawRepresentable, Codable, Hashable, Sendable {
    public let rawValue: UUID

    public init(rawValue: UUID = UUID()) {
        self.rawValue = rawValue
    }
}

public struct CloudDiscoveryScopeID: RawRepresentable, Codable, Hashable, Sendable {
    public let rawValue: UUID

    public init(rawValue: UUID = UUID()) {
        self.rawValue = rawValue
    }
}

public struct CloudClusterCandidateID: RawRepresentable, Codable, Hashable, Sendable {
    public let rawValue: UUID

    public init(rawValue: UUID = UUID()) {
        self.rawValue = rawValue
    }
}

public struct CloudConnectionID: RawRepresentable, Codable, Hashable, Sendable {
    public let rawValue: UUID

    public init(rawValue: UUID = UUID()) {
        self.rawValue = rawValue
    }
}

public struct CloudAccountOperationID: RawRepresentable, Codable, Hashable, Sendable {
    public let rawValue: UUID

    public init(rawValue: UUID = UUID()) {
        self.rawValue = rawValue
    }
}

public struct CloudAccountGeneration: RawRepresentable, Codable, Hashable, Comparable, Sendable {
    public let rawValue: UInt64

    public init(rawValue: UInt64) {
        self.rawValue = rawValue
    }

    public static func < (lhs: Self, rhs: Self) -> Bool {
        lhs.rawValue < rhs.rawValue
    }
}

// MARK: - Provider-neutral account and discovery models

public enum CloudAccountProvider: String, Codable, CaseIterable, Sendable {
    case azure
    case amazonWebServices
    case googleCloud

    public var clusterProvider: CloudKubeConfigProvider {
        switch self {
        case .azure: return .aks
        case .amazonWebServices: return .eks
        case .googleCloud: return .gke
        }
    }
}

public enum CloudAccountHealth: String, Codable, Sendable {
    case connected
    case limited
    case requiresReauthentication
    case offline
    case providerUnavailable
}

/// Account metadata is deliberately separate from credentials. Connectors own secret persistence.
public struct CloudAccountRecord: Codable, Equatable, Sendable {
    public let id: CloudAccountID
    public let provider: CloudAccountProvider
    public let credentialGeneration: CloudAccountGeneration
    public let localLabel: String
    public let health: CloudAccountHealth
    public let lastSuccessfulSync: Date?
    public let discoverableClusterCount: Int

    public init(
        id: CloudAccountID,
        provider: CloudAccountProvider,
        credentialGeneration: CloudAccountGeneration,
        localLabel: String,
        health: CloudAccountHealth = .connected,
        lastSuccessfulSync: Date? = nil,
        discoverableClusterCount: Int = 0
    ) {
        self.id = id
        self.provider = provider
        self.credentialGeneration = credentialGeneration
        self.localLabel = localLabel.trimmingCharacters(in: .whitespacesAndNewlines)
        self.health = health
        self.lastSuccessfulSync = lastSuccessfulSync
        self.discoverableClusterCount = max(0, discoverableClusterCount)
    }

    public var requiresReauthentication: Bool {
        health == .requiresReauthentication
    }

    func updatingAfterSync(clusterCount: Int, at date: Date?) -> Self {
        Self(
            id: id,
            provider: provider,
            credentialGeneration: credentialGeneration,
            localLabel: localLabel,
            health: health,
            lastSuccessfulSync: date,
            discoverableClusterCount: clusterCount
        )
    }

    func updatingLocalLabel(_ localLabel: String) -> Self {
        Self(
            id: id,
            provider: provider,
            credentialGeneration: credentialGeneration,
            localLabel: localLabel,
            health: health,
            lastSuccessfulSync: lastSuccessfulSync,
            discoverableClusterCount: discoverableClusterCount
        )
    }
}

public enum CloudDiscoveryScopeKind: String, Codable, Sendable {
    case tenant
    case subscription
    case organization
    case folder
    case account
    case role
    case project
    case region
    case location
}

public struct CloudDiscoveryScope: Codable, Equatable, Sendable {
    public let id: CloudDiscoveryScopeID
    public let provider: CloudAccountProvider
    public let accountID: CloudAccountID
    public let credentialGeneration: CloudAccountGeneration
    public let parentID: CloudDiscoveryScopeID?
    public let kind: CloudDiscoveryScopeKind
    public let displayName: String

    public init(
        id: CloudDiscoveryScopeID,
        provider: CloudAccountProvider,
        accountID: CloudAccountID,
        credentialGeneration: CloudAccountGeneration,
        parentID: CloudDiscoveryScopeID? = nil,
        kind: CloudDiscoveryScopeKind,
        displayName: String
    ) {
        self.id = id
        self.provider = provider
        self.accountID = accountID
        self.credentialGeneration = credentialGeneration
        self.parentID = parentID
        self.kind = kind
        self.displayName = displayName
    }
}

public enum CloudClusterCertificateStatus: String, Codable, Sendable {
    case trustedDataAvailable
    case systemTrust
    case insecure
    case invalid
    case unknown
}

public enum CloudClusterReachability: String, Codable, Sendable {
    case unknown
    case reachable
    case privateEndpoint
    case unreachable
}

public enum CloudClusterAuthenticationMethod: String, Codable, Sendable {
    case nativeAccount
    case guidedCLI
    case advancedStaticCredential
}

public struct CloudClusterCandidate: Codable, Equatable, Sendable {
    public let id: CloudClusterCandidateID
    public let provider: CloudAccountProvider
    public let accountID: CloudAccountID
    public let credentialGeneration: CloudAccountGeneration
    public let scopeIDs: [CloudDiscoveryScopeID]
    public let displayName: String
    public let endpointHost: String
    public let certificateStatus: CloudClusterCertificateStatus
    public let authenticationMethod: CloudClusterAuthenticationMethod
    public let reachability: CloudClusterReachability
    public let isAlreadyAdded: Bool

    public init(
        id: CloudClusterCandidateID,
        provider: CloudAccountProvider,
        accountID: CloudAccountID,
        credentialGeneration: CloudAccountGeneration,
        scopeIDs: [CloudDiscoveryScopeID],
        displayName: String,
        endpointHost: String,
        certificateStatus: CloudClusterCertificateStatus,
        authenticationMethod: CloudClusterAuthenticationMethod = .nativeAccount,
        reachability: CloudClusterReachability = .unknown,
        isAlreadyAdded: Bool = false
    ) {
        self.id = id
        self.provider = provider
        self.accountID = accountID
        self.credentialGeneration = credentialGeneration
        self.scopeIDs = scopeIDs
        self.displayName = displayName
        self.endpointHost = endpointHost
        self.certificateStatus = certificateStatus
        self.authenticationMethod = authenticationMethod
        self.reachability = reachability
        self.isAlreadyAdded = isAlreadyAdded
    }
}

public enum CloudConnectionSource: String, Codable, Sendable {
    case nativeAccount
    case guidedCLI
    case importedFile
    case watchedFolder
    case defaultKubeConfig
    case advancedStaticCredential
}

public struct CloudConnectionProvenance: Codable, Equatable, Sendable {
    public let id: CloudConnectionID
    public let generation: CloudAccountGeneration
    public let source: CloudConnectionSource
    public let provider: CloudAccountProvider?
    public let accountID: CloudAccountID?
    public let candidateID: CloudClusterCandidateID?

    public init(
        id: CloudConnectionID,
        generation: CloudAccountGeneration,
        source: CloudConnectionSource,
        provider: CloudAccountProvider? = nil,
        accountID: CloudAccountID? = nil,
        candidateID: CloudClusterCandidateID? = nil
    ) {
        self.id = id
        self.generation = generation
        self.source = source
        self.provider = provider
        self.accountID = accountID
        self.candidateID = candidateID
    }
}

// MARK: - Privacy-safe failures and diagnostics

public enum CloudAccountOperationStage: String, Codable, Sendable {
    case authorization
    case accountRefresh
    case scopeDiscovery
    case clusterDiscovery
    case credentialExchange
    case endpointAccess
    case kubernetesAuthorization
    case localDisconnect
}

public enum CloudAccountFailureClass: String, Codable, Sendable {
    case canceled
    case invalidAuthorizationResponse
    case invalidProviderResponse
    case permissionDenied
    case requiresReauthentication
    case throttled
    case offline
    case timeout
    case providerUnavailable
    case localSecretRemovalFailed
}

public enum CloudAccountRecoveryAction: String, Codable, Sendable {
    case retry
    case reauthorize
    case changeScope
    case openPermissionHelp
    case useCLIFallback
    case runAuthDoctor
}

public struct CloudAccountFailure: Codable, Error, LocalizedError, Equatable, Sendable {
    public let stage: CloudAccountOperationStage
    public let classification: CloudAccountFailureClass
    public let isRetryable: Bool
    public let recoveryAction: CloudAccountRecoveryAction
    public let retryAfter: TimeInterval?

    public init(
        stage: CloudAccountOperationStage,
        classification: CloudAccountFailureClass,
        isRetryable: Bool,
        recoveryAction: CloudAccountRecoveryAction,
        retryAfter: TimeInterval? = nil
    ) {
        self.stage = stage
        self.classification = classification
        self.isRetryable = isRetryable
        self.recoveryAction = recoveryAction
        if let retryAfter, retryAfter.isFinite, retryAfter >= 0 {
            self.retryAfter = min(retryAfter, 30)
        } else {
            self.retryAfter = nil
        }
    }

    public var errorDescription: String? {
        switch classification {
        case .canceled:
            return "The cloud account operation was canceled."
        case .invalidAuthorizationResponse:
            return "Rune rejected an invalid account authorization response."
        case .invalidProviderResponse:
            return "Rune rejected an invalid cloud provider response."
        case .permissionDenied:
            return "The connected account does not have permission for this operation."
        case .requiresReauthentication:
            return "The cloud account must be authorized again."
        case .throttled:
            return "The cloud provider temporarily limited this operation."
        case .offline:
            return "The cloud provider could not be reached while offline."
        case .timeout:
            return "The cloud account operation timed out."
        case .providerUnavailable:
            return "The cloud provider is temporarily unavailable."
        case .localSecretRemovalFailed:
            return "Rune could not remove the local account credentials."
        }
    }
}

public struct CloudAccountDiagnostic: Codable, Equatable, Sendable {
    public let provider: CloudAccountProvider
    public let stage: CloudAccountOperationStage
    public let classification: CloudAccountFailureClass?
    public let isRetryable: Bool
    public let recoveryAction: CloudAccountRecoveryAction?

    public init(
        provider: CloudAccountProvider,
        stage: CloudAccountOperationStage,
        classification: CloudAccountFailureClass? = nil,
        isRetryable: Bool = false,
        recoveryAction: CloudAccountRecoveryAction? = nil
    ) {
        self.provider = provider
        self.stage = stage
        self.classification = classification
        self.isRetryable = isRetryable
        self.recoveryAction = recoveryAction
    }
}

// MARK: - Connector contract

public struct CloudAccountConnectRequest: Sendable, Equatable {
    public let operationID: CloudAccountOperationID
    public let operationGeneration: CloudAccountGeneration
    public let localLabel: String

    public init(
        operationID: CloudAccountOperationID,
        operationGeneration: CloudAccountGeneration,
        localLabel: String
    ) {
        self.operationID = operationID
        self.operationGeneration = operationGeneration
        self.localLabel = localLabel.trimmingCharacters(in: .whitespacesAndNewlines)
    }
}

public struct CloudAccountBoundRequest: Sendable, Equatable {
    public let operationID: CloudAccountOperationID
    public let operationGeneration: CloudAccountGeneration
    public let accountID: CloudAccountID
    public let credentialGeneration: CloudAccountGeneration

    public init(
        operationID: CloudAccountOperationID,
        operationGeneration: CloudAccountGeneration,
        accountID: CloudAccountID,
        credentialGeneration: CloudAccountGeneration
    ) {
        self.operationID = operationID
        self.operationGeneration = operationGeneration
        self.accountID = accountID
        self.credentialGeneration = credentialGeneration
    }
}

public struct CloudAccountPageRequest: Sendable, Equatable {
    public let operationID: CloudAccountOperationID
    public let operationGeneration: CloudAccountGeneration
    public let accountID: CloudAccountID
    public let credentialGeneration: CloudAccountGeneration
    public let selectedScopeIDs: Set<CloudDiscoveryScopeID>
    public let pageToken: String?
    public let maximumResponseBytes: Int
    public let maximumConcurrentScopeRequests: Int

    public init(
        operationID: CloudAccountOperationID,
        operationGeneration: CloudAccountGeneration,
        accountID: CloudAccountID,
        credentialGeneration: CloudAccountGeneration,
        selectedScopeIDs: Set<CloudDiscoveryScopeID> = [],
        pageToken: String? = nil,
        maximumResponseBytes: Int,
        maximumConcurrentScopeRequests: Int
    ) {
        self.operationID = operationID
        self.operationGeneration = operationGeneration
        self.accountID = accountID
        self.credentialGeneration = credentialGeneration
        self.selectedScopeIDs = selectedScopeIDs
        self.pageToken = pageToken
        self.maximumResponseBytes = maximumResponseBytes
        self.maximumConcurrentScopeRequests = maximumConcurrentScopeRequests
    }
}

public struct CloudDiscoveryScopePage: Sendable, Equatable {
    public let scopes: [CloudDiscoveryScope]
    public let nextPageToken: String?
    public let issues: [CloudAccountFailure]

    public init(
        scopes: [CloudDiscoveryScope],
        nextPageToken: String? = nil,
        issues: [CloudAccountFailure] = []
    ) {
        self.scopes = scopes
        self.nextPageToken = nextPageToken
        self.issues = issues
    }
}

public struct CloudClusterDiscoveryPage: Sendable, Equatable {
    public let candidates: [CloudClusterCandidate]
    public let nextPageToken: String?
    public let issues: [CloudAccountFailure]

    public init(
        candidates: [CloudClusterCandidate],
        nextPageToken: String? = nil,
        issues: [CloudAccountFailure] = []
    ) {
        self.candidates = candidates
        self.nextPageToken = nextPageToken
        self.issues = issues
    }
}

public protocol CloudAccountConnector: Sendable {
    var provider: CloudAccountProvider { get }

    func connect(_ request: CloudAccountConnectRequest) async throws -> CloudAccountRecord
    func refresh(_ request: CloudAccountBoundRequest) async throws -> CloudAccountRecord
    func discoveryScopes(_ request: CloudAccountPageRequest) async throws -> CloudDiscoveryScopePage
    func discoverClusters(_ request: CloudAccountPageRequest) async throws -> CloudClusterDiscoveryPage

    /// Removes local secrets for exactly this account and credential generation. Provider-side
    /// consent revocation is a separate, optional action and must not block local cleanup.
    func disconnect(_ request: CloudAccountBoundRequest) async throws
    func diagnostics(_ request: CloudAccountBoundRequest) async -> [CloudAccountDiagnostic]
}

public struct CloudAccountDiscoveryLimits: Sendable, Equatable {
    public let maximumPages: Int
    public let maximumScopes: Int
    public let maximumClusters: Int
    public let maximumResponseBytes: Int
    public let maximumConcurrentScopeRequests: Int
    public let maximumRetriesPerPage: Int
    public let initialRetryDelay: TimeInterval

    public init(
        maximumPages: Int = 20,
        maximumScopes: Int = 2_000,
        maximumClusters: Int = 5_000,
        maximumResponseBytes: Int = 1_048_576,
        maximumConcurrentScopeRequests: Int = 4,
        maximumRetriesPerPage: Int = 2,
        initialRetryDelay: TimeInterval = 0.25
    ) {
        self.maximumPages = max(1, min(maximumPages, 100))
        self.maximumScopes = max(1, min(maximumScopes, 10_000))
        self.maximumClusters = max(1, min(maximumClusters, 25_000))
        self.maximumResponseBytes = max(1_024, min(maximumResponseBytes, 4_194_304))
        self.maximumConcurrentScopeRequests = max(1, min(maximumConcurrentScopeRequests, 16))
        self.maximumRetriesPerPage = max(0, min(maximumRetriesPerPage, 5))
        if initialRetryDelay.isFinite {
            self.initialRetryDelay = max(0, min(initialRetryDelay, 5))
        } else {
            self.initialRetryDelay = 0.25
        }
    }
}

public struct CloudAccountDiscoverySnapshot: Sendable, Equatable {
    public let accountID: CloudAccountID
    public let credentialGeneration: CloudAccountGeneration
    public let operationGeneration: CloudAccountGeneration
    public let scopes: [CloudDiscoveryScope]
    public let candidates: [CloudClusterCandidate]
    public let issues: [CloudAccountFailure]
    public let isPartial: Bool

    public init(
        accountID: CloudAccountID,
        credentialGeneration: CloudAccountGeneration,
        operationGeneration: CloudAccountGeneration,
        scopes: [CloudDiscoveryScope],
        candidates: [CloudClusterCandidate],
        issues: [CloudAccountFailure],
        isPartial: Bool
    ) {
        self.accountID = accountID
        self.credentialGeneration = credentialGeneration
        self.operationGeneration = operationGeneration
        self.scopes = scopes
        self.candidates = candidates
        self.issues = issues
        self.isPartial = isPartial
    }
}

public enum CloudAccountCoordinatorError: Error, LocalizedError, Equatable, Sendable {
    case connectorUnavailable(CloudAccountProvider)
    case accountNotFound
    case invalidLocalLabel
    case superseded
    case invalidConnectorResult

    public var errorDescription: String? {
        switch self {
        case .connectorUnavailable:
            return "Native account connection is not available for this provider."
        case .accountNotFound:
            return "The local cloud account no longer exists."
        case .invalidLocalLabel:
            return "The local cloud account label is invalid."
        case .superseded:
            return "A newer cloud account operation replaced this one."
        case .invalidConnectorResult:
            return "The cloud provider returned an invalid account discovery result."
        }
    }
}

// MARK: - Generation-safe orchestration

public actor CloudAccountCoordinator {
    public typealias RetrySleep = @Sendable (TimeInterval) async throws -> Void

    private struct ConnectOperation {
        let id: CloudAccountOperationID
        let generation: CloudAccountGeneration
        let task: Task<CloudAccountRecord, Error>
    }

    private struct RefreshOperation {
        let id: CloudAccountOperationID
        let generation: CloudAccountGeneration
        let credentialGeneration: CloudAccountGeneration
        let task: Task<CloudAccountRecord, Error>
    }

    private struct DiscoveryOperation {
        let id: CloudAccountOperationID
        let generation: CloudAccountGeneration
        let credentialGeneration: CloudAccountGeneration
        let task: Task<CloudAccountDiscoverySnapshot, Error>
    }

    private let connectors: [CloudAccountProvider: any CloudAccountConnector]
    private let limits: CloudAccountDiscoveryLimits
    private let now: @Sendable () -> Date
    private let retrySleep: RetrySleep
    private var accounts: [CloudAccountID: CloudAccountRecord] = [:]
    private var connectGenerations: [CloudAccountProvider: UInt64] = [:]
    private var accountOperationGenerations: [CloudAccountID: UInt64] = [:]
    private var connectOperations: [CloudAccountProvider: ConnectOperation] = [:]
    private var refreshOperations: [CloudAccountID: RefreshOperation] = [:]
    private var discoveryOperations: [CloudAccountID: DiscoveryOperation] = [:]

    public init(
        connectors: [any CloudAccountConnector],
        limits: CloudAccountDiscoveryLimits = CloudAccountDiscoveryLimits(),
        now: @escaping @Sendable () -> Date = Date.init,
        retrySleep: @escaping RetrySleep = CloudAccountCoordinator.defaultRetrySleep
    ) {
        self.connectors = connectors.reduce(into: [:]) { result, connector in
            result[connector.provider] = connector
        }
        self.limits = limits
        self.now = now
        self.retrySleep = retrySleep
    }

    public func connectedAccounts() -> [CloudAccountRecord] {
        accounts.values.sorted {
            if $0.provider.rawValue != $1.provider.rawValue {
                return $0.provider.rawValue < $1.provider.rawValue
            }
            return $0.id.rawValue.uuidString < $1.id.rawValue.uuidString
        }
    }

    public func account(id: CloudAccountID) -> CloudAccountRecord? {
        accounts[id]
    }

    public func connect(
        provider: CloudAccountProvider,
        localLabel: String = ""
    ) async throws -> CloudAccountRecord {
        guard let connector = connectors[provider] else {
            throw CloudAccountCoordinatorError.connectorUnavailable(provider)
        }
        guard let normalizedLabel = Self.normalizedLocalLabel(localLabel) else {
            throw CloudAccountCoordinatorError.invalidLocalLabel
        }
        connectOperations[provider]?.task.cancel()
        let generation = nextConnectGeneration(for: provider)
        let id = CloudAccountOperationID()
        let request = CloudAccountConnectRequest(
            operationID: id,
            operationGeneration: generation,
            localLabel: normalizedLabel
        )
        let task = Task { try await connector.connect(request) }
        connectOperations[provider] = ConnectOperation(id: id, generation: generation, task: task)

        do {
            let result = try await task.value
            guard let current = connectOperations[provider],
                  current.id == id,
                  current.generation == generation else {
                try? await discardSupersededConnection(result, connector: connector, operationGeneration: generation)
                throw CloudAccountCoordinatorError.superseded
            }
            connectOperations.removeValue(forKey: provider)
            guard result.provider == provider,
                  result.credentialGeneration.rawValue > 0,
                  let connectorLabel = Self.normalizedLocalLabel(result.localLabel),
                  accounts[result.id].map({ existing in
                      existing.provider == provider
                          && result.credentialGeneration > existing.credentialGeneration
                  }) ?? true else {
                try? await discardSupersededConnection(result, connector: connector, operationGeneration: generation)
                throw CloudAccountCoordinatorError.invalidConnectorResult
            }
            refreshOperations.removeValue(forKey: result.id)?.task.cancel()
            discoveryOperations.removeValue(forKey: result.id)?.task.cancel()
            let accepted = result.updatingLocalLabel(
                normalizedLabel.isEmpty ? connectorLabel : normalizedLabel
            )
            accounts[result.id] = accepted
            accountOperationGenerations[result.id] = 0
            return accepted
        } catch {
            if connectOperations[provider]?.id == id {
                connectOperations.removeValue(forKey: provider)
            }
            throw Self.normalizedCancellation(error)
        }
    }

    public func cancelConnection(provider: CloudAccountProvider) {
        _ = nextConnectGeneration(for: provider)
        connectOperations.removeValue(forKey: provider)?.task.cancel()
    }

    @discardableResult
    public func rename(accountID: CloudAccountID, localLabel: String) throws -> CloudAccountRecord {
        guard let account = accounts[accountID] else {
            throw CloudAccountCoordinatorError.accountNotFound
        }
        guard let label = Self.normalizedLocalLabel(localLabel), !label.isEmpty else {
            throw CloudAccountCoordinatorError.invalidLocalLabel
        }
        let updated = account.updatingLocalLabel(label)
        accounts[accountID] = updated
        return updated
    }

    public func refresh(accountID: CloudAccountID) async throws -> CloudAccountRecord {
        guard let account = accounts[accountID] else {
            throw CloudAccountCoordinatorError.accountNotFound
        }
        guard let connector = connectors[account.provider] else {
            throw CloudAccountCoordinatorError.connectorUnavailable(account.provider)
        }
        refreshOperations[accountID]?.task.cancel()
        discoveryOperations.removeValue(forKey: accountID)?.task.cancel()
        let generation = nextAccountOperationGeneration(for: accountID)
        let id = CloudAccountOperationID()
        let request = boundRequest(id: id, generation: generation, account: account)
        let task = Task { try await connector.refresh(request) }
        refreshOperations[accountID] = RefreshOperation(
            id: id,
            generation: generation,
            credentialGeneration: account.credentialGeneration,
            task: task
        )

        do {
            let result = try await task.value
            guard let current = refreshOperations[accountID],
                  current.id == id,
                  current.generation == generation,
                  current.credentialGeneration == account.credentialGeneration,
                  accountOperationGenerations[accountID] == generation.rawValue,
                  accounts[accountID]?.credentialGeneration == account.credentialGeneration else {
                throw CloudAccountCoordinatorError.superseded
            }
            refreshOperations.removeValue(forKey: accountID)
            guard result.id == accountID,
                  result.provider == account.provider,
                  result.credentialGeneration > account.credentialGeneration else {
                throw CloudAccountCoordinatorError.invalidConnectorResult
            }
            discoveryOperations.removeValue(forKey: accountID)?.task.cancel()
            let accepted = result.updatingLocalLabel(account.localLabel)
            accounts[accountID] = accepted
            return accepted
        } catch {
            if refreshOperations[accountID]?.id == id {
                refreshOperations.removeValue(forKey: accountID)
            }
            throw Self.normalizedCancellation(error, stage: .accountRefresh)
        }
    }

    public func synchronize(
        accountID: CloudAccountID,
        selectedScopeIDs: Set<CloudDiscoveryScopeID> = []
    ) async throws -> CloudAccountDiscoverySnapshot {
        guard let account = accounts[accountID] else {
            throw CloudAccountCoordinatorError.accountNotFound
        }
        guard let connector = connectors[account.provider] else {
            throw CloudAccountCoordinatorError.connectorUnavailable(account.provider)
        }
        discoveryOperations[accountID]?.task.cancel()
        refreshOperations.removeValue(forKey: accountID)?.task.cancel()
        let generation = nextAccountOperationGeneration(for: accountID)
        let id = CloudAccountOperationID()
        let limits = self.limits
        let retrySleep = self.retrySleep
        let task = Task {
            try await Self.loadDiscovery(
                connector: connector,
                account: account,
                operationID: id,
                operationGeneration: generation,
                selectedScopeIDs: selectedScopeIDs,
                limits: limits,
                retrySleep: retrySleep
            )
        }
        discoveryOperations[accountID] = DiscoveryOperation(
            id: id,
            generation: generation,
            credentialGeneration: account.credentialGeneration,
            task: task
        )

        do {
            let result = try await task.value
            guard let current = discoveryOperations[accountID],
                  current.id == id,
                  current.generation == generation,
                  current.credentialGeneration == account.credentialGeneration,
                  accountOperationGenerations[accountID] == generation.rawValue,
                  accounts[accountID]?.credentialGeneration == account.credentialGeneration else {
                throw CloudAccountCoordinatorError.superseded
            }
            discoveryOperations.removeValue(forKey: accountID)
            accounts[accountID] = account.updatingAfterSync(
                clusterCount: result.candidates.count,
                at: result.isPartial ? account.lastSuccessfulSync : now()
            )
            return result
        } catch {
            if discoveryOperations[accountID]?.id == id {
                discoveryOperations.removeValue(forKey: accountID)
            }
            throw Self.normalizedCancellation(error, stage: .clusterDiscovery)
        }
    }

    public func cancelSynchronization(accountID: CloudAccountID) {
        _ = nextAccountOperationGeneration(for: accountID)
        discoveryOperations.removeValue(forKey: accountID)?.task.cancel()
    }

    public func diagnostics(accountID: CloudAccountID) async throws -> [CloudAccountDiagnostic] {
        guard let account = accounts[accountID] else {
            throw CloudAccountCoordinatorError.accountNotFound
        }
        guard let connector = connectors[account.provider] else {
            throw CloudAccountCoordinatorError.connectorUnavailable(account.provider)
        }
        let generation = CloudAccountGeneration(
            rawValue: accountOperationGenerations[accountID, default: 0]
        )
        let request = boundRequest(
            id: CloudAccountOperationID(),
            generation: generation,
            account: account
        )
        return await connector.diagnostics(request)
    }

    public func disconnect(accountID: CloudAccountID) async throws {
        guard let account = accounts[accountID] else {
            throw CloudAccountCoordinatorError.accountNotFound
        }
        guard let connector = connectors[account.provider] else {
            throw CloudAccountCoordinatorError.connectorUnavailable(account.provider)
        }
        refreshOperations.removeValue(forKey: accountID)?.task.cancel()
        discoveryOperations.removeValue(forKey: accountID)?.task.cancel()
        let generation = nextAccountOperationGeneration(for: accountID)
        let request = boundRequest(
            id: CloudAccountOperationID(),
            generation: generation,
            account: account
        )
        do {
            try await connector.disconnect(request)
            guard accounts[accountID]?.credentialGeneration == account.credentialGeneration,
                  accountOperationGenerations[accountID] == generation.rawValue else {
                throw CloudAccountCoordinatorError.superseded
            }
            accounts.removeValue(forKey: accountID)
            accountOperationGenerations.removeValue(forKey: accountID)
        } catch {
            throw Self.normalizedCancellation(error, stage: .localDisconnect)
        }
    }

    private func nextConnectGeneration(for provider: CloudAccountProvider) -> CloudAccountGeneration {
        let next = connectGenerations[provider, default: 0] &+ 1
        connectGenerations[provider] = next
        return CloudAccountGeneration(rawValue: next)
    }

    private static func normalizedLocalLabel(_ value: String) -> String? {
        let label = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard label.utf8.count <= 256,
              label.unicodeScalars.allSatisfy({ !CharacterSet.controlCharacters.contains($0) }) else {
            return nil
        }
        return label
    }

    private func nextAccountOperationGeneration(for accountID: CloudAccountID) -> CloudAccountGeneration {
        let next = accountOperationGenerations[accountID, default: 0] &+ 1
        accountOperationGenerations[accountID] = next
        return CloudAccountGeneration(rawValue: next)
    }

    private func boundRequest(
        id: CloudAccountOperationID,
        generation: CloudAccountGeneration,
        account: CloudAccountRecord
    ) -> CloudAccountBoundRequest {
        CloudAccountBoundRequest(
            operationID: id,
            operationGeneration: generation,
            accountID: account.id,
            credentialGeneration: account.credentialGeneration
        )
    }

    private func discardSupersededConnection(
        _ account: CloudAccountRecord,
        connector: any CloudAccountConnector,
        operationGeneration: CloudAccountGeneration
    ) async throws {
        guard account.provider == connector.provider else { return }
        try await connector.disconnect(CloudAccountBoundRequest(
            operationID: CloudAccountOperationID(),
            operationGeneration: operationGeneration,
            accountID: account.id,
            credentialGeneration: account.credentialGeneration
        ))
    }

    private static func normalizedCancellation(
        _ error: any Error,
        stage: CloudAccountOperationStage = .authorization
    ) -> any Error {
        if error is CancellationError {
            return CloudAccountFailure(
                stage: stage,
                classification: .canceled,
                isRetryable: true,
                recoveryAction: .retry
            )
        }
        return error
    }

    public static func defaultRetrySleep(_ seconds: TimeInterval) async throws {
        guard seconds > 0 else {
            try Task.checkCancellation()
            return
        }
        let nanoseconds = UInt64(min(seconds, 30) * 1_000_000_000)
        try await Task<Never, Never>.sleep(nanoseconds: nanoseconds)
    }
}

// MARK: - Bounded discovery

private extension CloudAccountCoordinator {
    static func loadDiscovery(
        connector: any CloudAccountConnector,
        account: CloudAccountRecord,
        operationID: CloudAccountOperationID,
        operationGeneration: CloudAccountGeneration,
        selectedScopeIDs: Set<CloudDiscoveryScopeID>,
        limits: CloudAccountDiscoveryLimits,
        retrySleep: @escaping RetrySleep
    ) async throws -> CloudAccountDiscoverySnapshot {
        var scopes: [CloudDiscoveryScope] = []
        var scopeIndexes: [CloudDiscoveryScopeID: Int] = [:]
        var candidates: [CloudClusterCandidate] = []
        var candidateIndexes: [CloudClusterCandidateID: Int] = [:]
        var issues: [CloudAccountFailure] = []
        var isPartial = false

        var pageToken: String?
        var seenPageTokens = Set<String>()
        for pageIndex in 0..<limits.maximumPages {
            try Task.checkCancellation()
            let request = pageRequest(
                operationID: operationID,
                operationGeneration: operationGeneration,
                account: account,
                selectedScopeIDs: [],
                pageToken: pageToken,
                limits: limits
            )
            do {
                let page = try await retryingPage(
                    stage: .scopeDiscovery,
                    maximumRetries: limits.maximumRetriesPerPage,
                    initialDelay: limits.initialRetryDelay,
                    retrySleep: retrySleep
                ) {
                    try await connector.discoveryScopes(request)
                }
                guard page.scopes.allSatisfy({ valid($0, for: account) }) else {
                    throw CloudAccountCoordinatorError.invalidConnectorResult
                }
                let didTruncateScopes = appendUnique(
                    page.scopes,
                    to: &scopes,
                    indexes: &scopeIndexes,
                    maximumCount: limits.maximumScopes,
                    id: \.id
                )
                issues.append(contentsOf: page.issues)
                if didTruncateScopes || (scopes.count == limits.maximumScopes && page.nextPageToken != nil) {
                    isPartial = true
                    issues.append(limitFailure(stage: .scopeDiscovery))
                    break
                }
                guard let rawNext = page.nextPageToken else { break }
                guard let next = normalizedPageToken(rawNext) else {
                    throw CloudAccountCoordinatorError.invalidConnectorResult
                }
                guard seenPageTokens.insert(next).inserted else {
                    throw CloudAccountCoordinatorError.invalidConnectorResult
                }
                pageToken = next
                if pageIndex == limits.maximumPages - 1 {
                    isPartial = true
                    issues.append(limitFailure(stage: .scopeDiscovery))
                }
            } catch let error as CloudAccountFailure {
                issues.append(error)
                isPartial = true
                break
            }
        }

        pageToken = nil
        seenPageTokens.removeAll(keepingCapacity: true)
        for pageIndex in 0..<limits.maximumPages {
            try Task.checkCancellation()
            let request = pageRequest(
                operationID: operationID,
                operationGeneration: operationGeneration,
                account: account,
                selectedScopeIDs: selectedScopeIDs,
                pageToken: pageToken,
                limits: limits
            )
            do {
                let page = try await retryingPage(
                    stage: .clusterDiscovery,
                    maximumRetries: limits.maximumRetriesPerPage,
                    initialDelay: limits.initialRetryDelay,
                    retrySleep: retrySleep
                ) {
                    try await connector.discoverClusters(request)
                }
                guard page.candidates.allSatisfy({ valid($0, for: account) }) else {
                    throw CloudAccountCoordinatorError.invalidConnectorResult
                }
                let didTruncateCandidates = appendUnique(
                    page.candidates,
                    to: &candidates,
                    indexes: &candidateIndexes,
                    maximumCount: limits.maximumClusters,
                    id: \.id
                )
                issues.append(contentsOf: page.issues)
                if didTruncateCandidates
                    || (candidates.count == limits.maximumClusters && page.nextPageToken != nil) {
                    isPartial = true
                    issues.append(limitFailure(stage: .clusterDiscovery))
                    break
                }
                guard let rawNext = page.nextPageToken else { break }
                guard let next = normalizedPageToken(rawNext) else {
                    throw CloudAccountCoordinatorError.invalidConnectorResult
                }
                guard seenPageTokens.insert(next).inserted else {
                    throw CloudAccountCoordinatorError.invalidConnectorResult
                }
                pageToken = next
                if pageIndex == limits.maximumPages - 1 {
                    isPartial = true
                    issues.append(limitFailure(stage: .clusterDiscovery))
                }
            } catch let error as CloudAccountFailure {
                issues.append(error)
                isPartial = true
                break
            }
        }

        return CloudAccountDiscoverySnapshot(
            accountID: account.id,
            credentialGeneration: account.credentialGeneration,
            operationGeneration: operationGeneration,
            scopes: scopes,
            candidates: candidates,
            issues: issues,
            isPartial: isPartial || !issues.isEmpty
        )
    }

    static func retryingPage<Value: Sendable>(
        stage: CloudAccountOperationStage,
        maximumRetries: Int,
        initialDelay: TimeInterval,
        retrySleep: @escaping RetrySleep,
        operation: @escaping @Sendable () async throws -> Value
    ) async throws -> Value {
        var retry = 0
        while true {
            try Task.checkCancellation()
            do {
                return try await operation()
            } catch is CancellationError {
                throw CancellationError()
            } catch let failure as CloudAccountFailure {
                guard failure.isRetryable, retry < maximumRetries else { throw failure }
                let exponentialDelay = initialDelay * pow(2, Double(retry))
                try await retrySleep(failure.retryAfter ?? min(exponentialDelay, 30))
                retry += 1
            } catch {
                throw CloudAccountFailure(
                    stage: stage,
                    classification: .invalidProviderResponse,
                    isRetryable: false,
                    recoveryAction: .runAuthDoctor
                )
            }
        }
    }

    static func pageRequest(
        operationID: CloudAccountOperationID,
        operationGeneration: CloudAccountGeneration,
        account: CloudAccountRecord,
        selectedScopeIDs: Set<CloudDiscoveryScopeID>,
        pageToken: String?,
        limits: CloudAccountDiscoveryLimits
    ) -> CloudAccountPageRequest {
        CloudAccountPageRequest(
            operationID: operationID,
            operationGeneration: operationGeneration,
            accountID: account.id,
            credentialGeneration: account.credentialGeneration,
            selectedScopeIDs: selectedScopeIDs,
            pageToken: pageToken,
            maximumResponseBytes: limits.maximumResponseBytes,
            maximumConcurrentScopeRequests: limits.maximumConcurrentScopeRequests
        )
    }

    static func valid(_ scope: CloudDiscoveryScope, for account: CloudAccountRecord) -> Bool {
        scope.provider == account.provider
            && scope.accountID == account.id
            && scope.credentialGeneration == account.credentialGeneration
    }

    static func valid(_ candidate: CloudClusterCandidate, for account: CloudAccountRecord) -> Bool {
        candidate.provider == account.provider
            && candidate.accountID == account.id
            && candidate.credentialGeneration == account.credentialGeneration
    }

    static func normalizedPageToken(_ token: String?) -> String? {
        guard let token else { return nil }
        let trimmed = token.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, trimmed.utf8.count <= 4_096 else { return nil }
        return trimmed
    }

    static func appendUnique<Element, ID: Hashable>(
        _ incoming: [Element],
        to values: inout [Element],
        indexes: inout [ID: Int],
        maximumCount: Int,
        id: KeyPath<Element, ID>
    ) -> Bool {
        var didTruncate = false
        for value in incoming {
            let valueID = value[keyPath: id]
            if let index = indexes[valueID] {
                values[index] = value
            } else if values.count < maximumCount {
                indexes[valueID] = values.count
                values.append(value)
            } else {
                didTruncate = true
            }
        }
        return didTruncate
    }

    static func limitFailure(stage: CloudAccountOperationStage) -> CloudAccountFailure {
        CloudAccountFailure(
            stage: stage,
            classification: .invalidProviderResponse,
            isRetryable: false,
            recoveryAction: .changeScope
        )
    }
}

// MARK: - Filter-stable selection

public struct CloudClusterSelection: Sendable, Equatable {
    public private(set) var selectedIDs: Set<CloudClusterCandidateID>

    public init(selectedIDs: Set<CloudClusterCandidateID> = []) {
        self.selectedIDs = selectedIDs
    }

    public mutating func selectVisible(_ candidates: some Sequence<CloudClusterCandidate>) {
        selectedIDs.formUnion(candidates.lazy.map(\.id))
    }

    public mutating func deselectVisible(_ candidates: some Sequence<CloudClusterCandidate>) {
        selectedIDs.subtract(candidates.lazy.map(\.id))
    }

    /// Paginated and filtered updates retain unseen choices. Pruning happens only after a caller
    /// explicitly identifies a complete replacement result.
    public mutating func reconcile(
        with candidates: some Sequence<CloudClusterCandidate>,
        isCompleteReplacement: Bool
    ) {
        guard isCompleteReplacement else { return }
        selectedIDs.formIntersection(Set(candidates.lazy.map(\.id)))
    }
}
