import Foundation
import XCTest
@testable import RuneSecurity

final class CloudAccountCoordinatorTests: XCTestCase {
    private let now = Date(timeIntervalSince1970: 1_800_000_000)

    func testLatestConnectionWinsAndSupersededResultIsDiscarded() async throws {
        let firstGate = SyntheticCloudGate()
        let first = account(id: uuid(1), generation: 1, label: "First")
        let second = account(id: uuid(2), generation: 1, label: "Second")
        let connector = SyntheticCloudAccountConnector(
            connectSteps: [
                .gated(firstGate, first),
                .record(second)
            ]
        )
        let coordinator = CloudAccountCoordinator(connectors: [connector])

        let firstTask = Task {
            try await coordinator.connect(provider: .azure, localLabel: "First")
        }
        try await firstGate.waitUntilBlocked()
        let accepted = try await coordinator.connect(provider: .azure, localLabel: "Second")
        await firstGate.open()

        XCTAssertEqual(accepted, second)
        do {
            _ = try await firstTask.value
            XCTFail("Expected the older connection attempt to be rejected")
        } catch {
            XCTAssertEqual(error as? CloudAccountCoordinatorError, .superseded)
        }
        let stored = await coordinator.connectedAccounts()
        XCTAssertEqual(stored, [second])
        let discarded = await connector.disconnectRequests()
        XCTAssertEqual(discarded.count, 1)
        XCTAssertEqual(discarded.first?.accountID, first.id)
        XCTAssertEqual(discarded.first?.credentialGeneration, first.credentialGeneration)
    }

    func testCanceledConnectionCannotPublishLateResult() async throws {
        let gate = SyntheticCloudGate()
        let late = account(id: uuid(3), generation: 1, label: "Late")
        let connector = SyntheticCloudAccountConnector(
            connectSteps: [.gated(gate, late)]
        )
        let coordinator = CloudAccountCoordinator(connectors: [connector])

        let task = Task {
            try await coordinator.connect(provider: .azure)
        }
        try await gate.waitUntilBlocked()
        await coordinator.cancelConnection(provider: .azure)
        await gate.open()

        do {
            _ = try await task.value
            XCTFail("Expected cancellation to reject the delayed result")
        } catch {
            XCTAssertEqual(error as? CloudAccountCoordinatorError, .superseded)
        }
        let stored = await coordinator.connectedAccounts()
        XCTAssertTrue(stored.isEmpty)
    }

    func testDiscoveryPaginatesRetriesAndReconcilesByOpaqueIdentity() async throws {
        let account = account(id: uuid(10), generation: 1, label: "Synthetic")
        let subscription = scope(id: uuid(11), account: account, kind: .subscription, name: "Scope A")
        let region = scope(
            id: uuid(12),
            account: account,
            parentID: subscription.id,
            kind: .region,
            name: "Region B"
        )
        let candidateID = CloudClusterCandidateID(rawValue: uuid(13))
        let firstCandidate = candidate(
            id: candidateID,
            account: account,
            scopes: [subscription.id, region.id],
            name: "Cluster A",
            reachability: .unknown
        )
        let updatedCandidate = candidate(
            id: candidateID,
            account: account,
            scopes: [subscription.id, region.id],
            name: "Cluster A",
            reachability: .reachable
        )
        let secondCandidate = candidate(
            id: CloudClusterCandidateID(rawValue: uuid(14)),
            account: account,
            scopes: [region.id],
            name: "Cluster B",
            reachability: .privateEndpoint
        )
        let throttled = CloudAccountFailure(
            stage: .clusterDiscovery,
            classification: .throttled,
            isRetryable: true,
            recoveryAction: .retry,
            retryAfter: 0.75
        )
        let connector = SyntheticCloudAccountConnector(
            connectSteps: [.record(account)],
            scopeSteps: [
                .page(CloudDiscoveryScopePage(scopes: [subscription], nextPageToken: "scope-2")),
                .page(CloudDiscoveryScopePage(scopes: [region]))
            ],
            clusterSteps: [
                .failure(throttled),
                .page(CloudClusterDiscoveryPage(candidates: [firstCandidate], nextPageToken: "cluster-2")),
                .page(CloudClusterDiscoveryPage(candidates: [updatedCandidate, secondCandidate]))
            ]
        )
        let sleepRecorder = SyntheticRetrySleepRecorder()
        let fixedNow = now
        let limits = CloudAccountDiscoveryLimits(
            maximumPages: 4,
            maximumScopes: 8,
            maximumClusters: 8,
            maximumResponseBytes: 2_048,
            maximumConcurrentScopeRequests: 2,
            maximumRetriesPerPage: 2,
            initialRetryDelay: 0.1
        )
        let coordinator = CloudAccountCoordinator(
            connectors: [connector],
            limits: limits,
            now: { fixedNow },
            retrySleep: { seconds in await sleepRecorder.record(seconds) }
        )
        _ = try await coordinator.connect(provider: .azure)

        let result = try await coordinator.synchronize(
            accountID: account.id,
            selectedScopeIDs: [region.id]
        )

        XCTAssertEqual(result.scopes, [subscription, region])
        XCTAssertEqual(result.candidates, [updatedCandidate, secondCandidate])
        XCTAssertFalse(result.isPartial)
        XCTAssertTrue(result.issues.isEmpty)
        let delays = await sleepRecorder.delays()
        XCTAssertEqual(delays, [0.75])

        let scopeRequests = await connector.scopeRequests()
        XCTAssertEqual(scopeRequests.map(\.pageToken), [nil, "scope-2"])
        XCTAssertTrue(scopeRequests.allSatisfy { $0.maximumResponseBytes == 2_048 })
        XCTAssertTrue(scopeRequests.allSatisfy { $0.maximumConcurrentScopeRequests == 2 })
        let clusterRequests = await connector.clusterRequests()
        XCTAssertEqual(clusterRequests.map(\.pageToken), [nil, nil, "cluster-2"])
        XCTAssertTrue(clusterRequests.allSatisfy { $0.selectedScopeIDs == [region.id] })
        XCTAssertEqual(Set(clusterRequests.map(\.operationGeneration)).count, 1)
        XCTAssertTrue(clusterRequests.allSatisfy {
            $0.credentialGeneration == account.credentialGeneration
        })

        let synchronizedAccount = await coordinator.account(id: account.id)
        let updatedAccount = try XCTUnwrap(synchronizedAccount)
        XCTAssertEqual(updatedAccount.lastSuccessfulSync, now)
        XCTAssertEqual(updatedAccount.discoverableClusterCount, 2)
    }

    func testDiscoveryReturnsBoundedPartialResultsForLimitAndPermissionFailures() async throws {
        let account = account(id: uuid(20), generation: 1, label: "Synthetic")
        let first = candidate(
            id: CloudClusterCandidateID(rawValue: uuid(21)),
            account: account,
            name: "Cluster A"
        )
        let second = candidate(
            id: CloudClusterCandidateID(rawValue: uuid(22)),
            account: account,
            name: "Cluster B"
        )
        let third = candidate(
            id: CloudClusterCandidateID(rawValue: uuid(23)),
            account: account,
            name: "Cluster C"
        )
        let connector = SyntheticCloudAccountConnector(
            connectSteps: [.record(account)],
            scopeSteps: [.page(CloudDiscoveryScopePage(scopes: []))],
            clusterSteps: [
                .page(CloudClusterDiscoveryPage(candidates: [first, second, third]))
            ]
        )
        let coordinator = CloudAccountCoordinator(
            connectors: [connector],
            limits: CloudAccountDiscoveryLimits(maximumClusters: 2),
            retrySleep: { _ in }
        )
        _ = try await coordinator.connect(provider: .azure)

        let result = try await coordinator.synchronize(accountID: account.id)

        XCTAssertEqual(result.candidates, [first, second])
        XCTAssertTrue(result.isPartial)
        XCTAssertEqual(result.issues.count, 1)
        XCTAssertEqual(result.issues.first?.stage, .clusterDiscovery)
        XCTAssertEqual(result.issues.first?.recoveryAction, .changeScope)
        let accountAfterPartialSync = await coordinator.account(id: account.id)
        XCTAssertNil(accountAfterPartialSync?.lastSuccessfulSync)

        let permissionFailure = CloudAccountFailure(
            stage: .clusterDiscovery,
            classification: .permissionDenied,
            isRetryable: false,
            recoveryAction: .openPermissionHelp
        )
        let partialConnector = SyntheticCloudAccountConnector(
            connectSteps: [.record(account)],
            scopeSteps: [.page(CloudDiscoveryScopePage(scopes: []))],
            clusterSteps: [
                .page(CloudClusterDiscoveryPage(candidates: [first], nextPageToken: "more")),
                .failure(permissionFailure)
            ]
        )
        let partialCoordinator = CloudAccountCoordinator(
            connectors: [partialConnector],
            retrySleep: { _ in }
        )
        _ = try await partialCoordinator.connect(provider: .azure)

        let partial = try await partialCoordinator.synchronize(accountID: account.id)

        XCTAssertEqual(partial.candidates, [first])
        XCTAssertTrue(partial.isPartial)
        XCTAssertEqual(partial.issues, [permissionFailure])
    }

    func testCredentialRefreshSupersedesDiscoveryFromOlderGeneration() async throws {
        let discoveryGate = SyntheticCloudGate()
        let initial = account(id: uuid(30), generation: 1, label: "Synthetic")
        let refreshed = account(id: uuid(30), generation: 2, label: "Synthetic")
        let oldCandidate = candidate(
            id: CloudClusterCandidateID(rawValue: uuid(31)),
            account: initial,
            name: "Old cluster"
        )
        let connector = SyntheticCloudAccountConnector(
            connectSteps: [.record(initial)],
            refreshSteps: [.record(refreshed)],
            scopeSteps: [.page(CloudDiscoveryScopePage(scopes: []))],
            clusterSteps: [
                .gated(discoveryGate, CloudClusterDiscoveryPage(candidates: [oldCandidate]))
            ]
        )
        let coordinator = CloudAccountCoordinator(connectors: [connector])
        _ = try await coordinator.connect(provider: .azure)

        let discoveryTask = Task {
            try await coordinator.synchronize(accountID: initial.id)
        }
        try await discoveryGate.waitUntilBlocked()
        let acceptedRefresh = try await coordinator.refresh(accountID: initial.id)
        await discoveryGate.open()

        XCTAssertEqual(acceptedRefresh.credentialGeneration, CloudAccountGeneration(rawValue: 2))
        do {
            _ = try await discoveryTask.value
            XCTFail("Expected discovery bound to the older credential to be discarded")
        } catch {
            XCTAssertEqual(error as? CloudAccountCoordinatorError, .superseded)
        }
        let refreshedAccount = await coordinator.account(id: initial.id)
        let stored = try XCTUnwrap(refreshedAccount)
        XCTAssertEqual(stored.credentialGeneration, CloudAccountGeneration(rawValue: 2))
        XCTAssertEqual(stored.discoverableClusterCount, 0)
    }

    func testRejectsMismatchedCredentialGenerationAndCyclicPageToken() async throws {
        let account = account(id: uuid(40), generation: 1, label: "Synthetic")
        let mismatched = candidate(
            id: CloudClusterCandidateID(rawValue: uuid(41)),
            account: self.account(id: uuid(40), generation: 2, label: "Synthetic"),
            name: "Wrong generation"
        )
        let mismatchedConnector = SyntheticCloudAccountConnector(
            connectSteps: [.record(account)],
            scopeSteps: [.page(CloudDiscoveryScopePage(scopes: []))],
            clusterSteps: [.page(CloudClusterDiscoveryPage(candidates: [mismatched]))]
        )
        let mismatchedCoordinator = CloudAccountCoordinator(connectors: [mismatchedConnector])
        _ = try await mismatchedCoordinator.connect(provider: .azure)

        do {
            _ = try await mismatchedCoordinator.synchronize(accountID: account.id)
            XCTFail("Expected generation mismatch to fail closed")
        } catch {
            XCTAssertEqual(error as? CloudAccountCoordinatorError, .invalidConnectorResult)
        }

        let cyclicConnector = SyntheticCloudAccountConnector(
            connectSteps: [.record(account)],
            scopeSteps: [
                .page(CloudDiscoveryScopePage(scopes: [], nextPageToken: "same")),
                .page(CloudDiscoveryScopePage(scopes: [], nextPageToken: "same"))
            ]
        )
        let cyclicCoordinator = CloudAccountCoordinator(connectors: [cyclicConnector])
        _ = try await cyclicCoordinator.connect(provider: .azure)

        do {
            _ = try await cyclicCoordinator.synchronize(accountID: account.id)
            XCTFail("Expected cyclic pagination to fail closed")
        } catch {
            XCTAssertEqual(error as? CloudAccountCoordinatorError, .invalidConnectorResult)
        }
    }

    func testDisconnectRemovesOnlyExactLocalAccountAfterSecretCleanup() async throws {
        let account = account(id: uuid(50), generation: 4, label: "Synthetic")
        let connector = SyntheticCloudAccountConnector(connectSteps: [.record(account)])
        let coordinator = CloudAccountCoordinator(connectors: [connector])
        _ = try await coordinator.connect(provider: .azure)

        let renamed = try await coordinator.rename(accountID: account.id, localLabel: "  Local label  ")
        XCTAssertEqual(renamed.localLabel, "Local label")

        try await coordinator.disconnect(accountID: account.id)

        let stored = await coordinator.account(id: account.id)
        XCTAssertNil(stored)
        let requests = await connector.disconnectRequests()
        XCTAssertEqual(requests.count, 1)
        XCTAssertEqual(requests.first?.accountID, account.id)
        XCTAssertEqual(requests.first?.credentialGeneration, account.credentialGeneration)
    }

    func testInvalidLocalLabelFailsBeforeConnectorRuns() async throws {
        let account = account(id: uuid(51), generation: 1, label: "Synthetic")
        let connector = SyntheticCloudAccountConnector(connectSteps: [.record(account)])
        let coordinator = CloudAccountCoordinator(connectors: [connector])

        do {
            _ = try await coordinator.connect(
                provider: .azure,
                localLabel: String(repeating: "x", count: 257)
            )
            XCTFail("Expected bounded local label validation")
        } catch {
            XCTAssertEqual(error as? CloudAccountCoordinatorError, .invalidLocalLabel)
        }
        let requests = await connector.connectRequests()
        XCTAssertTrue(requests.isEmpty)
    }

    func testSelectionSurvivesPaginationAndFilteringUntilCompleteReplacement() {
        let account = account(id: uuid(60), generation: 1, label: "Synthetic")
        let first = candidate(
            id: CloudClusterCandidateID(rawValue: uuid(61)),
            account: account,
            name: "Cluster A"
        )
        let second = candidate(
            id: CloudClusterCandidateID(rawValue: uuid(62)),
            account: account,
            name: "Cluster B"
        )
        var selection = CloudClusterSelection()

        selection.selectVisible([first, second])
        selection.reconcile(with: [second], isCompleteReplacement: false)
        XCTAssertEqual(selection.selectedIDs, [first.id, second.id])

        selection.deselectVisible([second])
        XCTAssertEqual(selection.selectedIDs, [first.id])

        selection.reconcile(with: [second], isCompleteReplacement: true)
        XCTAssertTrue(selection.selectedIDs.isEmpty)
    }

    func testDiagnosticsProjectionContainsNoAccountOrProviderResourceIdentity() async throws {
        let account = account(id: uuid(70), generation: 1, label: "Synthetic")
        let diagnostic = CloudAccountDiagnostic(
            provider: .azure,
            stage: .credentialExchange,
            classification: .requiresReauthentication,
            isRetryable: true,
            recoveryAction: .reauthorize
        )
        let connector = SyntheticCloudAccountConnector(
            connectSteps: [.record(account)],
            diagnostics: [diagnostic]
        )
        let coordinator = CloudAccountCoordinator(connectors: [connector])
        _ = try await coordinator.connect(provider: .azure)

        let result = try await coordinator.diagnostics(accountID: account.id)
        let encoded = try JSONEncoder().encode(result)
        let text = String(decoding: encoded, as: UTF8.self)

        XCTAssertEqual(result, [diagnostic])
        XCTAssertFalse(text.contains(account.id.rawValue.uuidString))
        XCTAssertFalse(text.contains(account.localLabel))
        XCTAssertFalse(text.contains("endpoint"))
    }

    func testRenameSurvivesInFlightCredentialRefresh() async throws {
        let gate = SyntheticCloudGate()
        let initial = account(id: uuid(80), generation: 1, label: "Original")
        let refreshed = account(id: uuid(80), generation: 2, label: "Provider label")
        let connector = SyntheticCloudAccountConnector(
            connectSteps: [.record(initial)], refreshSteps: [.gated(gate, refreshed)]
        )
        let coordinator = CloudAccountCoordinator(connectors: [connector])
        _ = try await coordinator.connect(provider: .azure)
        let task = Task { try await coordinator.refresh(accountID: initial.id) }
        try await gate.waitUntilBlocked()

        _ = try await coordinator.rename(accountID: initial.id, localLabel: "Renamed locally")
        await gate.open()
        let result = try await task.value
        let stored = await coordinator.account(id: initial.id)

        XCTAssertEqual(result.localLabel, "Renamed locally")
        XCTAssertEqual(result.credentialGeneration, refreshed.credentialGeneration)
        XCTAssertEqual(stored, result)
    }

    func testRenameSurvivesInFlightDiscovery() async throws {
        let gate = SyntheticCloudGate()
        let initial = account(id: uuid(81), generation: 1, label: "Original")
        let connector = SyntheticCloudAccountConnector(
            connectSteps: [.record(initial)],
            clusterSteps: [.gated(gate, CloudClusterDiscoveryPage(candidates: []))]
        )
        let fixedNow = now
        let coordinator = CloudAccountCoordinator(connectors: [connector], now: { fixedNow })
        _ = try await coordinator.connect(provider: .azure)
        let task = Task { try await coordinator.synchronize(accountID: initial.id) }
        try await gate.waitUntilBlocked()

        _ = try await coordinator.rename(accountID: initial.id, localLabel: "Renamed locally")
        await gate.open()
        _ = try await task.value
        let stored = await coordinator.account(id: initial.id)

        XCTAssertEqual(stored?.localLabel, "Renamed locally")
        XCTAssertEqual(stored?.lastSuccessfulSync, now)
    }

    func testCancelWithoutDiscoveryDoesNotSupersedeCredentialRefresh() async throws {
        let gate = SyntheticCloudGate()
        let initial = account(id: uuid(82), generation: 1, label: "Synthetic")
        let refreshed = account(id: uuid(82), generation: 2, label: "Synthetic")
        let connector = SyntheticCloudAccountConnector(
            connectSteps: [.record(initial)], refreshSteps: [.gated(gate, refreshed)]
        )
        let coordinator = CloudAccountCoordinator(connectors: [connector])
        _ = try await coordinator.connect(provider: .azure)
        let task = Task { try await coordinator.refresh(accountID: initial.id) }
        try await gate.waitUntilBlocked()

        await coordinator.cancelSynchronization(accountID: initial.id)
        await gate.open()
        let result = try await task.value

        XCTAssertEqual(result, refreshed)
    }

    func testDiagnosticsCannotReturnAfterCredentialRefreshOrDisconnect() async throws {
        for shouldDisconnect in [false, true] {
            let gate = SyntheticCloudGate()
            let initial = account(id: uuid(83), generation: 1, label: "Synthetic")
            let refreshed = account(id: uuid(83), generation: 2, label: "Synthetic")
            let connector = SyntheticCloudAccountConnector(
                connectSteps: [.record(initial)],
                refreshSteps: [.record(refreshed)],
                diagnosticGates: [gate]
            )
            let coordinator = CloudAccountCoordinator(connectors: [connector])
            _ = try await coordinator.connect(provider: .azure)
            let task = Task { try await coordinator.diagnostics(accountID: initial.id) }
            try await gate.waitUntilBlocked()

            if shouldDisconnect {
                try await coordinator.disconnect(accountID: initial.id)
            } else {
                _ = try await coordinator.refresh(accountID: initial.id)
            }
            await gate.open()

            do {
                _ = try await task.value
                XCTFail("Expected stale account diagnostics to be rejected")
            } catch {
                XCTAssertEqual(error as? CloudAccountCoordinatorError, .superseded)
            }
        }
    }

    func testNewDiagnosticsSupersedeOlderSameAccountRequest() async throws {
        let gate = SyntheticCloudGate()
        let initial = account(id: uuid(84), generation: 1, label: "Synthetic")
        let connector = SyntheticCloudAccountConnector(
            connectSteps: [.record(initial)], diagnosticGates: [gate]
        )
        let coordinator = CloudAccountCoordinator(connectors: [connector])
        _ = try await coordinator.connect(provider: .azure)
        let task = Task { try await coordinator.diagnostics(accountID: initial.id) }
        try await gate.waitUntilBlocked()

        let newest = try await coordinator.diagnostics(accountID: initial.id)
        await gate.open()
        XCTAssertTrue(newest.isEmpty)
        do {
            _ = try await task.value
            XCTFail("Expected the earlier diagnostic request to be rejected")
        } catch {
            XCTAssertEqual(error as? CloudAccountCoordinatorError, .superseded)
        }
    }

    private func account(
        id: UUID,
        generation: UInt64,
        label: String
    ) -> CloudAccountRecord {
        CloudAccountRecord(
            id: CloudAccountID(rawValue: id),
            provider: .azure,
            credentialGeneration: CloudAccountGeneration(rawValue: generation),
            localLabel: label
        )
    }

    private func scope(
        id: UUID,
        account: CloudAccountRecord,
        parentID: CloudDiscoveryScopeID? = nil,
        kind: CloudDiscoveryScopeKind,
        name: String
    ) -> CloudDiscoveryScope {
        CloudDiscoveryScope(
            id: CloudDiscoveryScopeID(rawValue: id),
            provider: account.provider,
            accountID: account.id,
            credentialGeneration: account.credentialGeneration,
            parentID: parentID,
            kind: kind,
            displayName: name
        )
    }

    private func candidate(
        id: CloudClusterCandidateID,
        account: CloudAccountRecord,
        scopes: [CloudDiscoveryScopeID] = [],
        name: String,
        reachability: CloudClusterReachability = .unknown
    ) -> CloudClusterCandidate {
        CloudClusterCandidate(
            id: id,
            provider: account.provider,
            accountID: account.id,
            credentialGeneration: account.credentialGeneration,
            scopeIDs: scopes,
            displayName: name,
            endpointHost: "cluster.example.invalid",
            certificateStatus: .trustedDataAvailable,
            reachability: reachability
        )
    }

    private func uuid(_ suffix: UInt8) -> UUID {
        var bytes = [UInt8](repeating: 0, count: 16)
        bytes[15] = suffix
        return UUID(uuid: (
            bytes[0], bytes[1], bytes[2], bytes[3],
            bytes[4], bytes[5], bytes[6], bytes[7],
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15]
        ))
    }
}

private enum SyntheticConnectStep: Sendable {
    case record(CloudAccountRecord)
    case gated(SyntheticCloudGate, CloudAccountRecord)
    case failure(CloudAccountFailure)
}

private enum SyntheticRefreshStep: Sendable {
    case record(CloudAccountRecord)
    case gated(SyntheticCloudGate, CloudAccountRecord)
    case failure(CloudAccountFailure)
}

private enum SyntheticScopeStep: Sendable {
    case page(CloudDiscoveryScopePage)
    case failure(CloudAccountFailure)
}

private enum SyntheticClusterStep: Sendable {
    case page(CloudClusterDiscoveryPage)
    case gated(SyntheticCloudGate, CloudClusterDiscoveryPage)
    case failure(CloudAccountFailure)
}

private actor SyntheticCloudAccountConnector: CloudAccountConnector {
    nonisolated let provider: CloudAccountProvider = .azure

    private var connectScript: [SyntheticConnectStep]
    private var refreshScript: [SyntheticRefreshStep]
    private var scopeScript: [SyntheticScopeStep]
    private var clusterScript: [SyntheticClusterStep]
    private let diagnosticScript: [CloudAccountDiagnostic]
    private var diagnosticGates: [SyntheticCloudGate]
    private var capturedConnectRequests: [CloudAccountConnectRequest] = []
    private var capturedScopeRequests: [CloudAccountPageRequest] = []
    private var capturedClusterRequests: [CloudAccountPageRequest] = []
    private var capturedDisconnectRequests: [CloudAccountBoundRequest] = []

    init(
        connectSteps: [SyntheticConnectStep],
        refreshSteps: [SyntheticRefreshStep] = [],
        scopeSteps: [SyntheticScopeStep] = [.page(CloudDiscoveryScopePage(scopes: []))],
        clusterSteps: [SyntheticClusterStep] = [.page(CloudClusterDiscoveryPage(candidates: []))],
        diagnostics: [CloudAccountDiagnostic] = [],
        diagnosticGates: [SyntheticCloudGate] = []
    ) {
        self.connectScript = connectSteps
        self.refreshScript = refreshSteps
        self.scopeScript = scopeSteps
        self.clusterScript = clusterSteps
        self.diagnosticScript = diagnostics
        self.diagnosticGates = diagnosticGates
    }

    func connect(_ request: CloudAccountConnectRequest) async throws -> CloudAccountRecord {
        capturedConnectRequests.append(request)
        guard !connectScript.isEmpty else { throw invalidFailure(stage: .authorization) }
        let step = connectScript.removeFirst()
        switch step {
        case .record(let account):
            return account
        case .gated(let gate, let account):
            await gate.block()
            return account
        case .failure(let failure):
            throw failure
        }
    }

    func refresh(_ request: CloudAccountBoundRequest) async throws -> CloudAccountRecord {
        guard !refreshScript.isEmpty else { throw invalidFailure(stage: .accountRefresh) }
        let step = refreshScript.removeFirst()
        switch step {
        case .record(let account):
            return account
        case .gated(let gate, let account):
            await gate.block()
            return account
        case .failure(let failure):
            throw failure
        }
    }

    func discoveryScopes(_ request: CloudAccountPageRequest) async throws -> CloudDiscoveryScopePage {
        capturedScopeRequests.append(request)
        guard !scopeScript.isEmpty else { throw invalidFailure(stage: .scopeDiscovery) }
        let step = scopeScript.removeFirst()
        switch step {
        case .page(let page):
            return page
        case .failure(let failure):
            throw failure
        }
    }

    func discoverClusters(_ request: CloudAccountPageRequest) async throws -> CloudClusterDiscoveryPage {
        capturedClusterRequests.append(request)
        guard !clusterScript.isEmpty else { throw invalidFailure(stage: .clusterDiscovery) }
        let step = clusterScript.removeFirst()
        switch step {
        case .page(let page):
            return page
        case .gated(let gate, let page):
            await gate.block()
            return page
        case .failure(let failure):
            throw failure
        }
    }

    func disconnect(_ request: CloudAccountBoundRequest) async throws {
        capturedDisconnectRequests.append(request)
    }

    func diagnostics(_ request: CloudAccountBoundRequest) async -> [CloudAccountDiagnostic] {
        if !diagnosticGates.isEmpty {
            let gate = diagnosticGates.removeFirst()
            await gate.block()
        }
        return diagnosticScript
    }

    func scopeRequests() -> [CloudAccountPageRequest] { capturedScopeRequests }
    func clusterRequests() -> [CloudAccountPageRequest] { capturedClusterRequests }
    func disconnectRequests() -> [CloudAccountBoundRequest] { capturedDisconnectRequests }
    func connectRequests() -> [CloudAccountConnectRequest] { capturedConnectRequests }

    private func invalidFailure(stage: CloudAccountOperationStage) -> CloudAccountFailure {
        CloudAccountFailure(
            stage: stage,
            classification: .invalidProviderResponse,
            isRetryable: false,
            recoveryAction: .runAuthDoctor
        )
    }
}

private actor SyntheticCloudGate {
    private var continuation: CheckedContinuation<Void, Never>?
    private var isBlocked = false
    private var isOpen = false

    func block() async {
        isBlocked = true
        guard !isOpen else { return }
        await withCheckedContinuation { continuation in
            self.continuation = continuation
        }
    }

    func waitUntilBlocked() async throws {
        let deadline = ContinuousClock.now + .seconds(2)
        while !isBlocked {
            guard ContinuousClock.now < deadline else {
                throw SyntheticCloudTestError.timedOut
            }
            try await Task.sleep(for: .milliseconds(5))
        }
    }

    func open() {
        isOpen = true
        continuation?.resume()
        continuation = nil
    }
}

private actor SyntheticRetrySleepRecorder {
    private var values: [TimeInterval] = []

    func record(_ seconds: TimeInterval) {
        values.append(seconds)
    }

    func delays() -> [TimeInterval] {
        values
    }
}

private enum SyntheticCloudTestError: Error {
    case timedOut
}
