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

    func testPersistentCoordinatorRestoresRenameSyncAndRefreshThenDisconnects() async throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let initial = account(id: uuid(90), generation: 1, label: "Provider label")
        let rotated = account(id: uuid(90), generation: 2, label: "Provider label")
        let connector = SyntheticCloudAccountConnector(connectSteps: [.record(initial)], refreshSteps: [.record(rotated)])
        let fixedNow = now
        let coordinator = try CloudAccountCoordinator(connectors: [connector], restoringFrom: store, now: { fixedNow })
        _ = try await coordinator.connect(provider: .azure, localLabel: "Local account")
        _ = try await coordinator.rename(accountID: initial.id, localLabel: "Renamed account")
        _ = try await coordinator.synchronize(accountID: initial.id)
        XCTAssertEqual(try store.accounts().first?.lastSuccessfulSync, now)
        _ = try await coordinator.refresh(accountID: initial.id)

        let restarted = try CloudAccountCoordinator(connectors: [connector], restoringFrom: KeychainCloudAccountStore(secretStore: backing))
        let restored = await restarted.connectedAccounts()
        XCTAssertEqual(restored.count, 1)
        XCTAssertEqual(restored.first?.localLabel, "Renamed account")
        XCTAssertEqual(restored.first?.credentialGeneration, rotated.credentialGeneration)
        XCTAssertEqual(restored.first?.lastSuccessfulSync, now)
        try await restarted.disconnect(accountID: initial.id)
        XCTAssertTrue(try store.accounts().isEmpty)
        XCTAssertTrue(backing.keys.isEmpty)
    }

    func testFailedAccountCommitAndRenameDoNotPublishMemoryOnlyState() async throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let initial = account(id: uuid(91), generation: 1, label: "Original")
        let connector = SyntheticCloudAccountConnector(connectSteps: [.record(initial), .record(initial)])
        let coordinator = try CloudAccountCoordinator(connectors: [connector], restoringFrom: store)
        backing.failWrites(true)
        do {
            _ = try await coordinator.connect(provider: .azure)
            XCTFail("A failed Keychain commit must not publish the account")
        } catch { XCTAssertEqual(error as? CloudAccountStoreError, .storageUnavailable) }
        let failedAccounts = await coordinator.connectedAccounts()
        XCTAssertTrue(failedAccounts.isEmpty)
        XCTAssertTrue(backing.keys.isEmpty)

        backing.failWrites(false)
        _ = try await coordinator.connect(provider: .azure)
        backing.failWrites(true)
        do {
            _ = try await coordinator.rename(accountID: initial.id, localLabel: "Unsaved label")
            XCTFail("A failed rename must leave the previous metadata")
        } catch { XCTAssertEqual(error as? CloudAccountStoreError, .storageUnavailable) }
        let unchanged = await coordinator.account(id: initial.id)
        XCTAssertEqual(unchanged, initial)
        XCTAssertEqual(try store.accounts(), [initial])
    }

    func testExpiringCredentialReadersShareOneRefreshAndCanceledReaderDoesNotCancelOthers() async throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let initial = account(id: uuid(92), generation: 1, label: "Synthetic")
        let rotated = account(id: uuid(92), generation: 2, label: "Synthetic")
        try store.save(account: initial, credentials: CloudAccountCredentials(data: Data("synthetic-old".utf8), expiresAt: now.addingTimeInterval(20)))
        let gate = SyntheticCloudGate()
        let fresh = CloudAccountCredentials(data: Data("synthetic-rotated".utf8), expiresAt: now.addingTimeInterval(3_600))
        let connector = SyntheticCloudAccountConnector(connectSteps: [], refreshSteps: [.gated(gate, rotated)], credentials: fresh)
        let fixedNow = now
        let coordinator = try CloudAccountCoordinator(connectors: [connector], restoringFrom: store, now: { fixedNow })
        let baseline = backing.readCount
        let readers = (0..<8).map { _ in Task { try await coordinator.credentials(accountID: initial.id) } }
        try await gate.waitUntilBlocked()
        // Every reader has actually entered the coordinator and read the expired generation
        // before the provider is released. This avoids scheduling-dependent sleep assertions.
        try await waitForReads(backing, atLeast: baseline + readers.count)
        readers[0].cancel()
        await gate.open()
        do {
            _ = try await readers[0].value
            XCTFail("The canceled caller should observe cancellation")
        } catch { XCTAssertTrue(error is CancellationError) }
        for reader in readers.dropFirst() {
            let result = try await reader.value
            XCTAssertEqual(result.data, fresh.data)
        }
        let requests = await connector.refreshRequests()
        XCTAssertEqual(requests.count, 1)
        XCTAssertEqual(requests.first?.credentialGeneration, initial.credentialGeneration)
        XCTAssertEqual(try store.accounts().first?.credentialGeneration, rotated.credentialGeneration)
        let cached = try await coordinator.credentials(accountID: initial.id)
        XCTAssertEqual(cached.data, fresh.data)
        let requestsAfterCachedRead = await connector.refreshRequests()
        XCTAssertEqual(requestsAfterCachedRead.count, 1)
    }

    func testDiscoveryWaitsForActiveRefreshAndUsesCommittedGeneration() async throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let initial = account(id: uuid(93), generation: 1, label: "Synthetic")
        let rotated = account(id: uuid(93), generation: 2, label: "Synthetic")
        let gate = SyntheticCloudGate()
        let connector = SyntheticCloudAccountConnector(connectSteps: [.record(initial)], refreshSteps: [.gated(gate, rotated)])
        let coordinator = try CloudAccountCoordinator(connectors: [connector], restoringFrom: store)
        _ = try await coordinator.connect(provider: .azure)
        let refresh = Task { try await coordinator.refresh(accountID: initial.id) }
        try await gate.waitUntilBlocked()
        let baseline = backing.readCount
        let discovery = Task { try await coordinator.synchronize(accountID: initial.id) }
        try await waitForReads(backing, atLeast: baseline + 1)
        let prematureRequests = await connector.scopeRequests()
        XCTAssertTrue(prematureRequests.isEmpty)
        await gate.open()
        _ = try await refresh.value
        let discovered = try await discovery.value
        XCTAssertEqual(discovered.credentialGeneration, rotated.credentialGeneration)
        let requests = await connector.scopeRequests()
        XCTAssertEqual(requests.map(\.credentialGeneration), [rotated.credentialGeneration])
    }

    func testDisconnectRejectsLateRefreshWithoutPersistingItsSecrets() async throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let initial = account(id: uuid(94), generation: 1, label: "Synthetic")
        let late = account(id: uuid(94), generation: 2, label: "Late")
        let gate = SyntheticCloudGate()
        let connector = SyntheticCloudAccountConnector(connectSteps: [.record(initial)], refreshSteps: [.gated(gate, late)])
        let coordinator = try CloudAccountCoordinator(connectors: [connector], restoringFrom: store)
        _ = try await coordinator.connect(provider: .azure)
        let refresh = Task { try await coordinator.refresh(accountID: initial.id) }
        try await gate.waitUntilBlocked()
        try await coordinator.disconnect(accountID: initial.id)
        await gate.open()
        do {
            _ = try await refresh.value
            XCTFail("Disconnect must invalidate a non-cooperative refresh")
        } catch { XCTAssertEqual(error as? CloudAccountCoordinatorError, .superseded) }
        XCTAssertTrue(backing.keys.isEmpty)
        let current = await coordinator.connectedAccounts()
        XCTAssertTrue(current.isEmpty)
    }

    func testCancelDiscoveryWhileWaitingForRefreshDoesNotCancelSharedRefresh() async throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let initial = account(id: uuid(99), generation: 1, label: "Synthetic")
        let rotated = account(id: uuid(99), generation: 2, label: "Synthetic")
        let gate = SyntheticCloudGate()
        let connector = SyntheticCloudAccountConnector(connectSteps: [.record(initial)], refreshSteps: [.gated(gate, rotated)])
        let coordinator = try CloudAccountCoordinator(connectors: [connector], restoringFrom: store)
        _ = try await coordinator.connect(provider: .azure)
        let refresh = Task { try await coordinator.refresh(accountID: initial.id) }
        try await gate.waitUntilBlocked()
        let baseline = backing.readCount
        let discovery = Task { try await coordinator.synchronize(accountID: initial.id) }
        try await waitForReads(backing, atLeast: baseline + 1)
        await coordinator.cancelSynchronization(accountID: initial.id)
        await gate.open()
        let accepted = try await refresh.value
        XCTAssertEqual(accepted.credentialGeneration, rotated.credentialGeneration)
        do {
            _ = try await discovery.value
            XCTFail("Canceled discovery must not start provider reads after the refresh")
        } catch { XCTAssertEqual(error as? CloudAccountCoordinatorError, .superseded) }
        let requests = await connector.scopeRequests()
        XCTAssertTrue(requests.isEmpty)
    }

    func testCanceledConnectCallerCannotPersistNonCooperativeResult() async throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let initial = account(id: uuid(100), generation: 1, label: "Synthetic")
        let gate = SyntheticCloudGate()
        let connector = SyntheticCloudAccountConnector(connectSteps: [.gated(gate, initial)])
        let coordinator = try CloudAccountCoordinator(connectors: [connector], restoringFrom: store)
        let connection = Task { try await coordinator.connect(provider: .azure) }
        try await gate.waitUntilBlocked()
        connection.cancel()
        await gate.open()
        do {
            _ = try await connection.value
            XCTFail("A canceled caller must not publish credentials")
        } catch { XCTAssertEqual((error as? CloudAccountFailure)?.classification, .canceled) }
        XCTAssertTrue(backing.keys.isEmpty)
    }

    func testFailedRefreshReleasesSharedOperationAndLeavesStoredCredentialUnchanged() async throws {
        let store = KeychainCloudAccountStore(secretStore: CloudAccountTestSecretStore())
        let initial = account(id: uuid(101), generation: 1, label: "Synthetic")
        let rotated = account(id: uuid(101), generation: 2, label: "Synthetic")
        let failure = CloudAccountFailure(stage: .accountRefresh, classification: .offline, isRetryable: true, recoveryAction: .retry)
        let connector = SyntheticCloudAccountConnector(connectSteps: [.record(initial)], refreshSteps: [.failure(failure), .record(rotated)])
        let coordinator = try CloudAccountCoordinator(connectors: [connector], restoringFrom: store)
        _ = try await coordinator.connect(provider: .azure)
        do {
            _ = try await coordinator.refresh(accountID: initial.id)
            XCTFail("Expected the recoverable provider failure")
        } catch { XCTAssertEqual(error as? CloudAccountFailure, failure) }
        XCTAssertEqual(try store.accounts(), [initial])
        let retry = try await coordinator.refresh(accountID: initial.id)
        XCTAssertEqual(retry.credentialGeneration, rotated.credentialGeneration)
        XCTAssertEqual(try store.accounts(), [rotated])
    }

    func testReconnectWinsOverLateRefreshAndDisconnectCancelsPendingReconnect() async throws {
        for disconnect in [false, true] {
            let backing = CloudAccountTestSecretStore()
            let store = KeychainCloudAccountStore(secretStore: backing)
            let initial = account(id: uuid(95), generation: 1, label: "Initial")
            let late = account(id: uuid(95), generation: 2, label: "Late")
            let reconnected = account(id: uuid(95), generation: 3, label: "Reconnected")
            let gate = SyntheticCloudGate()
            let connector = SyntheticCloudAccountConnector(
                connectSteps: disconnect ? [.record(initial), .gated(gate, reconnected)] : [.record(initial), .record(reconnected)],
                refreshSteps: [.gated(gate, late)]
            )
            let coordinator = try CloudAccountCoordinator(connectors: [connector], restoringFrom: store)
            _ = try await coordinator.connect(provider: .azure)
            let delayed = Task {
                if disconnect { return try await coordinator.connect(provider: .azure) }
                return try await coordinator.refresh(accountID: initial.id)
            }
            try await gate.waitUntilBlocked()
            if disconnect {
                try await coordinator.disconnect(accountID: initial.id)
            } else {
                _ = try await coordinator.connect(provider: .azure)
            }
            await gate.open()
            do {
                _ = try await delayed.value
                XCTFail("A superseded operation must not commit its credential generation")
            } catch { XCTAssertEqual(error as? CloudAccountCoordinatorError, .superseded) }
            XCTAssertEqual(try store.accounts(), disconnect ? [] : [reconnected.updatingLocalLabel(initial.localLabel)])
        }
    }

    func testFailedReconnectDoesNotCancelRefreshAndSuccessfulReconnectKeepsLocalLabel() async throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let initial = account(id: uuid(102), generation: 1, label: "Provider label")
        let rotated = account(id: uuid(102), generation: 2, label: "Provider label")
        let reconnected = account(id: uuid(102), generation: 3, label: "Different provider label")
        let gate = SyntheticCloudGate()
        let connector = SyntheticCloudAccountConnector(connectSteps: [.record(initial), .record(reconnected), .record(reconnected)], refreshSteps: [.gated(gate, rotated)])
        let coordinator = try CloudAccountCoordinator(connectors: [connector], restoringFrom: store)
        _ = try await coordinator.connect(provider: .azure, localLabel: "My synthetic account")
        let refresh = Task { try await coordinator.refresh(accountID: initial.id) }
        try await gate.waitUntilBlocked()
        backing.failWrites(true)
        do {
            _ = try await coordinator.connect(provider: .azure)
            XCTFail("Expected the reconnect commit to fail")
        } catch { XCTAssertEqual(error as? CloudAccountStoreError, .storageUnavailable) }
        backing.failWrites(false)
        await gate.open()
        let refreshed = try await refresh.value
        XCTAssertEqual(refreshed.credentialGeneration, rotated.credentialGeneration)
        let accepted = try await coordinator.connect(provider: .azure)
        XCTAssertEqual(accepted.localLabel, "My synthetic account")
        XCTAssertEqual(accepted.credentialGeneration, reconnected.credentialGeneration)
        XCTAssertEqual(try store.accounts(), [accepted])
    }

    func testLocalDisconnectWorksWithProviderDisabledOrOffline() async throws {
        for disabled in [false, true] {
            let store = KeychainCloudAccountStore(secretStore: CloudAccountTestSecretStore())
            let record = account(id: uuid(96), generation: 1, label: "Synthetic")
            try store.save(account: record, credentials: CloudAccountCredentials(data: Data("synthetic-local-only".utf8)))
            let connector = SyntheticCloudAccountConnector(connectSteps: [], disconnectFailure: CloudAccountFailure(stage: .localDisconnect, classification: .offline, isRetryable: true, recoveryAction: .retry))
            let coordinator = try CloudAccountCoordinator(connectors: disabled ? [] : [connector], restoringFrom: store)
            try await coordinator.disconnect(accountID: record.id)
            XCTAssertTrue(try store.accounts().isEmpty)
            let current = await coordinator.connectedAccounts()
            XCTAssertTrue(current.isEmpty)
        }
    }

    func testFailedLocalDeletionRetainsAccountAndAllowsRetry() async throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let record = account(id: uuid(97), generation: 1, label: "Synthetic")
        try store.save(account: record, credentials: CloudAccountCredentials(data: Data("synthetic-local-only".utf8)))
        let coordinator = try CloudAccountCoordinator(connectors: [], restoringFrom: store)
        backing.failWrites(true)
        do {
            try await coordinator.disconnect(accountID: record.id)
            XCTFail("A failed secret deletion must remain visible and retryable")
        } catch { XCTAssertEqual(error as? CloudAccountStoreError, .storageUnavailable) }
        let current = await coordinator.account(id: record.id)
        XCTAssertEqual(current, record)
        XCTAssertEqual(try store.accounts(), [record])
        backing.failWrites(false)
        try await coordinator.disconnect(accountID: record.id)
        XCTAssertTrue(backing.keys.isEmpty)
    }

    func testExpiredAuthorizationNeverReachesPersistentStore() async throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let record = account(id: uuid(98), generation: 1, label: "Synthetic")
        let connector = SyntheticCloudAccountConnector(connectSteps: [.record(record)], credentials: CloudAccountCredentials(data: Data("synthetic-expired".utf8), expiresAt: now.addingTimeInterval(-1)))
        let fixedNow = now
        let coordinator = try CloudAccountCoordinator(connectors: [connector], restoringFrom: store, now: { fixedNow })
        do {
            _ = try await coordinator.connect(provider: .azure)
            XCTFail("Expired credentials must be rejected before persistence")
        } catch { XCTAssertEqual(error as? CloudAccountCoordinatorError, .invalidConnectorResult) }
        XCTAssertTrue(backing.keys.isEmpty)
    }

    private func waitForReads(_ store: CloudAccountTestSecretStore, atLeast count: Int) async throws {
        let deadline = ContinuousClock.now + .seconds(2)
        while store.readCount < count {
            guard ContinuousClock.now < deadline else { throw SyntheticCloudTestError.timedOut }
            try await Task.sleep(for: .milliseconds(5))
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
    private var capturedRefreshRequests: [CloudAccountBoundRequest] = []
    private var capturedScopeRequests: [CloudAccountPageRequest] = []
    private var capturedClusterRequests: [CloudAccountPageRequest] = []
    private var capturedDisconnectRequests: [CloudAccountBoundRequest] = []
    private let credentials: CloudAccountCredentials
    private let disconnectFailure: CloudAccountFailure?

    init(
        connectSteps: [SyntheticConnectStep],
        refreshSteps: [SyntheticRefreshStep] = [],
        scopeSteps: [SyntheticScopeStep] = [.page(CloudDiscoveryScopePage(scopes: []))],
        clusterSteps: [SyntheticClusterStep] = [.page(CloudClusterDiscoveryPage(candidates: []))],
        diagnostics: [CloudAccountDiagnostic] = [],
        diagnosticGates: [SyntheticCloudGate] = [],
        credentials: CloudAccountCredentials = CloudAccountCredentials(data: Data("synthetic-credential".utf8)),
        disconnectFailure: CloudAccountFailure? = nil
    ) {
        self.connectScript = connectSteps
        self.refreshScript = refreshSteps
        self.scopeScript = scopeSteps
        self.clusterScript = clusterSteps
        self.diagnosticScript = diagnostics
        self.diagnosticGates = diagnosticGates
        self.credentials = credentials
        self.disconnectFailure = disconnectFailure
    }

    func connect(_ request: CloudAccountConnectRequest) async throws -> CloudAccountAuthorizationResult {
        capturedConnectRequests.append(request)
        guard !connectScript.isEmpty else { throw invalidFailure(stage: .authorization) }
        let step = connectScript.removeFirst()
        switch step {
        case .record(let account):
            return CloudAccountAuthorizationResult(
                account: account, credentials: credentials
            )
        case .gated(let gate, let account):
            await gate.block()
            return CloudAccountAuthorizationResult(
                account: account, credentials: credentials
            )
        case .failure(let failure):
            throw failure
        }
    }

    func refresh(_ request: CloudAccountBoundRequest) async throws -> CloudAccountAuthorizationResult {
        capturedRefreshRequests.append(request)
        guard !refreshScript.isEmpty else { throw invalidFailure(stage: .accountRefresh) }
        let step = refreshScript.removeFirst()
        switch step {
        case .record(let account):
            return CloudAccountAuthorizationResult(
                account: account, credentials: credentials
            )
        case .gated(let gate, let account):
            await gate.block()
            return CloudAccountAuthorizationResult(
                account: account, credentials: credentials
            )
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
        if let disconnectFailure { throw disconnectFailure }
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
    func refreshRequests() -> [CloudAccountBoundRequest] { capturedRefreshRequests }

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
