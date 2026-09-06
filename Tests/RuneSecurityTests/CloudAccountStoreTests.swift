import Foundation
import Security
import XCTest
@testable import RuneSecurity

final class CloudAccountStoreTests: XCTestCase {
    func testMissingKeychainEntitlementExplainsInstallationProblemWithoutSuggestingUnlock() {
        let store = KeychainCloudAccountStore(secretStore: UnauthorizedCloudAccountSecretStore())
        XCTAssertThrowsError(try store.accounts()) {
            XCTAssertEqual($0 as? CloudAccountStoreError, .keychainAccessNotAuthorized)
            XCTAssertTrue($0.localizedDescription.contains("signed Rune installation"))
            XCTAssertFalse($0.localizedDescription.contains("Unlock"))
        }
    }
    func testRestartRestoresMetadataAndOnlyExactGenerationCanReadCredentials() throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let record = account()
        let secret = CloudAccountCredentials(data: Data("synthetic-refresh-material".utf8), expiresAt: Date(timeIntervalSince1970: 1_900_000_000))
        try store.save(account: record, credentials: secret)

        let restarted = KeychainCloudAccountStore(secretStore: backing)
        XCTAssertEqual(try restarted.accounts(), [record])
        XCTAssertEqual(try restarted.credentials(for: record).data, secret.data)
        XCTAssertEqual(try restarted.credentials(for: record).expiresAt, secret.expiresAt)
        let metadata = String(decoding: try JSONEncoder().encode(restarted.accounts()), as: UTF8.self)
        XCTAssertFalse(metadata.contains("synthetic-refresh-material"))
        XCTAssertFalse(metadata.contains(secret.data.base64EncodedString()))
        XCTAssertEqual(backing.keys, [KeychainCloudAccountStore.storageKey])
        XCTAssertThrowsError(try restarted.credentials(for: account(id: record.id, generation: 2))) {
            XCTAssertEqual($0 as? CloudAccountStoreError, .staleGeneration)
        }
    }

    func testCredentialAndAuthorizationDescriptionsAndMirrorsRedactSecretAndAccountIdentity() {
        let record = account(label: "Synthetic local label")
        let secret = CloudAccountCredentials(data: Data("synthetic-private-material".utf8))
        let result = CloudAccountAuthorizationResult(account: record, credentials: secret)
        var output = "\(secret) \(String(reflecting: secret)) \(result) \(String(reflecting: result))"
        dump(secret, to: &output)
        dump(result, to: &output)
        for privateValue in ["synthetic-private-material", secret.data.base64EncodedString(), record.localLabel, record.id.rawValue.uuidString] {
            XCTAssertFalse(output.contains(privateValue))
        }
        XCTAssertTrue(output.contains("redacted"))
    }

    func testRotationRejectsLateReadWriteDeleteAndMetadataFromOlderGeneration() throws {
        let store = KeychainCloudAccountStore(secretStore: CloudAccountTestSecretStore())
        let initial = account()
        let refreshed = account(id: initial.id, generation: 2)
        try store.save(account: initial, credentials: credential("old"))
        try store.save(account: refreshed, credentials: credential("new"), replacing: initial.credentialGeneration)

        let staleActions: [() throws -> Void] = [
            { _ = try store.credentials(for: initial) },
            { try store.save(account: self.account(id: initial.id, generation: 3), credentials: self.credential("late"), replacing: initial.credentialGeneration) },
            { try store.updateMetadata(initial) },
            { try store.remove(accountID: initial.id, provider: initial.provider, generation: initial.credentialGeneration) }
        ]
        for action in staleActions {
            XCTAssertThrowsError(try action()) { XCTAssertEqual($0 as? CloudAccountStoreError, .staleGeneration) }
        }
        XCTAssertEqual(try store.accounts(), [refreshed])
        XCTAssertEqual(try store.credentials(for: refreshed).data, credential("new").data)
    }

    func testDisconnectDoesNotResurrectOnLateRefreshAndLeavesOtherAccountUntouched() throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let first = account()
        let second = account()
        try store.save(account: first, credentials: credential("first"))
        try store.save(account: second, credentials: credential("second"))
        try store.remove(accountID: first.id, provider: first.provider, generation: first.credentialGeneration)
        try store.remove(accountID: first.id, provider: first.provider, generation: first.credentialGeneration)
        XCTAssertThrowsError(try store.save(account: account(id: first.id, generation: 2), credentials: credential("late"), replacing: first.credentialGeneration)) {
            XCTAssertEqual($0 as? CloudAccountStoreError, .staleGeneration)
        }
        XCTAssertEqual(try store.accounts(), [second])
        XCTAssertEqual(try store.credentials(for: second).data, credential("second").data)
        try store.remove(accountID: second.id, provider: second.provider, generation: second.credentialGeneration)
        XCTAssertTrue(backing.keys.isEmpty)
    }

    func testSameOpaqueIDCannotCrossProviderBoundaries() throws {
        let store = KeychainCloudAccountStore(secretStore: CloudAccountTestSecretStore())
        let original = account()
        try store.save(account: original, credentials: credential("first"))
        let wrongProvider = account(id: original.id, generation: 1, provider: .googleCloud)
        XCTAssertThrowsError(try store.credentials(for: wrongProvider))
        XCTAssertThrowsError(try store.updateMetadata(wrongProvider))
        XCTAssertThrowsError(try store.save(account: account(id: original.id, generation: 2, provider: .googleCloud), credentials: credential("wrong"), replacing: original.credentialGeneration))
        XCTAssertThrowsError(try store.remove(accountID: original.id, provider: .googleCloud, generation: original.credentialGeneration))
        XCTAssertEqual(try store.accounts(), [original])
    }

    func testRenamePreservesCredentialDataAndExpiry() throws {
        let store = KeychainCloudAccountStore(secretStore: CloudAccountTestSecretStore())
        let original = account()
        let secret = CloudAccountCredentials(data: Data("synthetic-token".utf8), expiresAt: Date(timeIntervalSince1970: 1_900_000_000))
        try store.save(account: original, credentials: secret)
        let renamed = account(id: original.id, label: "New label")
        try store.updateMetadata(renamed)
        XCTAssertEqual(try store.accounts(), [renamed])
        XCTAssertEqual(try store.credentials(for: renamed).data, secret.data)
        XCTAssertEqual(try store.credentials(for: renamed).expiresAt, secret.expiresAt)
    }

    func testFailedRotationOrDeleteLeavesCompletePreviousRecordAndRedactsStorageErrors() throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let initial = account()
        try store.save(account: initial, credentials: credential("initial"))
        let before = backing.rawData
        backing.failWrites(true)
        XCTAssertThrowsError(try store.save(account: account(id: initial.id, generation: 2), credentials: credential("rotated"), replacing: initial.credentialGeneration)) {
            XCTAssertEqual($0 as? CloudAccountStoreError, .storageUnavailable)
            XCTAssertFalse($0.localizedDescription.contains("synthetic-secret-error"))
        }
        XCTAssertThrowsError(try store.remove(accountID: initial.id, provider: initial.provider, generation: initial.credentialGeneration))
        XCTAssertEqual(backing.rawData, before)
        XCTAssertEqual(try store.accounts(), [initial])
        XCTAssertEqual(try store.credentials(for: initial).data, credential("initial").data)
    }

    func testMalformedVersionDuplicateIdentityAndOversizedStoreFailClosedWithoutOverwriting() throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        let record = account()
        try store.save(account: record, credentials: credential("fixture"))
        let object = try XCTUnwrap(JSONSerialization.jsonObject(with: XCTUnwrap(backing.rawData)) as? [String: Any])
        var futureVersion = object
        futureVersion["version"] = 2
        var duplicates = object
        let entries = try XCTUnwrap(object["entries"] as? [[String: Any]])
        duplicates["entries"] = entries + entries
        let corruptions = [
            Data("broken synthetic JSON".utf8),
            try JSONSerialization.data(withJSONObject: futureVersion),
            try JSONSerialization.data(withJSONObject: duplicates),
            Data(repeating: 0, count: KeychainCloudAccountStore.maximumStoreBytes + 1)
        ]
        for corrupted in corruptions {
            backing.replaceRawData(corrupted)
            XCTAssertThrowsError(try store.accounts()) { XCTAssertEqual($0 as? CloudAccountStoreError, .corruptedStore) }
            XCTAssertThrowsError(try store.save(account: account(), credentials: credential("new")))
            XCTAssertEqual(backing.rawData, corrupted)
        }
    }

    func testBoundedCredentialsAccountsAndInvalidDatesAreRejectedBeforeWriting() throws {
        let backing = CloudAccountTestSecretStore()
        let store = KeychainCloudAccountStore(secretStore: backing)
        for data in [Data(), Data(repeating: 1, count: KeychainCloudAccountStore.maximumCredentialBytes + 1)] {
            XCTAssertThrowsError(try store.save(account: account(), credentials: CloudAccountCredentials(data: data)))
        }
        XCTAssertThrowsError(try store.save(account: account(generation: 0), credentials: credential("fixture")))
        XCTAssertThrowsError(try store.save(account: account(label: String(repeating: "x", count: 257)), credentials: credential("fixture")))
        XCTAssertThrowsError(try store.save(account: account(), credentials: CloudAccountCredentials(data: Data([1]), expiresAt: Date(timeIntervalSinceReferenceDate: .infinity))))
        XCTAssertTrue(backing.keys.isEmpty)
        for _ in 0..<KeychainCloudAccountStore.maximumAccounts {
            try store.save(account: account(), credentials: credential("fixture"))
        }
        XCTAssertThrowsError(try store.save(account: account(), credentials: credential("overflow"))) {
            XCTAssertEqual($0 as? CloudAccountStoreError, .limitExceeded)
        }
        XCTAssertEqual(try store.accounts().count, KeychainCloudAccountStore.maximumAccounts)
    }

    private func account(id: CloudAccountID = CloudAccountID(), generation: UInt64 = 1, provider: CloudAccountProvider = .azure, label: String = "Synthetic") -> CloudAccountRecord {
        CloudAccountRecord(id: id, provider: provider, credentialGeneration: CloudAccountGeneration(rawValue: generation), localLabel: label)
    }

    private func credential(_ marker: String) -> CloudAccountCredentials {
        CloudAccountCredentials(data: Data("synthetic-\(marker)".utf8))
    }
}

private struct UnauthorizedCloudAccountSecretStore: SecretStore {
    func set(_ value: Data, for key: String) throws { throw KeychainError.operationFailed(status: errSecMissingEntitlement) }
    func get(for key: String) throws -> Data? { throw KeychainError.operationFailed(status: errSecMissingEntitlement) }
    func delete(for key: String) throws { throw KeychainError.operationFailed(status: errSecMissingEntitlement) }
}

/// Injectable Keychain boundary: no test writes account secrets to the user's Keychain.
final class CloudAccountTestSecretStore: SecretStore, @unchecked Sendable {
    private let lock = NSLock()
    private var values: [String: Data] = [:]
    private var shouldFailWrites = false
    private var reads = 0

    var keys: Set<String> { lock.withLock { Set(values.keys) } }
    var rawData: Data? { lock.withLock { values[KeychainCloudAccountStore.storageKey] } }
    var readCount: Int { lock.withLock { reads } }

    func failWrites(_ enabled: Bool) { lock.withLock { shouldFailWrites = enabled } }
    func replaceRawData(_ data: Data) { lock.withLock { values[KeychainCloudAccountStore.storageKey] = data } }
    func set(_ value: Data, for key: String) throws {
        try lock.withLock {
            if shouldFailWrites { throw FixtureError() }
            values[key] = value
        }
    }
    func get(for key: String) throws -> Data? {
        lock.withLock { reads += 1; return values[key] }
    }
    func delete(for key: String) throws {
        try lock.withLock {
            if shouldFailWrites { throw FixtureError() }
            values.removeValue(forKey: key)
        }
    }
    private struct FixtureError: LocalizedError {
        var errorDescription: String? { "synthetic-secret-error" }
    }
}
