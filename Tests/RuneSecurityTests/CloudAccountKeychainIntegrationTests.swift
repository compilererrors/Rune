import Foundation
import Security
import XCTest
@testable import RuneSecurity

final class CloudAccountKeychainIntegrationTests: XCTestCase {
    func testRealKeychainRoundTripRotationProtectionAndLocalDeletion() throws {
        guard ProcessInfo.processInfo.environment["RUNE_RUN_CLOUD_ACCOUNT_KEYCHAIN_TESTS"] == "1" else {
            throw XCTSkip("Run from a provisioned test host with RUNE_RUN_CLOUD_ACCOUNT_KEYCHAIN_TESTS=1 to verify Data Protection Keychain access.")
        }
        // A unique service isolates this fixture from every existing Rune credential.
        let service = "app.rune.tests.cloud-account.\(UUID().uuidString.lowercased())"
        let backing = KeychainStore(service: service, useDataProtectionKeychain: true)
        defer { try? backing.delete(for: KeychainCloudAccountStore.storageKey) }
        let store = KeychainCloudAccountStore(secretStore: backing)
        let initial = CloudAccountRecord(id: CloudAccountID(), provider: .azure, credentialGeneration: CloudAccountGeneration(rawValue: 1), localLabel: "Synthetic Keychain fixture")
        let rotated = CloudAccountRecord(id: initial.id, provider: initial.provider, credentialGeneration: CloudAccountGeneration(rawValue: 2), localLabel: initial.localLabel)
        try store.save(account: initial, credentials: CloudAccountCredentials(data: Data("synthetic-first-token".utf8)))
        let restarted = KeychainCloudAccountStore(secretStore: KeychainStore(service: service, useDataProtectionKeychain: true))
        XCTAssertEqual(try restarted.accounts(), [initial])
        XCTAssertEqual(try restarted.credentials(for: initial).data, Data("synthetic-first-token".utf8))

        let query: [CFString: Any] = [
            kSecClass: kSecClassGenericPassword,
            kSecAttrService: service,
            kSecAttrAccount: KeychainCloudAccountStore.storageKey,
            kSecUseDataProtectionKeychain: true,
            kSecReturnAttributes: true,
            kSecMatchLimit: kSecMatchLimitOne
        ]
        var attributes: CFTypeRef?
        XCTAssertEqual(SecItemCopyMatching(query as CFDictionary, &attributes), errSecSuccess)
        let values = try XCTUnwrap(attributes as? [String: Any])
        XCTAssertEqual(values[kSecAttrAccessible as String] as? String, kSecAttrAccessibleWhenUnlockedThisDeviceOnly as String)

        try restarted.save(account: rotated, credentials: CloudAccountCredentials(data: Data("synthetic-rotated-token".utf8)), replacing: initial.credentialGeneration)
        XCTAssertThrowsError(try store.remove(accountID: initial.id, provider: initial.provider, generation: initial.credentialGeneration)) {
            XCTAssertEqual($0 as? CloudAccountStoreError, .staleGeneration)
        }
        XCTAssertEqual(try store.credentials(for: rotated).data, Data("synthetic-rotated-token".utf8))
        try store.remove(accountID: rotated.id, provider: rotated.provider, generation: rotated.credentialGeneration)
        XCTAssertNil(try backing.get(for: KeychainCloudAccountStore.storageKey))
    }
}
