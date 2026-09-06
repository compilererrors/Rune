import Foundation
import RuneCore
import Security

public protocol SecretStore: Sendable {
    func set(_ value: Data, for key: String) throws
    func get(for key: String) throws -> Data?
    func delete(for key: String) throws
}

public enum KeychainError: LocalizedError, Sendable {
    case operationFailed(status: OSStatus)

    public var errorDescription: String? {
        switch self {
        case let .operationFailed(status):
            return "Keychain operation failed with status: \(status)"
        }
    }
}

public final class KeychainStore: SecretStore {
    private let service: String
    private let useDataProtectionKeychain: Bool

    public init(service: String = RuneApplicationIdentifiers.keychainService, useDataProtectionKeychain: Bool = false) {
        self.service = service
        self.useDataProtectionKeychain = useDataProtectionKeychain
    }

    private func identityQuery(for key: String) -> [CFString: Any] {
        var query: [CFString: Any] = [
            kSecClass: kSecClassGenericPassword,
            kSecAttrService: service,
            kSecAttrAccount: key
        ]
        // macOS ignores accessibility classes in the legacy file-based keychain. Keep that
        // backend for existing profiles until migration; new account records explicitly use DP.
        if useDataProtectionKeychain {
            query[kSecUseDataProtectionKeychain] = true
            query[kSecAttrSynchronizable] = false
        }
        return query
    }

    public func set(_ value: Data, for key: String) throws {
        let identityQuery = identityQuery(for: key)
        let update: [CFString: Any] = [
            kSecAttrAccessible: kSecAttrAccessibleWhenUnlockedThisDeviceOnly,
            kSecValueData: value
        ]

        let updateStatus = SecItemUpdate(identityQuery as CFDictionary, update as CFDictionary)
        if updateStatus == errSecSuccess { return }
        guard updateStatus == errSecItemNotFound else {
            throw KeychainError.operationFailed(status: updateStatus)
        }

        var query = identityQuery
        query[kSecAttrAccessible] = kSecAttrAccessibleWhenUnlockedThisDeviceOnly
        query[kSecValueData] = value
        let status = SecItemAdd(query as CFDictionary, nil)
        guard status == errSecSuccess else {
            throw KeychainError.operationFailed(status: status)
        }
    }

    public func get(for key: String) throws -> Data? {
        var query = identityQuery(for: key)
        query[kSecMatchLimit] = kSecMatchLimitOne
        query[kSecReturnData] = true

        var item: CFTypeRef?
        let status = SecItemCopyMatching(query as CFDictionary, &item)

        switch status {
        case errSecSuccess:
            return item as? Data
        case errSecItemNotFound:
            return nil
        default:
            throw KeychainError.operationFailed(status: status)
        }
    }

    public func delete(for key: String) throws {
        let query = identityQuery(for: key)

        let status = SecItemDelete(query as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else {
            throw KeychainError.operationFailed(status: status)
        }
    }
}
