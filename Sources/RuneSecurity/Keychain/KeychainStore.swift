import Foundation
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

    public init(service: String = "com.rune.app") {
        self.service = service
    }

    public func set(_ value: Data, for key: String) throws {
        try delete(for: key)

        let query: [CFString: Any] = [
            kSecClass: kSecClassGenericPassword,
            kSecAttrService: service,
            kSecAttrAccount: key,
            kSecValueData: value
        ]

        let status = SecItemAdd(query as CFDictionary, nil)
        guard status == errSecSuccess else {
            throw KeychainError.operationFailed(status: status)
        }
    }

    public func get(for key: String) throws -> Data? {
        let query: [CFString: Any] = [
            kSecClass: kSecClassGenericPassword,
            kSecAttrService: service,
            kSecAttrAccount: key,
            kSecMatchLimit: kSecMatchLimitOne,
            kSecReturnData: true
        ]

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
        let query: [CFString: Any] = [
            kSecClass: kSecClassGenericPassword,
            kSecAttrService: service,
            kSecAttrAccount: key
        ]

        let status = SecItemDelete(query as CFDictionary)
        guard status == errSecSuccess || status == errSecItemNotFound else {
            throw KeychainError.operationFailed(status: status)
        }
    }
}
