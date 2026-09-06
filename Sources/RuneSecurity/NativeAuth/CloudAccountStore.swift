import Foundation
import Security

/// Provider-specific token/temporary-credential data. Only the account store may persist it.
/// Deliberately not Codable, so metadata encoders cannot accidentally include credentials.
public struct CloudAccountCredentials: Sendable, CustomStringConvertible, CustomDebugStringConvertible, CustomReflectable {
    public let data: Data
    public let expiresAt: Date?

    public init(data: Data, expiresAt: Date? = nil) {
        self.data = data
        self.expiresAt = expiresAt
    }

    public var description: String { "CloudAccountCredentials(<redacted>)" }
    public var debugDescription: String { description }
    public var customMirror: Mirror { Mirror(self, children: ["credentials": "<redacted>"]) }
}

public enum CloudAccountStoreError: Error, LocalizedError, Equatable, Sendable {
    case corruptedStore
    case invalidRecord
    case limitExceeded
    case staleGeneration
    case storageUnavailable
    case keychainAccessNotAuthorized

    public var errorDescription: String? {
        switch self {
        case .corruptedStore: "The local cloud account store could not be decoded."
        case .invalidRecord: "The cloud account credentials or metadata are invalid."
        case .limitExceeded: "The local cloud account storage limit was reached."
        case .staleGeneration: "The cloud account credentials changed. Retry with the current account."
        case .storageUnavailable: "Rune could not access the cloud accounts in Keychain. Unlock Keychain and retry."
        case .keychainAccessNotAuthorized: "This Rune installation is not authorized to access cloud accounts in Keychain. Use a signed Rune installation and retry."
        }
    }
}

/// The coordinator commits credentials only after accepting a connector result. Connectors
/// may read the exact generation for an API request, but must never publish credentials.
public protocol CloudAccountStoring: Sendable {
    func accounts() throws -> [CloudAccountRecord]
    func credentials(for account: CloudAccountRecord) throws -> CloudAccountCredentials
    func save(account: CloudAccountRecord, credentials: CloudAccountCredentials, replacing generation: CloudAccountGeneration?) throws
    func updateMetadata(_ account: CloudAccountRecord) throws
    func remove(accountID: CloudAccountID, provider: CloudAccountProvider, generation: CloudAccountGeneration) throws
}

/// Metadata and secrets have separate projections but share one bounded Keychain item, so a
/// failed write cannot publish metadata without its credentials or orphan a rotated token.
/// No index, token, or account identity is written to preferences or the filesystem.
public final class KeychainCloudAccountStore: CloudAccountStoring, Sendable {
    private struct Entry: Codable {
        var account: CloudAccountRecord
        var credentialData: Data
        var expiresAt: Date?
    }

    private struct Index: Codable {
        var version = 1
        var entries: [Entry] = []
    }

    // Covers separate store instances in this process as well as concurrent callers. Keychain
    // I/O stays synchronous inside this short transaction; there are no suspension points.
    private static let transactionLock = NSLock()
    static let storageKey = "rune.cloud-accounts.v1"
    static let maximumAccounts = 64
    static let maximumCredentialBytes = 65_536
    static let maximumStoreBytes = 8_388_608
    private let secretStore: any SecretStore

    public init(secretStore: any SecretStore = KeychainStore(useDataProtectionKeychain: true)) {
        self.secretStore = secretStore
    }

    public func accounts() throws -> [CloudAccountRecord] {
        try Self.transactionLock.withLock {
            try load().entries.map(\.account).sorted { $0.id.rawValue.uuidString < $1.id.rawValue.uuidString }
        }
    }

    public func credentials(for account: CloudAccountRecord) throws -> CloudAccountCredentials {
        try Self.transactionLock.withLock {
            guard let entry = try load().entries.first(where: { $0.account.id == account.id }),
                  entry.account.provider == account.provider,
                  entry.account.credentialGeneration == account.credentialGeneration else {
                throw CloudAccountStoreError.staleGeneration
            }
            return CloudAccountCredentials(data: entry.credentialData, expiresAt: entry.expiresAt)
        }
    }

    public func save(
        account: CloudAccountRecord,
        credentials: CloudAccountCredentials,
        replacing generation: CloudAccountGeneration? = nil
    ) throws {
        try Self.transactionLock.withLock {
            let entry = Entry(account: account, credentialData: credentials.data, expiresAt: credentials.expiresAt)
            guard Self.isValid(entry) else { throw CloudAccountStoreError.invalidRecord }
            var index = try load()
            if let offset = index.entries.firstIndex(where: { $0.account.id == account.id }) {
                let current = index.entries[offset].account
                guard current.provider == account.provider,
                      generation == current.credentialGeneration,
                      account.credentialGeneration > current.credentialGeneration else {
                    throw CloudAccountStoreError.staleGeneration
                }
                index.entries[offset] = entry
            } else {
                guard generation == nil else { throw CloudAccountStoreError.staleGeneration }
                guard index.entries.count < Self.maximumAccounts else { throw CloudAccountStoreError.limitExceeded }
                index.entries.append(entry)
            }
            try write(index)
        }
    }

    public func updateMetadata(_ account: CloudAccountRecord) throws {
        try Self.transactionLock.withLock {
            var index = try load()
            guard let offset = index.entries.firstIndex(where: { $0.account.id == account.id }),
                  index.entries[offset].account.provider == account.provider,
                  index.entries[offset].account.credentialGeneration == account.credentialGeneration else {
                throw CloudAccountStoreError.staleGeneration
            }
            index.entries[offset].account = account
            guard Self.isValid(index.entries[offset]) else { throw CloudAccountStoreError.invalidRecord }
            try write(index)
        }
    }

    public func remove(accountID: CloudAccountID, provider: CloudAccountProvider, generation: CloudAccountGeneration) throws {
        try Self.transactionLock.withLock {
            var index = try load()
            guard let offset = index.entries.firstIndex(where: { $0.account.id == accountID }) else { return }
            guard index.entries[offset].account.provider == provider,
                  index.entries[offset].account.credentialGeneration == generation else {
                throw CloudAccountStoreError.staleGeneration
            }
            index.entries.remove(at: offset)
            if index.entries.isEmpty {
                do { try secretStore.delete(for: Self.storageKey) }
                catch { throw Self.storageFailure(error) }
            } else {
                try write(index)
            }
        }
    }

    private func load() throws -> Index {
        let data: Data?
        do { data = try secretStore.get(for: Self.storageKey) }
        catch { throw Self.storageFailure(error) }
        guard let data else { return Index() }
        guard data.count <= Self.maximumStoreBytes else { throw CloudAccountStoreError.corruptedStore }
        let index: Index
        do { index = try JSONDecoder().decode(Index.self, from: data) }
        catch { throw CloudAccountStoreError.corruptedStore }
        guard index.version == 1,
              index.entries.count <= Self.maximumAccounts,
              Set(index.entries.map(\.account.id)).count == index.entries.count,
              index.entries.allSatisfy(Self.isValid) else {
            throw CloudAccountStoreError.corruptedStore
        }
        return index
    }

    private func write(_ index: Index) throws {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys]
        let data: Data
        do { data = try encoder.encode(index) }
        catch { throw CloudAccountStoreError.invalidRecord }
        guard data.count <= Self.maximumStoreBytes else { throw CloudAccountStoreError.limitExceeded }
        do { try secretStore.set(data, for: Self.storageKey) }
        catch { throw Self.storageFailure(error) }
    }

    private static func storageFailure(_ error: any Error) -> CloudAccountStoreError {
        if let keychainError = error as? KeychainError,
           case .operationFailed(let status) = keychainError,
           status == errSecMissingEntitlement {
            return .keychainAccessNotAuthorized
        }
        return .storageUnavailable
    }

    private static func isValid(_ entry: Entry) -> Bool {
        let account = entry.account
        return account.credentialGeneration.rawValue > 0
            && account.localLabel.utf8.count <= 256
            && account.localLabel == account.localLabel.trimmingCharacters(in: .whitespacesAndNewlines)
            && account.localLabel.unicodeScalars.allSatisfy { !CharacterSet.controlCharacters.contains($0) }
            && (0...25_000).contains(account.discoverableClusterCount)
            && (account.lastSuccessfulSync?.timeIntervalSinceReferenceDate.isFinite ?? true)
            && (entry.expiresAt?.timeIntervalSinceReferenceDate.isFinite ?? true)
            && !entry.credentialData.isEmpty
            && entry.credentialData.count <= maximumCredentialBytes
    }
}
