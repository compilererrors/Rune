import Foundation
import Security

/// Builds a `SecIdentity` for client TLS from PEM cert + private key (kubeconfig `client-certificate-data` / `client-key-data`).
enum KubeTLSIdentity {
    /// Returns `nil` if the platform cannot form an identity (caller falls back to kubectl).
    static func makeIdentity(certPEM: Data, keyPEM: Data) -> SecIdentity? {
        guard let cert = importCertificate(pem: certPEM) else { return nil }
        guard let key = importPrivateKey(pem: keyPEM) else { return nil }

        let label = "com.rune.k8s.tls.\(UUID().uuidString)"
        let certQuery: [String: Any] = [
            kSecClass as String: kSecClassCertificate,
            kSecValueRef as String: cert,
            kSecAttrLabel as String: label,
            kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
        ]
        SecItemDelete(certQuery as CFDictionary)

        let keyQuery: [String: Any] = [
            kSecClass as String: kSecClassKey,
            kSecValueRef as String: key,
            kSecAttrLabel as String: label,
            kSecAttrAccessible as String: kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
        ]
        SecItemDelete(keyQuery as CFDictionary)

        guard SecItemAdd(certQuery as CFDictionary, nil) == errSecSuccess else { return nil }
        guard SecItemAdd(keyQuery as CFDictionary, nil) == errSecSuccess else {
            SecItemDelete(certQuery as CFDictionary)
            return nil
        }

        let identityQuery: [String: Any] = [
            kSecClass as String: kSecClassIdentity,
            kSecAttrLabel as String: label,
            kSecReturnRef as String: true
        ]
        var out: CFTypeRef?
        let status = SecItemCopyMatching(identityQuery as CFDictionary, &out)
        SecItemDelete(certQuery as CFDictionary)
        SecItemDelete(keyQuery as CFDictionary)
        guard status == errSecSuccess, let id = out else { return nil }
        return (id as! SecIdentity)
    }

    private static func importCertificate(pem: Data) -> SecCertificate? {
        var format = SecExternalFormat.formatPEMSequence
        var itemType = SecExternalItemType.itemTypeCertificate
        var items: CFArray?
        let st = SecItemImport(
            pem as CFData,
            nil,
            &format,
            &itemType,
            SecItemImportExportFlags(rawValue: 0),
            nil,
            nil,
            &items
        )
        guard st == errSecSuccess, let arr = items as? [Any] else { return nil }
        for case let c as SecCertificate in arr {
            return c
        }
        return nil
    }

    private static func importPrivateKey(pem: Data) -> SecKey? {
        var format = SecExternalFormat.formatPEMSequence
        var itemType = SecExternalItemType.itemTypePrivateKey
        var items: CFArray?
        let st = SecItemImport(
            pem as CFData,
            nil,
            &format,
            &itemType,
            SecItemImportExportFlags(rawValue: 0),
            nil,
            nil,
            &items
        )
        guard st == errSecSuccess, let arr = items as? [Any] else { return nil }
        for case let k as SecKey in arr {
            return k
        }
        return nil
    }
}
