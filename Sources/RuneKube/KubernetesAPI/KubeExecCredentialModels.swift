import Foundation

/// `kubectl` exec credential plugin stdout (`client.authentication.k8s.io`).
struct ExecCredentialResponseJSON: Decodable, Sendable {
    let status: Status?

    struct Status: Decodable, Sendable {
        let token: String?
        let expirationTimestamp: String?
    }
}
