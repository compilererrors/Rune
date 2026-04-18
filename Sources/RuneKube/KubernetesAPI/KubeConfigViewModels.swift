import Foundation

/// Subset of `kubectl config view -o json` needed for direct HTTPS to the API server.
struct KubeConfigViewJSON: Decodable {
    let clusters: [KubeConfigClusterEntry]
    let users: [KubeConfigUserEntry]
    let contexts: [KubeConfigContextEntry]
}

struct KubeConfigClusterEntry: Decodable {
    let name: String
    let cluster: ClusterUserCluster
}

struct KubeConfigUserEntry: Decodable {
    let name: String
    let user: ClusterUserUser
}

struct KubeConfigContextEntry: Decodable {
    let name: String
    let context: ContextDetail

    struct ContextDetail: Decodable {
        let cluster: String
        let user: String
    }
}

/// Cluster and user blobs share field names for cluster; user blob is separate type.
struct ClusterUserCluster: Decodable {
    let server: String
    let certificateAuthorityData: String?
    let certificateAuthority: String?
    let insecureSkipTLSVerify: Bool?

    enum CodingKeys: String, CodingKey {
        case server
        case certificateAuthorityData = "certificate-authority-data"
        case certificateAuthority = "certificate-authority"
        case insecureSkipTLSVerify = "insecure-skip-tls-verify"
    }
}

struct ClusterUserUser: Decodable {
    let token: String?
    let clientCertificateData: String?
    let clientKeyData: String?
    let clientCertificate: String?
    let clientKey: String?
    let exec: ExecPluginConfig?

    enum CodingKeys: String, CodingKey {
        case token
        case clientCertificateData = "client-certificate-data"
        case clientKeyData = "client-key-data"
        case clientCertificate = "client-certificate"
        case clientKey = "client-key"
        case exec
    }
}

/// `user.exec` from kubeconfig (credential plugin).
struct ExecPluginConfig: Decodable, Sendable {
    let apiVersion: String?
    let command: String
    let args: [String]?
    let env: [ExecEnvVar]?

    struct ExecEnvVar: Decodable, Sendable {
        let name: String
        let value: String
    }
}
