import Foundation

public struct NativeCloudClusterImportResult: Sendable, Equatable {
    public let provider: CloudKubeConfigProvider
    public let rawKubeConfig: String
    public let sourceName: String

    public init(
        provider: CloudKubeConfigProvider,
        rawKubeConfig: String,
        sourceName: String
    ) {
        self.provider = provider
        self.rawKubeConfig = rawKubeConfig
        self.sourceName = sourceName
    }
}

/// App Store-safe cloud discovery. Implementations call only provider HTTPS APIs;
/// credential material is supplied in memory and never written into kubeconfig.
public protocol NativeCloudClusterImporting: Sendable {
    func importAKS(
        _ request: AKSNativeClusterImportRequest,
        clientSecret: String
    ) async throws -> NativeCloudClusterImportResult

    func importEKS(
        _ request: AWSEKSClusterImportRequest,
        credentials: AWSEKSCredentials
    ) async throws -> NativeCloudClusterImportResult

    func importGKE(
        _ request: GKENativeClusterImportRequest,
        serviceAccountJSON: Data
    ) async throws -> NativeCloudClusterImportResult
}

public struct NativeCloudClusterImporter: NativeCloudClusterImporting, Sendable {
    private let aksImporter: AKSNativeClusterImporter
    private let eksImporter: AWSEKSClusterImporter
    private let gkeImporter: GKENativeClusterImporter

    public init(
        aksImporter: AKSNativeClusterImporter = AKSNativeClusterImporter(),
        eksImporter: AWSEKSClusterImporter = AWSEKSClusterImporter(),
        gkeImporter: GKENativeClusterImporter = GKENativeClusterImporter()
    ) {
        self.aksImporter = aksImporter
        self.eksImporter = eksImporter
        self.gkeImporter = gkeImporter
    }

    public func importAKS(
        _ request: AKSNativeClusterImportRequest,
        clientSecret: String
    ) async throws -> NativeCloudClusterImportResult {
        let result = try await aksImporter.importCluster(request, clientSecret: clientSecret)
        return NativeCloudClusterImportResult(
            provider: .aks,
            rawKubeConfig: result.rawKubeConfig,
            sourceName: result.sourceName
        )
    }

    public func importEKS(
        _ request: AWSEKSClusterImportRequest,
        credentials: AWSEKSCredentials
    ) async throws -> NativeCloudClusterImportResult {
        let result = try await eksImporter.importCluster(request, credentials: credentials)
        return NativeCloudClusterImportResult(
            provider: .eks,
            rawKubeConfig: result.rawKubeConfig,
            sourceName: result.sourceName
        )
    }

    public func importGKE(
        _ request: GKENativeClusterImportRequest,
        serviceAccountJSON: Data
    ) async throws -> NativeCloudClusterImportResult {
        let result = try await gkeImporter.importCluster(
            request,
            serviceAccountJSON: serviceAccountJSON
        )
        return NativeCloudClusterImportResult(
            provider: .gke,
            rawKubeConfig: result.rawKubeConfig,
            sourceName: result.sourceName
        )
    }
}
