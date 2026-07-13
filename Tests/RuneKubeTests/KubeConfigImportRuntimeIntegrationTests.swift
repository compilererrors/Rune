import Foundation
import XCTest
@testable import RuneCore
@testable import RuneFakeK8sSupport
@testable import RuneKube
@testable import RuneSecurity

final class KubeConfigImportRuntimeIntegrationTests: XCTestCase {
    func testImportedRelativeTokenFileSurvivesSourceRemovalAndReachesFakeCluster() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }

        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("KubeConfigImportRuntimeIntegrationTests.\(UUID().uuidString)", isDirectory: true)
        let sourceDirectory = root.appendingPathComponent("source", isDirectory: true)
        let credentialsDirectory = sourceDirectory.appendingPathComponent("credentials", isDirectory: true)
        let imports = root.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: credentialsDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        try "synthetic-token".write(
            to: credentialsDirectory.appendingPathComponent("token"),
            atomically: true,
            encoding: .utf8
        )
        let source = sourceDirectory.appendingPathComponent("config.yaml")
        let raw = server.kubeconfigYAML().replacingOccurrences(
            of: "token: fake-token",
            with: "tokenFile: credentials/token"
        )
        try raw.write(to: source, atomically: true, encoding: .utf8)

        let imported = try AppOwnedKubeConfigImportStore(rootDirectory: imports).saveImportedKubeConfig(
            raw: raw,
            sourceName: source.lastPathComponent,
            sourceURL: source
        )
        try FileManager.default.removeItem(at: sourceDirectory)

        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        let namespaces = try await KubernetesClient(commandTimeout: 2).listNamespaces(
            from: [KubeConfigSource(url: imported)],
            context: context
        )

        XCTAssertEqual(namespaces, ["alpha-zone", "bravo-zone"])
        XCTAssertTrue(server.requestLines().contains { $0.contains("/api/v1/namespaces") })
        XCTAssertTrue(FileManager.default.fileExists(
            atPath: imported.deletingLastPathComponent()
                .appendingPathComponent("assets/000-token")
                .path
        ))
    }

    func testImportedAbsoluteTokenFileSurvivesExternalFileRemovalAndReachesFakeCluster() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }

        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("KubeConfigImportRuntimeIntegrationTests.absolute.\(UUID().uuidString)", isDirectory: true)
        let sourceDirectory = root.appendingPathComponent("source", isDirectory: true)
        let externalDirectory = root.appendingPathComponent("external", isDirectory: true)
        let imports = root.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: sourceDirectory, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: externalDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let externalToken = externalDirectory.appendingPathComponent("token")
        try "synthetic-token".write(to: externalToken, atomically: true, encoding: .utf8)
        let source = sourceDirectory.appendingPathComponent("config.yaml")
        let raw = server.kubeconfigYAML().replacingOccurrences(
            of: "token: fake-token",
            with: "tokenFile: \(externalToken.path)"
        )
        try raw.write(to: source, atomically: true, encoding: .utf8)

        let imported = try AppOwnedKubeConfigImportStore(rootDirectory: imports).saveImportedKubeConfig(
            raw: raw,
            sourceName: source.lastPathComponent,
            sourceURL: source
        )
        try FileManager.default.removeItem(at: sourceDirectory)
        try FileManager.default.removeItem(at: externalDirectory)

        let namespaces = try await KubernetesClient(commandTimeout: 2).listNamespaces(
            from: [KubeConfigSource(url: imported)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        )

        XCTAssertEqual(namespaces, ["alpha-zone", "bravo-zone"])
        XCTAssertTrue(server.requestLines().contains { $0.contains("/api/v1/namespaces") })
        XCTAssertEqual(
            try String(
                contentsOf: imported.deletingLastPathComponent().appendingPathComponent("assets/000-token"),
                encoding: .utf8
            ),
            "synthetic-token"
        )
    }
}
