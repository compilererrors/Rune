import Foundation
import XCTest
@testable import RuneSecurity

final class KubeConfigImportPreparationTests: XCTestCase {
    private let raw = """
    clusters:
    - name: synthetic-cluster
      cluster: {server: https://cluster.example.invalid, certificate-authority: token}
    users:
    - name: synthetic-user
      user:
        tokenFile: token
        exec: {command: ./auth-plugin}
    """

    func testRepeatedBatchDocumentUsesEachSourceDirectoryAndPreservesDistinctOwnership() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let sources = ["first", "second"].map { root.appendingPathComponent($0) }
        for (index, source) in sources.enumerated() {
            try FileManager.default.createDirectory(at: source, withIntermediateDirectories: true)
            try Data("synthetic-token-\(index)".utf8).write(to: source.appendingPathComponent("token"))
        }
        let store = AppOwnedKubeConfigImportStore(rootDirectory: root.appendingPathComponent("imports"))
        let receipt = try store.publishImportedKubeConfigs(sources.map {
            KubeConfigImportStorePayload(raw: raw, sourceName: "config.yaml", sourceURL: $0.appendingPathComponent("config.yaml"))
        }, reusing: [])
        XCTAssertEqual(receipt.createdURLs.count, 2)
        XCTAssertEqual(receipt.reusedCount, 0)
        var digests: [String] = []
        for (index, config) in receipt.urls.enumerated() {
            let text = try String(contentsOf: config, encoding: .utf8)
            XCTAssertTrue(text.contains(sources[index].appendingPathComponent("auth-plugin").standardizedFileURL.path))
            XCTAssertFalse(text.contains(sources[1 - index].appendingPathComponent("auth-plugin").standardizedFileURL.path))
            let assets = config.deletingLastPathComponent().appendingPathComponent("assets")
            XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: assets.path).count, 1)
            XCTAssertEqual(try Data(contentsOf: assets.appendingPathComponent("000-certificate-authority.pem")), Data("synthetic-token-\(index)".utf8))
            digests.append(try store.record(forImportedKubeConfigAt: config).contentDigest)
        }
        XCTAssertNotEqual(digests[0], digests[1])
    }

    func testPreparedDocumentRereadsChangedReferencesAndRejectsLaterMissingFile() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let materializer = KubeConfigImportMaterializer()
        let document = try materializer.parse(raw)
        let token = root.appendingPathComponent("token")
        for value in ["first", "rotated", "missing"] {
            let destination = root.appendingPathComponent(value)
            try FileManager.default.createDirectory(at: destination, withIntermediateDirectories: true)
            if value == "missing" {
                try FileManager.default.removeItem(at: token)
                XCTAssertThrowsError(try materializer.materialize(document: document, sourceURL: root.appendingPathComponent("config.yaml"), importDirectory: destination))
            } else {
                let data = Data("synthetic-\(value)".utf8)
                try data.write(to: token)
                _ = try materializer.materialize(document: document, sourceURL: root.appendingPathComponent("config.yaml"), importDirectory: destination)
                XCTAssertEqual(try Data(contentsOf: destination.appendingPathComponent("assets/000-certificate-authority.pem")), data)
            }
        }
        XCTAssertEqual(try Data(contentsOf: root.appendingPathComponent("first/assets/000-certificate-authority.pem")), Data("synthetic-first".utf8))
    }
}
