import Foundation
import XCTest
@testable import RuneSecurity

final class KubeConfigImportPerformanceTests: XCTestCase {
    func testMaterializingTwentyExpandedKubeconfigsBenchmarkKPI() throws {
        let root = FileManager.default.temporaryDirectory
            .appendingPathComponent("KubeConfigImportPerformanceTests.\(UUID().uuidString)", isDirectory: true)
        let sourceDirectory = root.appendingPathComponent("source", isDirectory: true)
        let imports = root.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: sourceDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        for (name, value) in [
            ("ca.pem", "synthetic-ca"),
            ("client.pem", "synthetic-client"),
            ("client-key.pem", "synthetic-key"),
            ("token", "synthetic-token")
        ] {
            try value.write(
                to: sourceDirectory.appendingPathComponent(name),
                atomically: true,
                encoding: .utf8
            )
        }

        let clusters = (0..<40).map { index in
            """
            - name: synthetic-cluster-\(index)
              cluster:
                server: https://cluster-\(index).example.invalid
                certificate-authority: ca.pem
            """
        }.joined(separator: "\n")
        let users = (0..<40).map { index in
            """
            - name: synthetic-user-\(index)
              user:
                tokenFile: token
                client-certificate: client.pem
                client-key: client-key.pem
                exec:
                  apiVersion: client.authentication.k8s.io/v1
                  command: ./tools/auth-plugin
            """
        }.joined(separator: "\n")
        let raw = """
        apiVersion: v1
        kind: Config
        clusters:
        \(clusters)
        users:
        \(users)
        contexts: []
        """
        let source = sourceDirectory.appendingPathComponent("config.yaml")
        let store = AppOwnedKubeConfigImportStore(rootDirectory: imports)

        let start = ContinuousClock.now
        var imported: [URL] = []
        for index in 0..<20 {
            imported.append(try store.saveImportedKubeConfig(
                raw: raw,
                sourceName: "synthetic-\(index).yaml",
                sourceURL: source
            ))
        }
        let elapsed = ContinuousClock.now - start
        let seconds = Double(elapsed.components.seconds)
            + Double(elapsed.components.attoseconds) / 1_000_000_000_000_000_000

        XCTAssertEqual(imported.count, 20)
        XCTAssertTrue(imported.allSatisfy { FileManager.default.fileExists(atPath: $0.path) })
        #if DEBUG
        let maximumSeconds = 1.2
        #else
        let maximumSeconds = 0.6
        #endif
        XCTAssertLessThan(
            seconds,
            maximumSeconds,
            "KPI: materializing 20 kubeconfigs with 40 cluster/user entries and four credential files should stay below 1.2s in debug and 600ms in release."
        )
    }
}
