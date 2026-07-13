import Foundation
import XCTest
@testable import RuneSecurity

final class KubeConfigImportStoreTests: XCTestCase {
    func testFileImportMaterializesRelativeDataReferencesAndAbsolutizesRelativeExecWithoutCopyingIt() throws {
        let root = temporaryDirectory("materialize")
        let sourceDirectory = root.appendingPathComponent("source", isDirectory: true)
        let materials = sourceDirectory.appendingPathComponent("materials", isDirectory: true)
        let tools = sourceDirectory.appendingPathComponent("tools", isDirectory: true)
        let imports = root.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: materials, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: tools, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        let contents: [(String, String)] = [
            ("ca.pem", "synthetic-ca"),
            ("client.pem", "synthetic-client-certificate"),
            ("client-key.pem", "synthetic-client-key"),
            ("token", "synthetic-token")
        ]
        for (name, value) in contents {
            try value.write(
                to: materials.appendingPathComponent(name),
                atomically: true,
                encoding: .utf8
            )
        }
        let executable = tools.appendingPathComponent("auth-plugin")
        try "synthetic executable placeholder".write(to: executable, atomically: true, encoding: .utf8)

        let source = sourceDirectory.appendingPathComponent("config.yaml")
        let raw = """
        apiVersion: v1
        kind: Config
        current-context: synthetic-context
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://example.invalid
            certificate-authority: materials/ca.pem # preserve-ca-comment
        contexts:
        - name: synthetic-context
          context:
            cluster: synthetic-cluster
            user: synthetic-user
        users:
        - name: synthetic-user
          user:
            client-certificate: "materials/client.pem"
            client-key: 'materials/client-key.pem'
            tokenFile: materials/token
            exec:
              apiVersion: client.authentication.k8s.io/v1
              command: ./tools/auth-plugin # preserve-exec-comment
        """
        try raw.write(to: source, atomically: true, encoding: .utf8)

        let imported = try AppOwnedKubeConfigImportStore(rootDirectory: imports).saveImportedKubeConfig(
            raw: raw,
            sourceName: source.lastPathComponent,
            sourceURL: source
        )
        let saved = try String(contentsOf: imported, encoding: .utf8)
        let importDirectory = imported.deletingLastPathComponent()
        let assetsDirectory = importDirectory.appendingPathComponent("assets", isDirectory: true)
        let assets = try FileManager.default.contentsOfDirectory(
            at: assetsDirectory,
            includingPropertiesForKeys: nil
        ).sorted { $0.lastPathComponent < $1.lastPathComponent }

        XCTAssertTrue(saved.contains("assets/000-certificate-authority.pem"))
        XCTAssertTrue(saved.contains("assets/001-client-certificate.pem"))
        XCTAssertTrue(saved.contains("assets/002-client-key.pem"))
        XCTAssertTrue(saved.contains("assets/003-token"))
        XCTAssertTrue(saved.contains(executable.standardizedFileURL.path))
        XCTAssertEqual(assets.map(\.lastPathComponent), [
            "000-certificate-authority.pem",
            "001-client-certificate.pem",
            "002-client-key.pem",
            "003-token"
        ])
        XCTAssertFalse(assets.contains { $0.lastPathComponent.contains("exec") || $0.lastPathComponent.contains("plugin") })
        XCTAssertEqual(try String(contentsOf: assets[0], encoding: .utf8), "synthetic-ca")
        XCTAssertEqual(try String(contentsOf: assets[1], encoding: .utf8), "synthetic-client-certificate")
        XCTAssertEqual(try String(contentsOf: assets[2], encoding: .utf8), "synthetic-client-key")
        XCTAssertEqual(try String(contentsOf: assets[3], encoding: .utf8), "synthetic-token")
        XCTAssertEqual(try permissions(of: imports), 0o700)
        XCTAssertEqual(try permissions(of: importDirectory), 0o700)
        XCTAssertEqual(try permissions(of: assetsDirectory), 0o700)
        XCTAssertEqual(try permissions(of: imported), 0o600)
        for asset in assets {
            XCTAssertEqual(try permissions(of: asset), 0o600)
        }
    }

    func testRepeatedRelativeReferenceIsCopiedOnceAndReusesMaterializedPath() throws {
        let root = temporaryDirectory("deduplicate")
        let sourceDirectory = root.appendingPathComponent("source", isDirectory: true)
        let imports = root.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: sourceDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }

        try "synthetic-ca".write(
            to: sourceDirectory.appendingPathComponent("ca.pem"),
            atomically: true,
            encoding: .utf8
        )
        let source = sourceDirectory.appendingPathComponent("config.yaml")
        let raw = """
        clusters:
        - name: synthetic-a
          cluster:
            server: https://a.example.invalid
            certificate-authority: ca.pem
        - name: synthetic-b
          cluster:
            server: https://b.example.invalid
            certificate-authority: ./ca.pem
        """

        let imported = try AppOwnedKubeConfigImportStore(rootDirectory: imports).saveImportedKubeConfig(
            raw: raw,
            sourceName: "config.yaml",
            sourceURL: source
        )
        let saved = try String(contentsOf: imported, encoding: .utf8)
        let assets = try FileManager.default.contentsOfDirectory(
            at: imported.deletingLastPathComponent().appendingPathComponent("assets", isDirectory: true),
            includingPropertiesForKeys: nil
        )

        XCTAssertEqual(assets.count, 1)
        XCTAssertEqual(saved.components(separatedBy: "assets/000-certificate-authority.pem").count - 1, 2)
    }

    func testAnchorsMergeKeysAndInlineMappingsAreResolvedBeforeMaterialization() throws {
        let root = temporaryDirectory("yaml-features")
        let sourceDirectory = root.appendingPathComponent("source", isDirectory: true)
        let imports = root.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: sourceDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        try "synthetic-ca".write(
            to: sourceDirectory.appendingPathComponent("ca.pem"),
            atomically: true,
            encoding: .utf8
        )
        try "synthetic-token".write(
            to: sourceDirectory.appendingPathComponent("token"),
            atomically: true,
            encoding: .utf8
        )
        let source = sourceDirectory.appendingPathComponent("config.yaml")
        let raw = """
        apiVersion: v1
        kind: Config
        cluster-defaults: &cluster-defaults {server: https://example.invalid, certificate-authority: ca.pem}
        user-defaults: &user-defaults
          tokenFile: token
          exec: {apiVersion: client.authentication.k8s.io/v1, command: auth-plugin}
        clusters:
        - name: synthetic-cluster
          cluster:
            <<: *cluster-defaults
        users:
        - name: synthetic-user
          user: {<<: *user-defaults}
        extensions:
        - name: synthetic-extension
          extension: {enabled: true, mode: synthetic}
        """

        let imported = try AppOwnedKubeConfigImportStore(rootDirectory: imports).saveImportedKubeConfig(
            raw: raw,
            sourceName: "config.yaml",
            sourceURL: source
        )
        let saved = try String(contentsOf: imported, encoding: .utf8)
        let assets = try FileManager.default.contentsOfDirectory(
            at: imported.deletingLastPathComponent().appendingPathComponent("assets", isDirectory: true),
            includingPropertiesForKeys: nil
        )

        XCTAssertEqual(Set(assets.map(\.lastPathComponent)), ["000-certificate-authority.pem", "001-token"])
        XCTAssertTrue(saved.contains("assets/000-certificate-authority.pem"))
        XCTAssertTrue(saved.contains("assets/001-token"))
        XCTAssertTrue(saved.contains("synthetic-extension"))
        XCTAssertTrue(saved.contains("synthetic-extension") && saved.contains("synthetic"))
    }

    func testPasteWithRelativeDataReferenceFailsWithoutLeavingPartialImport() throws {
        let root = temporaryDirectory("paste-relative")
        defer { try? FileManager.default.removeItem(at: root) }
        let store = AppOwnedKubeConfigImportStore(rootDirectory: root)

        XCTAssertThrowsError(try store.saveImportedKubeConfig(
            raw:
            """
            clusters:
            - name: synthetic-cluster
              cluster:
                server: https://example.invalid
                certificate-authority: relative/ca.pem
            """,
            sourceName: "pasted.yaml"
        )) { error in
            XCTAssertEqual(
                error as? KubeConfigImportMaterializationError,
                .sourceLocationRequired(reference: "certificate-authority")
            )
            XCTAssertTrue(error.localizedDescription.contains("Import the kubeconfig file"))
        }
        XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: root.path), [])
    }

    func testPasteWithSlashRelativeExecFailsButBarePathCommandRemainsPortable() throws {
        let root = temporaryDirectory("paste-exec")
        defer { try? FileManager.default.removeItem(at: root) }
        let store = AppOwnedKubeConfigImportStore(rootDirectory: root)
        let relativeExec = """
        users:
        - name: synthetic-user
          user:
            exec:
              command: ./auth-plugin
        """

        XCTAssertThrowsError(try store.saveImportedKubeConfig(raw: relativeExec, sourceName: "pasted.yaml")) { error in
            XCTAssertEqual(
                error as? KubeConfigImportMaterializationError,
                .sourceLocationRequired(reference: "exec command")
            )
        }

        let portableExec = relativeExec.replacingOccurrences(of: "./auth-plugin", with: "auth-plugin")
        let imported = try store.saveImportedKubeConfig(raw: portableExec, sourceName: "pasted.yaml")
        XCTAssertEqual(try String(contentsOf: imported, encoding: .utf8), portableExec)
        XCTAssertFalse(FileManager.default.fileExists(
            atPath: imported.deletingLastPathComponent().appendingPathComponent("assets").path
        ))
    }

    func testMissingReferenceFailsWithoutLeakingSourcePathOrLeavingPartialImport() throws {
        let root = temporaryDirectory("missing")
        let sourceDirectory = root.appendingPathComponent("source", isDirectory: true)
        let imports = root.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: sourceDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let source = sourceDirectory.appendingPathComponent("config.yaml")
        let raw = """
        users:
        - name: synthetic-user
          user:
            tokenFile: missing-token
        """

        XCTAssertThrowsError(try AppOwnedKubeConfigImportStore(rootDirectory: imports).saveImportedKubeConfig(
            raw: raw,
            sourceName: source.lastPathComponent,
            sourceURL: source
        )) { error in
            XCTAssertEqual(
                error as? KubeConfigImportMaterializationError,
                .missingReference(reference: "token file")
            )
            XCTAssertFalse(error.localizedDescription.contains(root.path))
            XCTAssertFalse(error.localizedDescription.contains("missing-token"))
        }
        XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: imports.path), [])
    }

    func testAbsoluteDataReferencesAreMaterializedWhileAbsoluteExecRemainsExternal() throws {
        let root = temporaryDirectory("absolute")
        let external = root.appendingPathComponent("external", isDirectory: true)
        let imports = root.appendingPathComponent("imports", isDirectory: true)
        try FileManager.default.createDirectory(at: external, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let ca = external.appendingPathComponent("ca.pem")
        let clientCertificate = external.appendingPathComponent("client.pem")
        let clientKey = external.appendingPathComponent("client-key.pem")
        let token = external.appendingPathComponent("token")
        try "synthetic-ca".write(to: ca, atomically: true, encoding: .utf8)
        try "synthetic-client".write(to: clientCertificate, atomically: true, encoding: .utf8)
        try "synthetic-key".write(to: clientKey, atomically: true, encoding: .utf8)
        try "synthetic-token".write(to: token, atomically: true, encoding: .utf8)
        let raw = """
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://example.invalid
            certificate-authority: \(ca.path)
        users:
        - name: synthetic-user
          user:
            client-certificate: \(clientCertificate.path)
            client-key: \(clientKey.path)
            tokenFile: \(token.path)
            exec:
              command: /synthetic/absolute/auth-plugin
        """

        let imported = try AppOwnedKubeConfigImportStore(rootDirectory: imports).saveImportedKubeConfig(
            raw: raw,
            sourceName: "absolute.yaml"
        )

        let saved = try String(contentsOf: imported, encoding: .utf8)
        let assetsDirectory = imported.deletingLastPathComponent().appendingPathComponent("assets", isDirectory: true)
        let assets = try FileManager.default.contentsOfDirectory(
            at: assetsDirectory,
            includingPropertiesForKeys: nil
        ).sorted { $0.lastPathComponent < $1.lastPathComponent }
        XCTAssertEqual(assets.map(\.lastPathComponent), [
            "000-certificate-authority.pem",
            "001-client-certificate.pem",
            "002-client-key.pem",
            "003-token"
        ])
        XCTAssertTrue(saved.contains("assets/000-certificate-authority.pem"))
        XCTAssertTrue(saved.contains("assets/001-client-certificate.pem"))
        XCTAssertTrue(saved.contains("assets/002-client-key.pem"))
        XCTAssertTrue(saved.contains("assets/003-token"))
        XCTAssertTrue(saved.contains("/synthetic/absolute/auth-plugin"))
        XCTAssertFalse(saved.contains(ca.path))
        XCTAssertFalse(saved.contains(clientCertificate.path))
        XCTAssertFalse(saved.contains(clientKey.path))
        XCTAssertFalse(saved.contains(token.path))

        try FileManager.default.removeItem(at: external)
        XCTAssertEqual(try String(contentsOf: assets[0], encoding: .utf8), "synthetic-ca")
        XCTAssertEqual(try String(contentsOf: assets[1], encoding: .utf8), "synthetic-client")
        XCTAssertEqual(try String(contentsOf: assets[2], encoding: .utf8), "synthetic-key")
        XCTAssertEqual(try String(contentsOf: assets[3], encoding: .utf8), "synthetic-token")
    }

    func testUnavailableAbsoluteDataReferenceRollsBackWithSanitizedActionableError() throws {
        let root = temporaryDirectory("absolute-unavailable")
        let imports = root.appendingPathComponent("imports", isDirectory: true)
        let unavailable = root.appendingPathComponent("outside/missing-token")
        defer { try? FileManager.default.removeItem(at: root) }
        let raw = """
        users:
        - name: synthetic-user
          user:
            tokenFile: \(unavailable.path)
        """

        XCTAssertThrowsError(try AppOwnedKubeConfigImportStore(rootDirectory: imports).saveImportedKubeConfig(
            raw: raw,
            sourceName: "config.yaml"
        )) { error in
            XCTAssertEqual(
                error as? KubeConfigImportMaterializationError,
                .missingReference(reference: "token file")
            )
            XCTAssertTrue(error.localizedDescription.contains("import a folder"))
            XCTAssertFalse(error.localizedDescription.contains(root.path))
            XCTAssertFalse(error.localizedDescription.contains("missing-token"))
        }
        XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: imports.path), [])
    }

    func testTildeDataReferenceIsMaterializedFromHomeDirectory() throws {
        let root = temporaryDirectory("tilde")
        let syntheticHome = root.appendingPathComponent("home", isDirectory: true)
        let credentials = syntheticHome.appendingPathComponent("credentials", isDirectory: true)
        let importDirectory = root.appendingPathComponent("import", isDirectory: true)
        try FileManager.default.createDirectory(at: credentials, withIntermediateDirectories: true)
        try FileManager.default.createDirectory(at: importDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        try "synthetic-token".write(
            to: credentials.appendingPathComponent("token"),
            atomically: true,
            encoding: .utf8
        )
        let raw = """
        users:
        - name: synthetic-user
          user:
            tokenFile: ~/credentials/token
            exec:
              command: ~/bin/auth-plugin
        """

        let saved = try KubeConfigImportMaterializer(
            homeDirectoryProvider: { syntheticHome }
        ).materialize(raw: raw, sourceURL: nil, importDirectory: importDirectory)

        XCTAssertTrue(saved.contains("assets/000-token"))
        XCTAssertTrue(saved.contains("~/bin/auth-plugin"))
        XCTAssertEqual(
            try String(contentsOf: importDirectory.appendingPathComponent("assets/000-token"), encoding: .utf8),
            "synthetic-token"
        )
        XCTAssertEqual(
            try FileManager.default.contentsOfDirectory(
                at: importDirectory.appendingPathComponent("assets", isDirectory: true),
                includingPropertiesForKeys: nil
            ).count,
            1
        )
    }

    func testMultipleDocumentsAreRejectedWithoutLeavingPartialImport() throws {
        let root = temporaryDirectory("multiple-documents")
        defer { try? FileManager.default.removeItem(at: root) }

        XCTAssertThrowsError(try AppOwnedKubeConfigImportStore(rootDirectory: root).saveImportedKubeConfig(
            raw: "clusters: []\n---\nusers: []\n",
            sourceName: "config.yaml"
        )) { error in
            XCTAssertEqual(error as? KubeConfigImportMaterializationError, .multipleDocuments)
        }
        XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: root.path), [])
    }

    private func temporaryDirectory(_ label: String) -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("KubeConfigImportStoreTests.\(label).\(UUID().uuidString)", isDirectory: true)
    }

    private func permissions(of url: URL) throws -> Int {
        let attributes = try FileManager.default.attributesOfItem(atPath: url.path)
        return (attributes[.posixPermissions] as? NSNumber)?.intValue ?? -1
    }
}
