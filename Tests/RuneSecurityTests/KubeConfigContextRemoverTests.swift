import XCTest
import RuneCore
@testable import RuneSecurity

final class KubeConfigContextRemoverTests: XCTestCase {
    func testPreviewDoesNotModifyKubeConfig() throws {
        let fixture = try makeFixture(contents: kubeConfig)
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        let original = try Data(contentsOf: fixture.config)

        let preview = try remover(for: fixture.directory).previewRemoval(
            of: "context-one",
            from: [KubeConfigSource(url: fixture.config)]
        )

        XCTAssertEqual(preview.affectedSourceDisplayNames, ["config.yaml"])
        XCTAssertEqual(preview.removedClusterCount, 1)
        XCTAssertEqual(preview.removedUserCount, 0)
        XCTAssertEqual(try Data(contentsOf: fixture.config), original)
    }

    func testRemovalKeepsSharedEntriesAndUpdatesCurrentContext() throws {
        let fixture = try makeFixture(contents: kubeConfig)
        defer { try? FileManager.default.removeItem(at: fixture.directory) }

        let original = try Data(contentsOf: fixture.config)
        let preview = try remover(for: fixture.directory).removeContext(
            named: "context-one",
            from: [KubeConfigSource(url: fixture.config)]
        )
        let updated = try String(contentsOf: fixture.config, encoding: .utf8)
        let review = KubeConfigImportValidator().validate(raw: updated)

        XCTAssertEqual(preview.removedClusterCount, 1)
        XCTAssertEqual(preview.removedUserCount, 0, "The remaining context still uses the shared user.")
        XCTAssertTrue(review.isValid)
        XCTAssertEqual(review.contexts.map(\.name), ["context-two"])
        XCTAssertTrue(updated.contains("current-context: context-two"))
        XCTAssertFalse(updated.contains("name: cluster-one"))
        XCTAssertTrue(updated.contains("name: cluster-two"))
        XCTAssertTrue(updated.contains("name: shared-user"))
        let backup = try XCTUnwrap(backupFiles(in: fixture.directory).first)
        XCTAssertEqual(try Data(contentsOf: backup), original)
        let backupAttributes = try FileManager.default.attributesOfItem(atPath: backup.path)
        XCTAssertEqual(backupAttributes[.posixPermissions] as? NSNumber, NSNumber(value: 0o600))
    }

    func testRemovalDeletesUnusedUserAndPreservesFilePermissions() throws {
        let fixture = try makeFixture(contents: kubeConfig.replacingOccurrences(
            of: "user: shared-user\n    namespace: one",
            with: "user: user-one\n    namespace: one"
        ).replacingOccurrences(
            of: "users:\n- name: shared-user",
            with: "users:\n- name: user-one\n  user:\n    token: first-token\n- name: shared-user"
        ))
        defer { try? FileManager.default.removeItem(at: fixture.directory) }
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: fixture.config.path)

        let preview = try remover(for: fixture.directory).removeContext(
            named: "context-one",
            from: [KubeConfigSource(url: fixture.config)]
        )
        let updated = try String(contentsOf: fixture.config, encoding: .utf8)
        let attributes = try FileManager.default.attributesOfItem(atPath: fixture.config.path)

        XCTAssertEqual(preview.removedUserCount, 1)
        XCTAssertFalse(updated.contains("name: user-one"))
        XCTAssertEqual(attributes[.posixPermissions] as? NSNumber, NSNumber(value: 0o600))
    }

    func testRemovalFailsBeforeWritingWhenAnotherLoadedSourceIsMalformed() throws {
        let valid = try makeFixture(contents: kubeConfig)
        let malformedURL = valid.directory.appendingPathComponent("malformed.yaml")
        try "contexts: [".write(to: malformedURL, atomically: true, encoding: .utf8)
        defer { try? FileManager.default.removeItem(at: valid.directory) }
        let original = try Data(contentsOf: valid.config)

        XCTAssertThrowsError(
            try remover(for: valid.directory).removeContext(
                named: "context-one",
                from: [KubeConfigSource(url: valid.config), KubeConfigSource(url: malformedURL)]
            )
        )
        XCTAssertEqual(try Data(contentsOf: valid.config), original)
    }

    private func makeFixture(contents: String) throws -> (directory: URL, config: URL) {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("KubeConfigContextRemoverTests.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let config = directory.appendingPathComponent("config.yaml")
        try contents.write(to: config, atomically: true, encoding: .utf8)
        return (directory, config)
    }

    private func remover(for directory: URL) -> KubeConfigContextRemover {
        KubeConfigContextRemover(
            backupRootDirectory: directory.appendingPathComponent("backups", isDirectory: true)
        )
    }

    private func backupFiles(in directory: URL) throws -> [URL] {
        let root = directory.appendingPathComponent("backups", isDirectory: true)
        guard FileManager.default.fileExists(atPath: root.path) else { return [] }
        return try FileManager.default.contentsOfDirectory(
            at: root,
            includingPropertiesForKeys: [.isDirectoryKey]
        ).flatMap { transaction in
            try FileManager.default.contentsOfDirectory(at: transaction, includingPropertiesForKeys: nil)
        }
    }

    private var kubeConfig: String {
        """
        apiVersion: v1
        kind: Config
        current-context: context-one
        clusters:
        - name: cluster-one
          cluster:
            server: https://one.example.invalid
        - name: cluster-two
          cluster:
            server: https://two.example.invalid
        users:
        - name: shared-user
          user:
            token: synthetic-token
        contexts:
        - name: context-one
          context:
            cluster: cluster-one
            user: shared-user
            namespace: one
        - name: context-two
          context:
            cluster: cluster-two
            user: shared-user
            namespace: two
        """
    }
}
