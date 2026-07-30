import RuneStore
import Foundation
import XCTest

final class NamespaceListOrderingTests: XCTestCase {
    func testMergePreservesPreviousOrderThenAppendsSortedNew() {
        let merged = NamespaceListOrdering.merge(
            previousOrder: ["zoo", "apple", "middle"],
            apiNames: ["new1", "middle", "apple", "zoo", "aaa"]
        )
        XCTAssertEqual(merged, ["zoo", "apple", "middle", "aaa", "new1"])
    }

    func testMergeEmptyPreviousUsesSortedApiOnly() {
        let merged = NamespaceListOrdering.merge(previousOrder: [], apiNames: ["b", "a"])
        XCTAssertEqual(merged, ["a", "b"])
    }

    func testJSONStoreKeepsCollidingAndFilesystemEquivalentContextNamesIsolated() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneNamespaceListPersistenceTests.\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = JSONNamespaceListPersistenceStore(directoryURL: directory)
        let decomposed = "synthetic-cafe\u{301}"
        let longContext = "synthetic-" + String(repeating: "🚀", count: 180)
        let fixtures = [
            ("synthetic/team", ["slash-only"]),
            ("synthetic:team", ["colon-only"]),
            ("synthetic_team", ["underscore-only"]),
            ("Synthetic-Team", ["case-upper-only"]),
            ("synthetic-team", ["case-lower-only"]),
            ("synthetic-café", ["unicode-composed-only"]),
            (decomposed, ["unicode-decomposed-only"]),
            (longContext, ["long-name-only"])
        ]

        for (contextName, namespaces) in fixtures {
            store.save(
                names: namespaces,
                contextName: contextName,
                scopeIdentity: "synthetic-scope"
            )
        }

        for (contextName, namespaces) in fixtures {
            XCTAssertEqual(
                store.load(contextName: contextName, scopeIdentity: "synthetic-scope"),
                namespaces,
                "Cache ownership must remain exact for \(contextName.debugDescription)."
            )
        }
        XCTAssertEqual(
            try FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil).count,
            fixtures.count
        )
    }

    func testJSONStoreRejectsDifferentSourceScope() {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneNamespaceListPersistenceTests.\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = JSONNamespaceListPersistenceStore(directoryURL: directory)

        store.save(
            names: ["old-scope-only"],
            contextName: "synthetic-context",
            scopeIdentity: "source-revision-a"
        )

        XCTAssertNil(
            store.load(
                contextName: "synthetic-context",
                scopeIdentity: "source-revision-b"
            )
        )
    }

    func testJSONStoreRejectsWrongOwnerUnknownSchemaAndLegacyPayload() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneNamespaceListPersistenceTests.\(UUID().uuidString)", isDirectory: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = JSONNamespaceListPersistenceStore(directoryURL: directory)
        store.save(
            names: ["verified"],
            contextName: "synthetic-context",
            scopeIdentity: "synthetic-scope"
        )
        let fileURL = try XCTUnwrap(
            FileManager.default
                .contentsOfDirectory(at: directory, includingPropertiesForKeys: nil)
                .first
        )

        let wrongOwner = """
        {"schemaVersion":2,"contextName":"other-context","scopeIdentity":"synthetic-scope","namespaces":["wrong-owner"]}
        """
        try Data(wrongOwner.utf8).write(to: fileURL, options: .atomic)
        XCTAssertNil(
            store.load(contextName: "synthetic-context", scopeIdentity: "synthetic-scope")
        )

        let unknownSchema = """
        {"schemaVersion":999,"contextName":"synthetic-context","scopeIdentity":"synthetic-scope","namespaces":["unknown-schema"]}
        """
        try Data(unknownSchema.utf8).write(to: fileURL, options: .atomic)
        XCTAssertNil(
            store.load(contextName: "synthetic-context", scopeIdentity: "synthetic-scope")
        )

        let legacyPayload = """
        {"schemaVersion":1,"namespaces":["legacy-unowned"]}
        """
        try Data(legacyPayload.utf8).write(to: fileURL, options: .atomic)
        XCTAssertNil(
            store.load(contextName: "synthetic-context", scopeIdentity: "synthetic-scope")
        )
    }
}
