import Foundation
import XCTest
@testable import RuneSecurity

final class KubeConfigImportOwnershipTests: XCTestCase {
    func testRemovingOneEntryPreservesOtherEntriesInTheSameBatch() throws {
        try withStore { store, root in
            let urls = try store.saveImportedKubeConfigs([payload("first"), payload("second")])
            let secondRecord = try store.record(forImportedKubeConfigAt: urls[1])
            try store.removeImportedKubeConfigs(at: [urls[0]])
            try store.removeImportedKubeConfigs(at: [urls[0]])
            XCTAssertFalse(FileManager.default.fileExists(atPath: urls[0].path))
            XCTAssertEqual(try String(contentsOf: urls[1], encoding: .utf8), payload("second").raw)
            XCTAssertEqual(try store.record(forImportedKubeConfigAt: urls[1]), secondRecord)
            try store.removeImportedKubeConfigs(at: [urls[1]])
            XCTAssertTrue(try FileManager.default.contentsOfDirectory(atPath: root.appendingPathComponent("imports").path).isEmpty)
        }
    }

    func testRemovalValidatesEveryEntryBeforeDeletingAnyFiles() throws {
        try withStore { store, root in
            let urls = try store.saveImportedKubeConfigs([payload("first"), payload("second")])
            let external = root.appendingPathComponent("external.yaml")
            try "synthetic external data".write(to: external, atomically: true, encoding: .utf8)
            XCTAssertThrowsError(try store.removeImportedKubeConfigs(at: [urls[0], external]))
            XCTAssertTrue(urls.allSatisfy { FileManager.default.fileExists(atPath: $0.path) })
            XCTAssertEqual(try String(contentsOf: external, encoding: .utf8), "synthetic external data")
            for directory in [urls[0].deletingLastPathComponent(), urls[0].deletingLastPathComponent().deletingLastPathComponent()] {
                XCTAssertThrowsError(try store.removeImportedKubeConfigs(at: [directory]))
            }
            XCTAssertTrue(urls.allSatisfy { FileManager.default.fileExists(atPath: $0.path) })
        }
    }

    func testMissingCorruptAndOversizedOwnershipRecordsNeverAuthorizeDeletionOrReuse() throws {
        try withStore { store, _ in
            let original = try store.saveImportedKubeConfig(raw: payload("first").raw, sourceName: "config.yaml")
            let recordURL = original.deletingLastPathComponent().appendingPathComponent(KubeConfigImportContents.recordFilename)
            for corruption: Data? in [nil, Data("invalid synthetic record".utf8), Data(repeating: 32, count: KubeConfigImportContents.maximumRecordBytes + 1)] {
                if let corruption { try corruption.write(to: recordURL) }
                else { try FileManager.default.removeItem(at: recordURL) }
                XCTAssertThrowsError(try store.removeImportedKubeConfigs(at: [original]))
                XCTAssertTrue(FileManager.default.fileExists(atPath: original.path))
                let publication = try store.publishImportedKubeConfigs([payload("first")], reusing: [original])
                XCTAssertEqual(publication.reusedCount, 0)
                XCTAssertNotEqual(publication.urls.first, original)
                try store.removeImportedKubeConfigs(at: publication.createdURLs)
            }
        }
    }

    func testSymlinkedImportDirectoryCannotDeleteOrReuseItsExternalTarget() throws {
        try withStore { store, root in
            let original = try store.saveImportedKubeConfig(raw: payload("first").raw, sourceName: "config.yaml")
            let directory = original.deletingLastPathComponent()
            let relocated = root.appendingPathComponent("external-entry", isDirectory: true)
            try FileManager.default.moveItem(at: directory, to: relocated)
            try FileManager.default.createSymbolicLink(at: directory, withDestinationURL: relocated)
            XCTAssertThrowsError(try store.removeImportedKubeConfigs(at: [original]))
            let publication = try store.publishImportedKubeConfigs([payload("first")], reusing: [original])
            XCTAssertEqual(publication.reusedCount, 0)
            XCTAssertEqual(try String(contentsOf: relocated.appendingPathComponent(original.lastPathComponent), encoding: .utf8), payload("first").raw)
        }
    }

    func testSymlinkedConfigurationAndUnexpectedFilesPreventEntryDeletion() throws {
        try withStore { store, root in
            let urls = try store.saveImportedKubeConfigs([payload("first"), payload("second")])
            let extra = urls[0].deletingLastPathComponent().appendingPathComponent("user-note.txt")
            try "synthetic note".write(to: extra, atomically: true, encoding: .utf8)
            XCTAssertThrowsError(try store.removeImportedKubeConfigs(at: urls))
            XCTAssertTrue(urls.allSatisfy { FileManager.default.fileExists(atPath: $0.path) })
            try FileManager.default.removeItem(at: extra)
            let external = root.appendingPathComponent("external.yaml")
            try payload("first").raw.write(to: external, atomically: true, encoding: .utf8)
            try FileManager.default.removeItem(at: urls[0])
            try FileManager.default.createSymbolicLink(at: urls[0], withDestinationURL: external)
            XCTAssertThrowsError(try store.removeImportedKubeConfigs(at: urls))
            XCTAssertTrue(FileManager.default.fileExists(atPath: external.path))
            XCTAssertTrue(FileManager.default.fileExists(atPath: urls[1].path))
        }
    }

    func testModifiedConfigurationIsNeitherDeletedNorReused() throws {
        try withStore { store, _ in
            let original = try store.saveImportedKubeConfig(raw: payload("first").raw, sourceName: "config.yaml")
            try payload("changed").raw.write(to: original, atomically: true, encoding: .utf8)
            XCTAssertThrowsError(try store.removeImportedKubeConfigs(at: [original])) {
                XCTAssertEqual($0 as? KubeConfigImportOwnershipError, .changedContents)
            }
            let publication = try store.publishImportedKubeConfigs([payload("first")], reusing: [original])
            XCTAssertEqual(publication.reusedCount, 0)
            XCTAssertEqual(try String(contentsOf: original, encoding: .utf8), payload("changed").raw)
        }
    }

    func testExactReimportPreservesOpaqueIdentityAndRevisionAcrossStoreRestart() throws {
        try withStore { store, root in
            let original = try store.saveImportedKubeConfig(raw: payload("first").raw, sourceName: "first.yaml")
            let record = try store.record(forImportedKubeConfigAt: original)
            let restarted = AppOwnedKubeConfigImportStore(rootDirectory: root.appendingPathComponent("imports"))
            let publication = try restarted.publishImportedKubeConfigs([payload("first", filename: "another-name.yaml")], reusing: [original])
            XCTAssertEqual(publication.urls, [original])
            XCTAssertTrue(publication.createdURLs.isEmpty)
            XCTAssertEqual(publication.reusedCount, 1)
            XCTAssertEqual(try restarted.record(forImportedKubeConfigAt: original), record)
            let encoded = String(decoding: try JSONEncoder().encode(record), as: UTF8.self)
            XCTAssertFalse(encoded.contains("synthetic-first"))
            XCTAssertFalse(encoded.contains(root.path))
            XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: root.appendingPathComponent("imports").path).count, 1)
            let recordURL = original.deletingLastPathComponent().appendingPathComponent(KubeConfigImportContents.recordFilename)
            XCTAssertEqual((try FileManager.default.attributesOfItem(atPath: recordURL.path)[.posixPermissions] as? NSNumber)?.intValue, 0o600)
        }
    }

    func testIdenticalBatchEntriesReuseOnlyOneCreatedFileAndRollbackIsExact() throws {
        try withStore { store, _ in
            let publication = try store.publishImportedKubeConfigs([payload("first"), payload("first", filename: "copy.yaml")], reusing: [])
            XCTAssertEqual(publication.urls, publication.createdURLs)
            XCTAssertEqual(publication.createdURLs.count, 1)
            XCTAssertEqual(publication.reusedCount, 1)
            try store.removeImportedKubeConfigs(at: publication.createdURLs)
            XCTAssertFalse(FileManager.default.fileExists(atPath: publication.urls[0].path))
        }
    }

    func testMixedPublicationRollbackLeavesReusedImportAndItsRevisionUntouched() throws {
        try withStore { store, _ in
            let existing = try store.saveImportedKubeConfig(raw: payload("first").raw, sourceName: "first.yaml")
            let record = try store.record(forImportedKubeConfigAt: existing)
            let publication = try store.publishImportedKubeConfigs([payload("first"), payload("second")], reusing: [existing])
            XCTAssertEqual(publication.reusedCount, 1)
            XCTAssertEqual(publication.createdURLs.count, 1)
            XCTAssertEqual(publication.urls.first, existing)
            try store.removeImportedKubeConfigs(at: publication.createdURLs)
            XCTAssertEqual(try store.record(forImportedKubeConfigAt: existing), record)
            XCTAssertFalse(FileManager.default.fileExists(atPath: publication.urls[1].path))
        }
    }

    func testRepeatedCopyKeepsLastDefinitionPrecedenceForUpdateExisting() throws {
        try withStore { store, _ in
            let first = payload("first")
            let middle = payload("middle")
            let publication = try store.publishImportedKubeConfigs([first, middle, first], reusing: [])
            XCTAssertEqual(publication.createdURLs.count, 2)
            XCTAssertEqual(publication.reusedCount, 1)
            let newestFirst = try publication.urls.reversed().map { try String(contentsOf: $0, encoding: .utf8) }
            XCTAssertEqual(newestFirst, [first.raw, middle.raw])
        }
    }

    func testLateMaterializationFailureLeavesPreviouslyReusedFilesAndNoStagingArtifacts() throws {
        try withStore { store, root in
            let existing = try store.saveImportedKubeConfig(raw: payload("first").raw, sourceName: "first.yaml")
            let before = try FileManager.default.contentsOfDirectory(atPath: root.appendingPathComponent("imports").path)
            let invalid = KubeConfigImportStorePayload(raw: "users: [{name: synthetic, user: {tokenFile: missing-token}}]", sourceName: "broken.yaml", sourceURL: root.appendingPathComponent("source.yaml"))
            XCTAssertThrowsError(try store.publishImportedKubeConfigs([payload("first"), payload("second"), invalid], reusing: [existing]))
            XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: root.appendingPathComponent("imports").path), before)
            XCTAssertEqual(try String(contentsOf: existing, encoding: .utf8), payload("first").raw)
        }
    }

    func testAssetsParticipateInReuseIdentityAndChangedOwnedAssetsPreventDeletion() throws {
        try withStore { store, root in
            let asset = root.appendingPathComponent("token")
            try "synthetic-token-one".write(to: asset, atomically: true, encoding: .utf8)
            let input = KubeConfigImportStorePayload(raw: "users: [{name: synthetic, user: {tokenFile: token}}]", sourceName: "config.yaml", sourceURL: root.appendingPathComponent("source.yaml"))
            let original = try XCTUnwrap(store.publishImportedKubeConfigs([input], reusing: []).urls.first)
            XCTAssertEqual(try store.publishImportedKubeConfigs([input], reusing: [original]).reusedCount, 1)
            try "synthetic-token-two".write(to: asset, atomically: true, encoding: .utf8)
            let changed = try store.publishImportedKubeConfigs([input], reusing: [original])
            XCTAssertEqual(changed.reusedCount, 0)
            XCTAssertNotEqual(changed.urls.first, original)
            let ownedAsset = original.deletingLastPathComponent().appendingPathComponent("assets/000-token")
            try "synthetic-edited-owned-token".write(to: ownedAsset, atomically: true, encoding: .utf8)
            XCTAssertThrowsError(try store.removeImportedKubeConfigs(at: [original])) {
                XCTAssertEqual($0 as? KubeConfigImportOwnershipError, .changedContents)
            }
        }
    }

    func testExternalFilesAndDifferentAccountOriginsAreNotSilentlyMerged() throws {
        try withStore { store, root in
            let external = root.appendingPathComponent("external.yaml")
            try payload("first").raw.write(to: external, atomically: true, encoding: .utf8)
            let first = try store.publishImportedKubeConfigs([payload("first")], reusing: [external])
            XCTAssertEqual(first.reusedCount, 0)
            let account = CloudAccountID()
            let nativeOrigin = KubeConfigImportOrigin(source: .nativeAccount, provider: .azure, accountID: account)
            let native = KubeConfigImportStorePayload(raw: payload("first").raw, sourceName: "native.yaml", sourceURL: nil, origin: nativeOrigin)
            let second = try store.publishImportedKubeConfigs([native], reusing: first.urls)
            XCTAssertEqual(second.reusedCount, 0)
            XCTAssertEqual(try store.record(forImportedKubeConfigAt: XCTUnwrap(second.urls.first)).origin, nativeOrigin)
            let third = try store.publishImportedKubeConfigs([native], reusing: first.urls + second.urls)
            XCTAssertEqual(third.urls, second.urls)
            XCTAssertEqual(third.reusedCount, 1)
            XCTAssertTrue(FileManager.default.fileExists(atPath: external.path))
        }
    }

    func testNativeOriginRequiresAnExactAccountBeforeWriting() throws {
        try withStore { store, root in
            let invalid = KubeConfigImportStorePayload(raw: payload("first").raw, sourceName: "config.yaml", sourceURL: nil, origin: .init(source: .nativeAccount, provider: .azure))
            XCTAssertThrowsError(try store.publishImportedKubeConfigs([invalid], reusing: []))
            XCTAssertFalse(FileManager.default.fileExists(atPath: root.appendingPathComponent("imports").path))
        }
    }

    private func payload(_ name: String, filename: String = "config.yaml") -> KubeConfigImportStorePayload {
        .init(raw: "users: [{name: synthetic-\(name), user: {token: synthetic-token-\(name)}}]\n", sourceName: filename, sourceURL: nil)
    }

    private func withStore(_ body: (AppOwnedKubeConfigImportStore, URL) throws -> Void) throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("RuneImportOwnershipTests.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        try body(AppOwnedKubeConfigImportStore(rootDirectory: root.appendingPathComponent("imports")), root)
    }
}
