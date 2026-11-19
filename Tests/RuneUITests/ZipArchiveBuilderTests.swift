import Foundation
import RuneExport
import XCTest

final class ZipArchiveBuilderTests: XCTestCase {
    private let logText = (0..<4_000)
        .map { "2026-05-05T10:00:\(String(format: "%02d", $0 % 60))Z level=info message=\($0) räksmörgås" }
        .joined(separator: "\n")

    func testEntriesAreDeflatedAndReadBackUnchanged() throws {
        let random = Data((0..<4_096).map { _ in UInt8.random(in: 0...255) })
        let archive = try ZipArchiveBuilder.build(entries: [
            ZipArchiveEntry(path: "logs/merged.log", data: Data(logText.utf8)),
            ZipArchiveEntry(path: "logs/random.bin", data: random),
            ZipArchiveEntry(path: "logs/short.txt", data: Data("ok".utf8)),
            ZipArchiveEntry(path: "logs/empty.txt", data: Data())
        ])

        XCTAssertLessThan(archive.count, logText.utf8.count / 4, "a log archive should be much smaller than its logs")
        let entries = try ZipArchiveTestSupport.entries(from: archive)
        XCTAssertEqual(entries["logs/merged.log"].map { String(decoding: $0, as: UTF8.self) }, logText)
        XCTAssertEqual(entries["logs/random.bin"].map { Data($0) }, random, "data that does not shrink is stored")
        XCTAssertEqual(entries["logs/short.txt"].map { String(decoding: $0, as: UTF8.self) }, "ok")
        XCTAssertEqual(entries["logs/empty.txt"]?.count, 0)
    }

    func testTheSystemUnzipAcceptsTheArchiveAndRestoresDatesAndText() throws {
        let unzip = "/usr/bin/unzip"
        guard FileManager.default.isExecutableFile(atPath: unzip) else { throw XCTSkip("unzip is not installed") }
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("ZipArchiveBuilderTests.\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }

        let exported = try XCTUnwrap(Calendar(identifier: .gregorian).date(
            from: DateComponents(year: 2026, month: 5, day: 5, hour: 10, minute: 30, second: 12)
        ))
        let archive = try ZipArchiveBuilder.build(
            entries: [
                ZipArchiveEntry(path: "export/pods/api-0.log", data: Data(logText.utf8)),
                ZipArchiveEntry(path: "export/metadata.json", data: Data(#"{"scope":"full"}"#.utf8))
            ],
            modificationDate: exported
        )
        let archiveURL = directory.appendingPathComponent("export.zip")
        try archive.write(to: archiveURL)

        func run(_ arguments: [String]) throws -> Int32 {
            let process = Process()
            process.executableURL = URL(fileURLWithPath: unzip)
            process.arguments = arguments
            process.standardOutput = Pipe()
            process.standardError = Pipe()
            try process.run()
            process.waitUntilExit()
            return process.terminationStatus
        }
        XCTAssertEqual(try run(["-tq", archiveURL.path]), 0, "unzip found the archive damaged")
        XCTAssertEqual(try run(["-q", archiveURL.path, "-d", directory.path]), 0)

        let unpacked = directory.appendingPathComponent("export/pods/api-0.log")
        XCTAssertEqual(try String(contentsOf: unpacked, encoding: .utf8), logText)
        let modified = try XCTUnwrap(
            FileManager.default.attributesOfItem(atPath: unpacked.path)[.modificationDate] as? Date
        )
        XCTAssertEqual(modified.timeIntervalSince(exported), 0, accuracy: 2, "entries carry the export time, not 1980")
    }

    func testMoreEntriesThanTheFormatCountsIsAnErrorNotATrap() {
        let entries = (0...Int(UInt16.max)).map { ZipArchiveEntry(path: "f\($0)", data: Data()) }
        XCTAssertThrowsError(try ZipArchiveBuilder.build(entries: entries)) { error in
            XCTAssertEqual(error as? ZipArchiveError, .tooManyEntries(entries.count))
        }
        XCTAssertNoThrow(try ZipArchiveBuilder.build(entries: Array(entries.dropLast())))
    }

    func testPathsAreKeptInsideTheArchive() throws {
        let archive = try ZipArchiveBuilder.build(entries: [
            ZipArchiveEntry(path: "/../outside/../pods/api 0.log", data: Data("a".utf8)),
            ZipArchiveEntry(path: "", data: Data("dropped".utf8))
        ])
        XCTAssertEqual(Array(try ZipArchiveTestSupport.entries(from: archive).keys), ["outside/pods/api 0.log"])
    }
}
