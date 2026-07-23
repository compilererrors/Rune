import Foundation
import XCTest
@testable import RuneDiagnostics

final class VerboseTraceSecurityTests: XCTestCase {
    func testKubernetesRequestTraceRedactsIdentifiersAndQueryValues() {
        let message = """
        start method=GET context=Synthetic Private Context \
        path=/apis/apps/v1/namespaces/synthetic-private/deployments/synthetic-workload/status?fieldSelector=metadata.name%3Dsynthetic-workload&token=synthetic-token \
        server=https://synthetic-user:synthetic-password@api.synthetic.invalid:6443 \
        tls=kubeconfig-ca auth=bearer-token
        """

        let sanitized = VerboseKubeTrace.privacySafeMessage(message)

        XCTAssertTrue(sanitized.contains("method=GET"))
        XCTAssertTrue(sanitized.contains("context=<redacted-context>"))
        XCTAssertTrue(sanitized.contains(
            "path=/apis/apps/v1/namespaces/<namespace>/deployments/<name>/status"
                + "?fieldSelector=<redacted>&<redacted>"
        ))
        XCTAssertTrue(sanitized.contains("server=<redacted-host>"))
        XCTAssertTrue(sanitized.contains("tls=kubeconfig-ca"))
        XCTAssertTrue(sanitized.contains("auth=bearer-token"))
        XCTAssertFalse(sanitized.contains("Synthetic Private Context"))
        XCTAssertFalse(sanitized.contains("synthetic-private"))
        XCTAssertFalse(sanitized.contains("synthetic-workload"))
        XCTAssertFalse(sanitized.contains("synthetic-token"))
        XCTAssertFalse(sanitized.contains("api.synthetic.invalid"))
        XCTAssertFalse(sanitized.contains("synthetic-password"))
    }

    func testKubernetesTraceRedactsCredentialsHostsAndUnstructuredErrors() {
        let message = """
        server-trust rejected host=api.synthetic.invalid \
        serverName=tls.synthetic.invalid \
        serverTrust=trust.synthetic.invalid \
        namespace=synthetic-private-namespace \
        authorization=Bearer synthetic-bearer \
        token=synthetic-token \
        password=synthetic-password \
        error=request failed at https://synthetic-user:synthetic-password@api.synthetic.invalid/private?token=synthetic-token
        """

        let sanitized = VerboseKubeTrace.privacySafeMessage(message)

        XCTAssertTrue(sanitized.contains("host=<redacted-host>"))
        XCTAssertTrue(sanitized.contains("serverName=<redacted-host>"))
        XCTAssertTrue(sanitized.contains("serverTrust=<redacted-host>"))
        XCTAssertTrue(sanitized.contains("namespace=<redacted-namespace>"))
        XCTAssertTrue(sanitized.contains("authorization=<redacted>"))
        XCTAssertTrue(sanitized.contains("token=<redacted>"))
        XCTAssertTrue(sanitized.contains("password=<redacted>"))
        XCTAssertTrue(sanitized.contains("error=<redacted>"))
        XCTAssertFalse(sanitized.contains("synthetic-bearer"))
        XCTAssertFalse(sanitized.contains("synthetic-token"))
        XCTAssertFalse(sanitized.contains("synthetic-password"))
        XCTAssertFalse(sanitized.contains("api.synthetic.invalid"))
        XCTAssertFalse(sanitized.contains("tls.synthetic.invalid"))
        XCTAssertFalse(sanitized.contains("trust.synthetic.invalid"))
        XCTAssertFalse(sanitized.contains("synthetic-private-namespace"))
    }

    func testKubeconfigSummaryDoesNotExposePathBasenames() {
        let summary = VerboseKubeTrace.kubeconfigSummary([
            "KUBECONFIG": "/synthetic/private/team-a.yaml:/synthetic/private/team-b.yaml"
        ])

        XCTAssertEqual(summary, "configured(2)")
        XCTAssertEqual(VerboseKubeTrace.kubeconfigSummary([:]), "default-discovery")
        XCTAssertFalse(summary.contains("team-a"))
        XCTAssertFalse(summary.contains("team-b"))
        XCTAssertFalse(summary.contains("/synthetic"))
    }

    func testTraceRedactsIdentifierValuesContainingFieldLikeTextAndFreeErrors() {
        let message = """
        context=synthetic-context tenant=synthetic-private-tenant \
        namespace=synthetic-namespace owner=synthetic-private-owner \
        path=/api/v1/namespaces/synthetic-namespace/pods \
        warning: request exposed synthetic-private-warning
        """

        let sanitized = VerboseKubeTrace.privacySafeMessage(message)

        XCTAssertTrue(sanitized.contains("context=<redacted-context>"))
        XCTAssertTrue(sanitized.contains("namespace=<redacted-namespace>"))
        XCTAssertTrue(sanitized.contains("warning: <redacted>"))
        XCTAssertFalse(sanitized.contains("synthetic-context"))
        XCTAssertFalse(sanitized.contains("synthetic-private-tenant"))
        XCTAssertFalse(sanitized.contains("synthetic-namespace"))
        XCTAssertFalse(sanitized.contains("synthetic-private-owner"))
        XCTAssertFalse(sanitized.contains("synthetic-private-warning"))
    }

    func testTraceWriterRepairsExistingDirectoryAndFilePermissions() throws {
        let fileManager = FileManager.default
        let root = fileManager.temporaryDirectory
            .appendingPathComponent("RuneDiagnosticsTraceTests-\(UUID().uuidString)", isDirectory: true)
        defer { try? fileManager.removeItem(at: root) }

        let directory = root.appendingPathComponent("Logs", isDirectory: true)
        let active = directory.appendingPathComponent("debug-trace.log")
        let rotated = directory.appendingPathComponent("debug-trace.log.1")
        try fileManager.createDirectory(
            at: directory,
            withIntermediateDirectories: true,
            attributes: [.posixPermissions: NSNumber(value: Int16(0o777))]
        )
        try Data("existing\n".utf8).write(to: active)
        try Data("rotated\n".utf8).write(to: rotated)
        try fileManager.setAttributes(
            [.posixPermissions: NSNumber(value: Int16(0o666))],
            ofItemAtPath: active.path
        )
        try fileManager.setAttributes(
            [.posixPermissions: NSNumber(value: Int16(0o666))],
            ofItemAtPath: rotated.path
        )

        try DebugTraceWriter.writeLine("next\n", to: active, maxBytes: 1_024)

        XCTAssertEqual(try permissions(at: directory), 0o700)
        XCTAssertEqual(try permissions(at: active), 0o600)
        XCTAssertEqual(try permissions(at: rotated), 0o600)
        XCTAssertEqual(try String(contentsOf: active, encoding: .utf8), "existing\nnext\n")
    }

    func testTraceRotationKeepsActiveAndRotatedFilesPrivate() throws {
        let fileManager = FileManager.default
        let root = fileManager.temporaryDirectory
            .appendingPathComponent("RuneDiagnosticsRotationTests-\(UUID().uuidString)", isDirectory: true)
        defer { try? fileManager.removeItem(at: root) }

        let directory = root.appendingPathComponent("Logs", isDirectory: true)
        let active = directory.appendingPathComponent("debug-trace.log")
        let rotated = directory.appendingPathComponent("debug-trace.log.1")
        try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
        try Data("oversized-existing-line\n".utf8).write(to: active)
        try Data("stale-rotation\n".utf8).write(to: rotated)
        try fileManager.setAttributes(
            [.posixPermissions: NSNumber(value: Int16(0o666))],
            ofItemAtPath: active.path
        )
        try fileManager.setAttributes(
            [.posixPermissions: NSNumber(value: Int16(0o666))],
            ofItemAtPath: rotated.path
        )

        try DebugTraceWriter.writeLine("fresh\n", to: active, maxBytes: 16)

        XCTAssertEqual(try permissions(at: directory), 0o700)
        XCTAssertEqual(try permissions(at: active), 0o600)
        XCTAssertEqual(try permissions(at: rotated), 0o600)
        XCTAssertEqual(try String(contentsOf: active, encoding: .utf8), "fresh\n")
        XCTAssertEqual(
            try String(contentsOf: rotated, encoding: .utf8),
            "oversized-existing-line\n"
        )
    }

    func testTraceWriterRefusesSymbolicLinkTargets() throws {
        let fileManager = FileManager.default
        let root = fileManager.temporaryDirectory
            .appendingPathComponent("RuneDiagnosticsSymlinkTests-\(UUID().uuidString)", isDirectory: true)
        defer { try? fileManager.removeItem(at: root) }
        let directory = root.appendingPathComponent("Logs", isDirectory: true)
        let target = root.appendingPathComponent("unrelated.log")
        let active = directory.appendingPathComponent("debug-trace.log")
        try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
        try Data("unchanged\n".utf8).write(to: target)
        try fileManager.createSymbolicLink(at: active, withDestinationURL: target)

        XCTAssertThrowsError(
            try DebugTraceWriter.writeLine("must-not-follow\n", to: active, maxBytes: 1_024)
        )
        XCTAssertEqual(try String(contentsOf: target, encoding: .utf8), "unchanged\n")
    }

    func testTraceClearPreservesDirectoryAtLogFilePath() throws {
        let fileManager = FileManager.default
        let root = fileManager.temporaryDirectory
            .appendingPathComponent("RuneDiagnosticsClearDirectoryTests-\(UUID().uuidString)", isDirectory: true)
        defer { try? fileManager.removeItem(at: root) }

        let directory = root.appendingPathComponent("Logs", isDirectory: true)
        let active = directory.appendingPathComponent("debug-trace.log", isDirectory: true)
        let sentinel = active.appendingPathComponent("must-remain.txt")
        let rotated = directory.appendingPathComponent("debug-trace.log.1")
        try fileManager.createDirectory(at: active, withIntermediateDirectories: true)
        try Data("sentinel\n".utf8).write(to: sentinel)
        try Data("rotated\n".utf8).write(to: rotated)

        XCTAssertThrowsError(try DebugTraceWriter.clearFiles(at: active))
        XCTAssertTrue(fileManager.fileExists(atPath: active.path))
        XCTAssertEqual(try String(contentsOf: sentinel, encoding: .utf8), "sentinel\n")
        XCTAssertFalse(fileManager.fileExists(atPath: rotated.path))
    }

    func testTraceClearUnlinksSymbolWithoutTouchingTarget() throws {
        let fileManager = FileManager.default
        let root = fileManager.temporaryDirectory
            .appendingPathComponent("RuneDiagnosticsClearSymlinkTests-\(UUID().uuidString)", isDirectory: true)
        defer { try? fileManager.removeItem(at: root) }

        let directory = root.appendingPathComponent("Logs", isDirectory: true)
        let target = root.appendingPathComponent("unrelated.log")
        let active = directory.appendingPathComponent("debug-trace.log")
        try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
        try Data("unchanged\n".utf8).write(to: target)
        try fileManager.createSymbolicLink(at: active, withDestinationURL: target)

        try DebugTraceWriter.clearFiles(at: active)

        XCTAssertFalse(fileManager.fileExists(atPath: active.path))
        XCTAssertEqual(try String(contentsOf: target, encoding: .utf8), "unchanged\n")
    }

    private func permissions(at url: URL) throws -> Int {
        let attributes = try FileManager.default.attributesOfItem(atPath: url.path)
        return (attributes[.posixPermissions] as? NSNumber)?.intValue ?? -1
    }
}
