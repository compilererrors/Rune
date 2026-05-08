import XCTest
@testable import RuneUI

final class ResourceStructuredLogAnalyzerTests: XCTestCase {
    func testDetectsJSONLAndExtractsCommonFields() {
        let text = """
        {"timestamp":"2026-05-06T10:00:00Z","level":"info","message":"started","pod":"api-0","container":"app","requestId":"req-1","trace_id":"trace-a","namespace":"default"}
        {"timestamp":"2026-05-06T10:00:01Z","severity":"error","msg":"failed","pod_name":"api-0","container_name":"app","request_id":"req-2","traceId":"trace-b","namespace_name":"default"}
        """

        let summary = ResourceStructuredLogAnalyzer.analyze(text: text)

        XCTAssertTrue(summary.isStructured)
        XCTAssertEqual(summary.totalLineCount, 2)
        XCTAssertEqual(summary.jsonLineCount, 2)
        XCTAssertEqual(summary.field("level")?.nonEmptyCount, 2)
        XCTAssertEqual(summary.field("message")?.sampleValues, ["started", "failed"])
        XCTAssertEqual(summary.field("pod")?.sampleValues, ["api-0"])
        XCTAssertEqual(summary.field("requestID")?.nonEmptyCount, 2)
        XCTAssertEqual(summary.field("traceID")?.nonEmptyCount, 2)
    }

    func testDetectsJSONPayloadAfterUnifiedLogPrefix() {
        let text = """
        [api-0] {"level":"info","message":"ready","kubernetes":{"pod_name":"api-0","container_name":"app","namespace_name":"default"}}
        [api-1] {"level":"warn","message":"slow","kubernetes":{"pod_name":"api-1","container_name":"worker","namespace_name":"default"}}
        """

        let summary = ResourceStructuredLogAnalyzer.analyze(text: text)

        XCTAssertTrue(summary.isStructured)
        XCTAssertEqual(summary.field("pod")?.sampleValues, ["api-0", "api-1"])
        XCTAssertEqual(summary.field("container")?.sampleValues, ["app", "worker"])
        XCTAssertEqual(summary.field("namespace")?.sampleValues, ["default"])
    }

    func testFieldSamplesKeepObservedAliasSearchQueries() {
        let text = """
        {"severity":"error","msg":"failed","requestId":"req-1"}
        {"level":"info","message":"recovered","request_id":"req-2"}
        """

        let summary = ResourceStructuredLogAnalyzer.analyze(text: text)

        let level = summary.field("level")
        let message = summary.field("message")
        let requestID = summary.field("requestID")

        XCTAssertEqual(level?.sampleSearchQuery(for: "error"), #""severity":"error""#)
        XCTAssertEqual(level?.sampleSearchQuery(for: "info"), #""level":"info""#)
        XCTAssertEqual(message?.sampleSearchQuery(for: "failed"), #""msg":"failed""#)
        XCTAssertEqual(requestID?.sampleSearchQuery(for: "req-1"), #""requestId":"req-1""#)
        XCTAssertEqual(ResourceStructuredLogFieldSearch.query(field: try XCTUnwrap(level), value: "error"), #""severity":"error""#)
    }

    func testDoesNotClassifyPlainTextLogsAsStructured() {
        let text = """
        INFO started
        WARN slow request
        ERROR failed
        """

        let summary = ResourceStructuredLogAnalyzer.analyze(text: text)

        XCTAssertFalse(summary.isStructured)
        XCTAssertEqual(summary.totalLineCount, 3)
        XCTAssertEqual(summary.jsonLineCount, 0)
        XCTAssertTrue(summary.fields.isEmpty)
    }

    func testDuplicateDetectionUsesStructuredMessageFingerprint() {
        let text = """
        {"timestamp":"2026-05-06T10:00:00Z","level":"error","message":"database unavailable","pod":"api-0"}
        {"timestamp":"2026-05-06T10:00:01Z","level":"error","message":"database unavailable","pod":"api-1"}
        {"timestamp":"2026-05-06T10:00:02Z","level":"info","message":"recovered","pod":"api-1"}
        """

        let summary = ResourceStructuredLogAnalyzer.analyze(text: text)

        XCTAssertEqual(summary.duplicateLines.count, 1)
        XCTAssertEqual(summary.duplicateLines.first?.count, 2)
        XCTAssertEqual(summary.duplicateLines.first?.fingerprint, "error|database unavailable")
    }

    func testDuplicateDetectionWorksForPlainTextLogs() {
        let text = """
        repeated line
        unique line
        repeated line
        """

        let summary = ResourceStructuredLogAnalyzer.analyze(text: text)

        XCTAssertFalse(summary.isStructured)
        XCTAssertEqual(summary.duplicateLines.count, 1)
        XCTAssertEqual(summary.duplicateLines.first?.count, 2)
        XCTAssertEqual(summary.duplicateLines.first?.sampleLine, "repeated line")
    }
}
