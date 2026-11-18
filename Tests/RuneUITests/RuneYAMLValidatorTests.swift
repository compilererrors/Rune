import RuneCore
@testable import RuneUI
import XCTest

/// A fixture corpus for YAML validation: every problem is reported at the
/// exact line, column and range, with a message that says what is wrong.
final class RuneYAMLValidatorTests: XCTestCase {
    private let valid = """
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: sample-api
      namespace: team-alpha
      labels:
        app: sample-api
    spec:
      replicas: 2
      template:
        spec:
          containers:
          - name: api
            image: registry.example/sample-api:1.4.2
            ports:
            - containerPort: 8080
    """

    func testValidManifestHasNoIssues() {
        XCTAssertEqual(YAMLValidator.validate(valid), [])
        XCTAssertEqual(YAMLValidator.validate(valid, target: .init(kind: "Deployment", name: "sample-api", namespace: "team-alpha")), [])
    }

    func testBadIndentationIsReportedWhereTheParserStops() throws {
        let text = """
        apiVersion: v1
        kind: Pod
        metadata:
          name: sample
         namespace: team-alpha
        """
        let issue = try XCTUnwrap(YAMLValidator.validate(text).first)
        XCTAssertEqual(issue.source, .syntax)
        XCTAssertEqual(issue.severity, .error)
        XCTAssertEqual(issue.line, 5)
        XCTAssertEqual(issue.column, 2)
        XCTAssertTrue(issue.message.contains("Did not find expected key") || issue.message.contains("block"), issue.message)
        try assertRange(issue, in: text, startsWith: "namespace")
    }

    func testUnclosedQuoteNamesTheQuote() throws {
        let text = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: "sample
        data: {}
        """
        let issue = try XCTUnwrap(YAMLValidator.validate(text).first)
        XCTAssertEqual(issue.source, .syntax)
        XCTAssertTrue(issue.message.lowercased().contains("quoted scalar"), issue.message)
        XCTAssertNotNil(issue.line)
        XCTAssertNotNil(issue.range)
    }

    func testStrayTabIsReportedAtTheTab() throws {
        let text = "apiVersion: v1\nkind: Pod\nmetadata:\n\tname: sample\n"
        let issues = YAMLValidator.validate(text)
        let issue = try XCTUnwrap(issues.first)
        XCTAssertEqual(issue.message, "Tabs are not allowed in YAML indentation.")
        XCTAssertEqual(issue.line, 4)
        XCTAssertEqual(issue.column, 1)
        XCTAssertEqual(issue.range, YAMLValidationRange(location: (text as NSString).range(of: "\t").location, length: 1))
        XCTAssertEqual(issues.filter { $0.line == 4 }.count, 1, "one message per line, not the parser's generic one as well")
    }

    /// A tab in the text of a block scalar is content (a Makefile or a
    /// script in a ConfigMap); the document is valid and must stay appliable.
    func testTabsInsideBlockScalarsAreNotErrors() {
        let text = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: sample\ndata:\n  Makefile: |\n    build:\n    \tgo build ./...\n    \t\n  notes: >\n    folded\n    \ttext\n"
        XCTAssertEqual(YAMLValidator.validate(text), [])
    }

    func testATabThatBreaksTheDocumentIsStillReportedNextToValidTabs() throws {
        let text = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: sample\ndata:\n  script: |\n    run:\n    \techo ok\n\tbroken: true\n"
        let issues = YAMLValidator.validate(text)
        XCTAssertEqual(issues.map(\.message), ["Tabs are not allowed in YAML indentation."])
        XCTAssertEqual(issues.first?.line, 9, "the tab the parser stopped on, not the one inside the block scalar")
    }

    /// The parser (and Kubernetes' own, a port of it) reads a block scalar's
    /// indentation from its first line, so a tab there is an error.
    func testATabOnTheFirstLineOfABlockScalarIsReported() throws {
        let text = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: sample\ndata:\n  script: |\n    \techo ok\n"
        let issue = try XCTUnwrap(YAMLValidator.validate(text).first)
        XCTAssertEqual(issue.message, "Tabs are not allowed in YAML indentation.")
        XCTAssertEqual(issue.line, 7)
    }

    func testAnEmptyLabelOrAnnotationValueIsAWarningNotAnError() throws {
        let text = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: sample
          labels:
            tier:
          annotations:
            example.test/note: ~
            example.test/count: 3
        """
        let issues = YAMLValidator.validate(text)
        XCTAssertEqual(issues.filter { $0.severity == .warning }.map(\.message), [
            "metadata.labels.tier has no value; write \"\" for an empty string",
            "metadata.annotations.example.test/note has no value; write \"\" for an empty string",
        ])
        XCTAssertEqual(issues.filter { $0.severity == .error }.map(\.message), [
            "metadata.annotations.example.test/count must be a string; quote the value",
        ], "a number is still an error")
        try assertRange(try XCTUnwrap(issues.first), in: text, startsWith: "tier")
    }

    func testDuplicateKeyIsNamed() throws {
        let text = """
        apiVersion: v1
        kind: Pod
        metadata:
          name: one
          name: two
        """
        let issue = try XCTUnwrap(YAMLValidator.validate(text).first)
        XCTAssertEqual(issue.source, .syntax)
        XCTAssertTrue(issue.message.hasPrefix("Duplicate key “name”"), issue.message)
        XCTAssertEqual(issue.line.map { [4, 5].contains($0) }, true)
        XCTAssertEqual(issue.column, 3)
    }

    func testInvalidListItemIsReported() throws {
        let text = """
        apiVersion: v1
        kind: Pod
        metadata:
          name: sample
        spec:
          containers:
          - name: api
            image: registry.example/api:1
           - name: sidecar
        """
        let issue = try XCTUnwrap(YAMLValidator.validate(text).first)
        XCTAssertEqual(issue.source, .syntax)
        XCTAssertEqual(issue.line, 9)
    }

    func testMultiDocumentFilesValidateEveryDocument() throws {
        let text = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: first
        ---
        apiVersion: v1
        metadata:
          name: second
        """
        let issues = YAMLValidator.validate(text)
        let issue = try XCTUnwrap(issues.first)
        XCTAssertEqual(issue.message, "Missing kind")
        XCTAssertEqual(issue.source, .kubernetes)
        XCTAssertEqual(issue.line, 6, "reported at the start of the second document")
    }

    func testCRLFLineEndingsKeepExactPositions() throws {
        let text = "apiVersion: v1\r\nkind: Pod\r\nmetadata:\r\n  name: sample\r\n namespace: x\r\n"
        let issue = try XCTUnwrap(YAMLValidator.validate(text).first)
        XCTAssertEqual(issue.line, 5)
        try assertRange(issue, in: text, startsWith: "namespace")
    }

    func testUnicodeColumnsMapToTheRightCharacters() throws {
        let text = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: sample
          labels:
            team: 🚀
            count: 3
        """
        let issue = try XCTUnwrap(YAMLValidator.validate(text).first)
        XCTAssertEqual(issue.message, "metadata.labels.count must be a string; quote the value")
        XCTAssertEqual(issue.line, 7)
        XCTAssertEqual(issue.column, 12)
        try assertRange(issue, in: text, startsWith: "3")
    }

    func testMissingKindAndNonStringApiVersion() throws {
        let text = """
        apiVersion: 1
        metadata:
          name: sample
        """
        let issues = YAMLValidator.validate(text)
        // Sorted by position: a missing key is reported at the document start.
        XCTAssertEqual(issues.map(\.message), [
            "Missing kind",
            "apiVersion must be a string, for example v1 or apps/v1"
        ])
        XCTAssertEqual(issues.last?.line, 1)
        XCTAssertEqual(issues.last?.column, 13)
    }

    func testCommonFieldTypesAreChecked() {
        let text = """
        apiVersion: apps/v1
        kind: Deployment
        metadata:
          name: sample
        spec:
          replicas: two
          template:
            spec:
              containers:
              - name: api
                image: example/api:1
                ports:
                - containerPort: http
        """
        let messages = YAMLValidator.validate(text).map(\.message)
        XCTAssertEqual(messages, [
            "spec.replicas must be a whole number",
            "spec.template.spec.containers[0].ports[0].containerPort must be a number"
        ])
    }

    func testTargetMismatchPointsAtTheField() throws {
        let text = """
        apiVersion: v1
        kind: Service
        metadata:
          name: other
          namespace: team-beta
        """
        let issues = YAMLValidator.validate(text, target: .init(kind: "Pod", name: "sample", namespace: "team-alpha"))
        XCTAssertEqual(issues.map(\.line), [2, 4, 5])
        XCTAssertEqual(issues.map(\.severity), [.error, .error, .error])
        try assertRange(issues[1], in: text, startsWith: "other")
    }

    func testMissingImageIsAWarningNotAnError() {
        let text = """
        apiVersion: v1
        kind: Pod
        metadata:
          name: sample
        spec:
          containers:
          - name: api
        """
        let issues = YAMLValidator.validate(text)
        XCTAssertEqual(issues.map(\.severity), [.warning])
        XCTAssertEqual(issues.first?.line, 7)
    }

    func testValidationOfALargeDocumentStaysFastKPI() {
        var lines = ["apiVersion: v1", "kind: ConfigMap", "metadata:", "  name: large", "data:"]
        for index in 0..<11_995 {
            lines.append("  key-\(index): \"value \(index)\"")
        }
        let text = lines.joined(separator: "\n")
        XCTAssertEqual(text.split(separator: "\n").count, 12_000)

        let started = ContinuousClock.now
        let issues = YAMLValidator.validate(text)
        let elapsed = started.duration(to: .now)
        XCTAssertEqual(issues, [])
        let seconds = Double(elapsed.components.seconds) + Double(elapsed.components.attoseconds) / 1e18
        #if DEBUG
        let budget = 1.5
        #else
        let budget = 0.4
        #endif
        XCTAssertLessThan(seconds, budget, "KPI: validating a 12,000-line manifest should stay below \(budget)s off the main thread.")
    }

    private func assertRange(_ issue: YAMLValidationIssue, in text: String, startsWith expected: String, file: StaticString = #filePath, line: UInt = #line) throws {
        let range = try XCTUnwrap(issue.range, file: file, line: line)
        let marked = (text as NSString).substring(with: range.nsRange)
        XCTAssertTrue(marked.hasPrefix(expected) || expected.hasPrefix(marked), "marked “\(marked)”, expected “\(expected)”", file: file, line: line)
    }
}
