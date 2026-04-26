import XCTest
import CoreGraphics
import RuneCore
@testable import RuneUI

final class YAMLLanguageServiceTests: XCTestCase {
    func testAnalyzeHighlightsDirectiveKeyScalarsAndComment() {
        let source = """
        ---
        enabled: true # comment
        count: 12
        """

        let analysis = YAMLLanguageService.analyze(source)

        XCTAssertTrue(analysis.highlights.contains { $0.kind == .directive && nsSubstring(source, $0.range) == "---" })
        XCTAssertTrue(analysis.highlights.contains { $0.kind == .key && nsSubstring(source, $0.range) == "enabled" })
        XCTAssertTrue(analysis.highlights.contains { $0.kind == .boolean && nsSubstring(source, $0.range) == "true" })
        XCTAssertTrue(analysis.highlights.contains { $0.kind == .comment && nsSubstring(source, $0.range) == "# comment" })
        XCTAssertTrue(analysis.highlights.contains { $0.kind == .number && nsSubstring(source, $0.range) == "12" })
    }

    func testAnalyzeHighlightsQuotedKeysAnchorsAndAliases() {
        let source = """
        "display-name": &main "Rune"
        alias: *main
        """

        let analysis = YAMLLanguageService.analyze(source)

        XCTAssertTrue(analysis.highlights.contains { $0.kind == .key && nsSubstring(source, $0.range) == "\"display-name\"" })
        XCTAssertTrue(analysis.highlights.contains { $0.kind == .anchor && nsSubstring(source, $0.range) == "&main" })
        XCTAssertTrue(analysis.highlights.contains { $0.kind == .alias && nsSubstring(source, $0.range) == "*main" })
        XCTAssertTrue(analysis.highlights.contains { $0.kind == .string && nsSubstring(source, $0.range) == "\"Rune\"" })
    }

    func testAnalyzeReportsTabAndFlowDelimiterDiagnostics() {
        let source = "metadata:\n\tname: api\nitems: [1, 2\n"

        let analysis = YAMLLanguageService.analyze(source)

        XCTAssertTrue(analysis.validationIssues.contains {
            $0.message == "Tabs are not allowed in YAML indentation."
                && nsSubstring(source, $0.range?.nsRange) == "\t"
        })
        XCTAssertTrue(analysis.validationIssues.contains {
            $0.message == "Unclosed '[' flow collection."
                && nsSubstring(source, $0.range?.nsRange) == "["
        })
    }

    func testAnalyzeOnlyReportsTabsInIndentation() {
        let source = "metadata:\n  value: hello\tworld\n"

        let analysis = YAMLLanguageService.analyze(source)

        XCTAssertFalse(analysis.validationIssues.contains {
            $0.message == "Tabs are not allowed in YAML indentation."
        })
    }

    func testAnalyzeWarnsForOddIndentation() {
        let source = "metadata:\n   name: api\n"

        let analysis = YAMLLanguageService.analyze(source)

        XCTAssertTrue(analysis.validationIssues.contains {
            $0.severity == .warning
                && $0.message == "Indentation is not aligned to a two-space YAML level."
                && $0.line == 2
        })
    }

    func testAnalyzeReportsUnexpectedIndentationBelowScalarMapping() {
        let source = """
        apiVersion: v1
        kind: Pod
          metadata:
            name: api
        """

        let analysis = YAMLLanguageService.analyze(source)

        XCTAssertTrue(analysis.validationIssues.contains {
            $0.severity == .error
                && $0.message == "Unexpected indentation. The previous line does not start a nested YAML block."
                && $0.line == 3
        })
    }

    func testAnalyzeAllowsPlainScalarContinuationBelowMappingValue() {
        let source = """
        spec:
          containers:
            - name: app
              env:
                - name: JVM_OPTS
                  value: -javaagent:applicationinsights-agent.jar -XX:+UseContainerSupport -Xmx200M
                    -Xms50M
                - name: NEXT
                  value: ok
        """

        let analysis = YAMLLanguageService.analyze(source)

        XCTAssertFalse(analysis.validationIssues.contains {
            $0.message == "Unexpected indentation. The previous line does not start a nested YAML block."
        })
    }

    func testAnalyzeStillReportsUnexpectedNestedMappingBelowScalarMappingValue() {
        let source = """
        spec:
          value: plain
            nested: invalid
        """

        let analysis = YAMLLanguageService.analyze(source)

        XCTAssertTrue(analysis.validationIssues.contains {
            $0.severity == .error
                && $0.message == "Unexpected indentation. The previous line does not start a nested YAML block."
                && $0.line == 3
        })
    }

    func testAnalyzeSuppressesIndentWarningWhenSameLineHasSyntaxError() {
        let source = """
        metadata:
          labels:
             app: api
        kind: Pod
          metadata:
        """

        let analysis = YAMLLanguageService.analyze(source)

        XCTAssertTrue(analysis.validationIssues.contains {
            $0.severity == .error
                && $0.message == "Unexpected indentation. The previous line does not start a nested YAML block."
                && $0.line == 5
        })
        XCTAssertFalse(analysis.validationIssues.contains {
            $0.severity == .warning
                && $0.message == "Indentation is not aligned to a two-space YAML level."
                && $0.line == 5
        })
        XCTAssertTrue(analysis.validationIssues.contains {
            $0.severity == .warning
                && $0.message == "Indentation is not aligned to a two-space YAML level."
                && $0.line == 3
        })
    }

    func testAnalyzeAllowsIndentationBelowBlockOpeners() {
        let source = """
        apiVersion: v1
        kind: Pod
        metadata:
          labels:
            app: api
        spec:
          containers:
            - name: app
              image: app:latest
        """

        let analysis = YAMLLanguageService.analyze(source)

        XCTAssertFalse(analysis.validationIssues.contains {
            $0.message == "Unexpected indentation. The previous line does not start a nested YAML block."
        })
    }

    func testDocumentModelProducesTokensAndDiagnosticsInOnePass() {
        let source = """
        apiVersion: v1
        kind: Pod
          metadata:
            labels:
              app: api
        """

        let model = YAMLDocumentModel(source: source)

        XCTAssertTrue(model.tokens.contains { $0.kind == .key && nsSubstring(source, $0.range) == "apiVersion" })
        XCTAssertTrue(model.tokens.contains { $0.kind == .string || $0.kind == .number || $0.kind == .boolean || $0.kind == .key })
        XCTAssertTrue(model.diagnostics.contains {
            $0.severity == .error
                && $0.message == "Unexpected indentation. The previous line does not start a nested YAML block."
                && $0.line == 3
        })
    }

    func testAnalyzeReportsMessagesAndLineNumbersForQuotedStringErrors() {
        let source = "metadata:\n  name: \"Rune\n"

        let analysis = YAMLLanguageService.analyze(source)

        guard let issue = analysis.validationIssues.first(where: { $0.message == "Unclosed double-quoted string." }) else {
            return XCTFail("Expected a quoted-string validation issue")
        }

        XCTAssertEqual(issue.line, 2)
        XCTAssertEqual(issue.column, 9)
    }

    func testSuggestedIndentationFollowsMappingAndSequenceShapes() {
        XCTAssertEqual(YAMLLanguageService.suggestedIndentation(after: "metadata:"), "  ")
        XCTAssertEqual(YAMLLanguageService.suggestedIndentation(after: "  - name:"), "    ")
        XCTAssertEqual(YAMLLanguageService.suggestedIndentation(after: "  name: value"), "  ")
    }

    func testSoftTabWhitespaceAdvancesToTwoSpaceStop() {
        XCTAssertEqual(YAMLLanguageService.softTabWhitespace(forColumn: 0), "  ")
        XCTAssertEqual(YAMLLanguageService.softTabWhitespace(forColumn: 1), " ")
        XCTAssertEqual(YAMLLanguageService.softTabWhitespace(forColumn: 2), "  ")
    }

    func testIndentGuideMetricsAlignToTwoSpaceColumns() {
        let metrics = YAMLIndentGuideMetrics()

        XCTAssertEqual(metrics.guideLevels(forIndentColumns: 0), [])
        XCTAssertEqual(metrics.guideLevels(forIndentColumns: 2), [1])
        XCTAssertEqual(metrics.guideLevels(forIndentColumns: 6), [1, 2, 3])
        XCTAssertEqual(metrics.guideColumn(forLevel: 1), 1)
        XCTAssertEqual(metrics.guideColumn(forLevel: 2), 3)
        XCTAssertEqual(metrics.guideXPosition(forLevel: 2, columnWidth: 8, insetX: 10), 34, accuracy: 0.001)
    }

    func testTabStopMetricsUseConfiguredIndentWidth() {
        let metrics = YAMLTabStopMetrics(indentWidth: 2)

        XCTAssertEqual(metrics.defaultInterval(spaceWidth: 7.5), 15, accuracy: 0.001)
    }

    func testTabMarkerMetricsStayCenteredInGlyphRect() {
        let metrics = YAMLTabMarkerMetrics()
        let glyphRect = CGRect(x: 20, y: 0, width: 24, height: 18)
        let lineRect = CGRect(x: 0, y: 4, width: 200, height: 18)

        let marker = metrics.markerRect(glyphRect: glyphRect, lineRect: lineRect)

        XCTAssertEqual(marker.midX, glyphRect.midX, accuracy: 0.001)
        XCTAssertEqual(marker.midY, lineRect.midY, accuracy: 0.001)
        XCTAssertEqual(marker.width, 16, accuracy: 0.001)
        XCTAssertEqual(marker.height, 11, accuracy: 0.001)
    }

    func testYAMLTextNavigationPrefersIssueRange() {
        let source = "apiVersion: v1\nkind: Pod\n"
        let issue = YAMLValidationIssue(
            source: .syntax,
            severity: .error,
            message: "Problem",
            line: 2,
            column: 1,
            range: YAMLValidationRange(location: 12, length: 4)
        )

        let request = YAMLTextNavigationRequest(issue: issue, sequence: 1)

        XCTAssertEqual(YAMLTextNavigation.targetRange(in: source, request: request), NSRange(location: 12, length: 4))
    }

    func testYAMLTextNavigationFallsBackToLineAndColumn() {
        let source = "apiVersion: v1\nkind: Pod\nmetadata:\n"
        let issue = YAMLValidationIssue(
            source: .kubernetes,
            severity: .error,
            message: "Problem",
            line: 2,
            column: 3
        )

        let request = YAMLTextNavigationRequest(issue: issue, sequence: 1)

        XCTAssertEqual(YAMLTextNavigation.targetRange(in: source, request: request), NSRange(location: 17, length: 1))
    }

    func testYAMLAnalysisIsFastEnoughForLargeKubernetesManifest() {
        let container = """
              - name: app
                image: example/app:latest
                env:
                  - name: FEATURE_FLAG
                    value: "true"
                  - name: RETRY_COUNT
                    value: "12"
                resources:
                  requests:
                    memory: 300Mi
                  limits:
                    memory: 1Gi
        """
        let source = """
        apiVersion: v1
        kind: Pod
        metadata:
          name: large
          labels:
            app: large
        spec:
          containers:
        """ + String(repeating: container + "\n", count: 120)

        let start = CFAbsoluteTimeGetCurrent()
        let analysis = YAMLLanguageService.analyze(source)
        let elapsed = CFAbsoluteTimeGetCurrent() - start

        XCTAssertFalse(analysis.highlights.isEmpty)
        XCTAssertLessThan(elapsed, 0.15)
    }

    func testLargeDocumentsPreferViewportAnalysis() {
        let line = "metadata:\n  labels:\n    app: api\n"
        let source = String(repeating: line, count: 1_000)

        XCTAssertTrue(YAMLLanguageService.prefersViewportAnalysis(source))
    }

    func testFragmentAnalysisKeepsAbsoluteRangesAndLines() {
        let source = """
        apiVersion: v1
        kind: Pod
        metadata:
          labels:
            app: api
          bad: [1, 2
        spec:
          containers: []
        """
        let nsSource = source as NSString
        let badOffset = nsSource.range(of: "bad:").location
        let analysis = YAMLLanguageService.analyzeFragment(
            source,
            range: NSRange(location: badOffset, length: 16)
        )

        XCTAssertTrue(analysis.highlights.contains {
            $0.kind == .key && nsSubstring(source, $0.range) == "bad"
        })
        XCTAssertTrue(analysis.validationIssues.contains {
            $0.message == "Unclosed '[' flow collection."
                && $0.line == 6
                && nsSubstring(source, $0.range?.nsRange) == "["
        })
    }

    private func nsSubstring(_ source: String, _ range: NSRange?) -> String {
        guard let range else { return "" }
        return (source as NSString).substring(with: range)
    }
}
