import XCTest
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

    private func nsSubstring(_ source: String, _ range: NSRange?) -> String {
        guard let range else { return "" }
        return (source as NSString).substring(with: range)
    }
}
