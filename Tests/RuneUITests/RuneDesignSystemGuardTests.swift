import Foundation
import XCTest

/// Keeps views on the design system in Sources/RuneUI/Design (see its
/// README). Views compose tokens and components; this fails when a view
/// styles text or controls by hand.
final class RuneDesignSystemGuardTests: XCTestCase {
    private struct Rule {
        let id: String
        let pattern: String
        let fix: String
    }

    private struct Exception {
        let file: String
        let rule: String
        let reason: String
    }

    private let rules = [
        Rule(id: "system-text-style",
             pattern: #"\.font\(\.(largeTitle|title|title2|title3|headline|subheadline|body|callout|footnote|caption|caption2)\b"#,
             fix: "use .runeText(role) so text follows the interface font size"),
        Rule(id: "fixed-font-size",
             pattern: #"\.font\(\.system\(size:\s*[0-9]"#,
             fix: "use .runeText(role) for text or RuneGlyphSize for symbols"),
        Rule(id: "native-segmented",
             pattern: #"\.pickerStyle\(\.segmented\)"#,
             fix: "use RuneTabBar"),
        Rule(id: "native-button-style",
             pattern: #"\.buttonStyle\(\.(bordered|borderedProminent|borderless|link)\)"#,
             fix: "use RuneToolbarButtonStyle with a role, or RuneCompactButtonStyle"),
        Rule(id: "rounded-border-field",
             pattern: #"\.textFieldStyle\(\.roundedBorder\)"#,
             fix: "use RuneFormTextFieldStyle"),
        Rule(id: "plain-field",
             pattern: #"\.textFieldStyle\(\.plain\)"#,
             fix: "use RuneFormTextFieldStyle"),
        Rule(id: "borderless-menu",
             pattern: #"\.menuStyle\(\.borderlessButton\)"#,
             fix: "use RuneToolbarMenu"),
        Rule(id: "menu-picker",
             pattern: #"\.pickerStyle\(\.menu\)"#,
             fix: "use RuneToolbarPicker"),
        Rule(id: "button-toggle",
             pattern: #"\.toggleStyle\(\.button\)"#,
             fix: "use RuneToolbarToggleStyle"),
        Rule(id: "color-literal",
             pattern: #"Color\.(red|green|blue|orange|yellow|purple|pink|teal|mint|cyan|indigo|brown|gray)\b"#,
             fix: "use RuneSemanticColorRole, the theme palette or RuneCategoryColor"),
        Rule(id: "radius-literal",
             pattern: #"(cornerRadius:\s*[0-9]|xRadius:\s*[0-9]|cornerRadius\s*=\s*[0-9])"#,
             fix: "use a RuneRadius token or RuneRadius.concentric(inner:inset:)"),
        Rule(id: "implicit-color-literal",
             pattern: #"(color|tint|fill):\s*\.(red|green|blue|orange|yellow|purple|gray)\b"#,
             fix: "use RuneSemanticColorRole or the theme palette")
    ]

    /// Each exception names why it is not a design-system component.
    private let exceptions = [
        Exception(file: "Views/Root/RuneRootView+WindowScaffold.swift", rule: "button-toggle",
                  reason: "native window toolbar item; NSToolbar draws it"),
        Exception(file: "Views/CommandPaletteView.swift", rule: "plain-field",
                  reason: "the palette's large search field is its own surface"),
        Exception(file: "Views/ResourceLogsInspectorView.swift", rule: "plain-field",
                  reason: "the log search bar draws the field inside its own search surface"),
        Exception(file: "Views/AddClusterNativeContextSection.swift", rule: "menu-picker",
                  reason: "a form popup with an optional placeholder row")
    ]

    /// Whole rows and cards that are tappable draw their own row surface with
    /// `.buttonStyle(.plain)`. New ones must be rows too; raise this only
    /// after checking the new use is not a control.
    private let plainRowButtonBudget = 25

    func testViewsUseDesignSystemInsteadOfAdHocStyling() throws {
        var violations: [String] = []
        for (path, source) in try viewSources() {
            let lines = source.components(separatedBy: "\n")
            for rule in rules {
                if exceptions.contains(where: { $0.file == path && $0.rule == rule.id }) { continue }
                let regex = try NSRegularExpression(pattern: rule.pattern)
                for (index, line) in lines.enumerated() {
                    let range = NSRange(line.startIndex..., in: line)
                    if regex.firstMatch(in: line, range: range) != nil {
                        violations.append("\(path):\(index + 1) [\(rule.id)] \(rule.fix)")
                    }
                }
            }
        }
        XCTAssertTrue(violations.isEmpty, "Ad-hoc styling outside Design/:\n" + violations.joined(separator: "\n"))
    }

    func testExceptionsStillApply() throws {
        let sources = Dictionary(uniqueKeysWithValues: try viewSources())
        for exception in exceptions {
            let source = try XCTUnwrap(sources[exception.file], "\(exception.file) no longer exists; drop its exception")
            let rule = try XCTUnwrap(rules.first { $0.id == exception.rule })
            XCTAssertNotNil(
                source.range(of: rule.pattern, options: .regularExpression),
                "\(exception.file) no longer needs the \(exception.rule) exception; drop it"
            )
        }
    }

    func testPlainButtonsStayWithinRowBudget() throws {
        let count = try viewSources().reduce(0) { total, entry in
            total + entry.1.components(separatedBy: ".buttonStyle(.plain)").count - 1
        }
        XCTAssertLessThanOrEqual(
            count, plainRowButtonBudget,
            "New .buttonStyle(.plain) outside Design/. Controls use RuneToolbarButtonStyle or RuneCompactButtonStyle; only whole tappable rows may use .plain."
        )
    }

    func testDesignReadmeDocumentsTheRulesWithoutTables() throws {
        let readme = try String(contentsOf: designDirectory.appendingPathComponent("README.md"), encoding: .utf8)
        for component in ["RuneToolbarButtonStyle", "RuneCompactButtonStyle", "RuneTabBar", "RuneFormTextFieldStyle",
                          "RuneBadge", "RuneStatusDot", "RuneToolbarOverflowRow", "RuneToolbarMenu", "RuneToolbarButtonGroup", "RuneControlTextFieldStyle", "RuneControlCard", "RuneStableLabel", "runeText"] {
            XCTAssertTrue(readme.contains(component), "README must say when to use \(component)")
        }
        let tableRows = readme.components(separatedBy: "\n").filter { $0.trimmingCharacters(in: .whitespaces).hasPrefix("|") }
        XCTAssertTrue(tableRows.isEmpty, "Documentation uses sections, never tables")
    }

    private var sourceRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI")
    }

    private var designDirectory: URL { sourceRoot.appendingPathComponent("Design") }

    /// Every Swift file in RuneUI except the design system itself and the
    /// debug gallery, which shows component variants on purpose.
    private func viewSources() throws -> [(String, String)] {
        let root = sourceRoot.standardizedFileURL.path + "/"
        let enumerator = try XCTUnwrap(FileManager.default.enumerator(at: sourceRoot, includingPropertiesForKeys: nil))
        var result: [(String, String)] = []
        for case let url as URL in enumerator where url.pathExtension == "swift" {
            let path = url.standardizedFileURL.path.replacingOccurrences(of: root, with: "")
            if path.hasPrefix("Design/") || path.hasPrefix("Debug/") { continue }
            result.append((path, try String(contentsOf: url, encoding: .utf8)))
        }
        return result.sorted { $0.0 < $1.0 }
    }
}
