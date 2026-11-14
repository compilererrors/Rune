import XCTest

/// Inspectors name what they show, never the section they sit in.
final class RuneInspectorTitleTests: XCTestCase {
    func testTerminalInspectorIsTitledByItsTargetPod() throws {
        let source = try RuneRootViewSource.text()
        let terminal = try XCTUnwrap(source.sliceBetween("var terminalDetails: some View {", "var terminalInspectorPod: PodSummary? {"))

        XCTAssertTrue(terminal.contains("RuneInspectorScaffold("), "the terminal inspector uses the shared inspector header")
        XCTAssertTrue(terminal.contains("title: pod.name"), "the header names the target pod")
        XCTAssertTrue(terminal.contains("subtitle: inspectorSubtitle(\"Pod\", namespace: pod.namespace)"), "with a kind and namespace subtitle")
        XCTAssertTrue(terminal.contains("status: RuneInspectorStatus(pod.displayStatus)"), "and the pod's status as the same pill as the pod inspector")
        XCTAssertTrue(terminal.contains("title: \"No pod targeted\""), "an empty state when no pod is targeted")
        XCTAssertFalse(terminal.contains("Text(appString(.terminal))"), "never the section name")
    }

    func testOverviewInspectorIsTitledByItsClusterContext() throws {
        let source = try RuneRootViewSource.text()
        let overview = try XCTUnwrap(source.sliceBetween("var overviewDetails: some View {", "inspectorActionButtonRow {"))
        XCTAssertTrue(overview.contains("Text(viewModel.state.selectedContext?.name ?? \"No cluster selected\")"))
        XCTAssertFalse(overview.contains("Text(\"Overview\")"))
    }

    func testEveryInspectorScaffoldIsTitledByTheInspectedObject() throws {
        let source = try RuneRootViewSource.text()
        let titles = source.components(separatedBy: "RuneInspectorScaffold(").dropFirst().compactMap { block -> String? in
            guard let line = block.split(separator: "\n").first(where: { $0.contains("title:") }) else { return nil }
            return line.trimmingCharacters(in: .whitespaces)
        }
        XCTAssertFalse(titles.isEmpty)
        for title in titles {
            XCTAssertFalse(title.contains("appString(."), "\(title) names a section, not the inspected object")
            XCTAssertFalse(title.contains("title: \""), "\(title) is a fixed label, not the inspected object")
        }
    }
}

private extension String {
    func sliceBetween(_ start: String, _ end: String) -> String? {
        guard let lower = range(of: start)?.upperBound,
              let upper = range(of: end, range: lower..<endIndex)?.lowerBound else { return nil }
        return String(self[lower..<upper])
    }
}
