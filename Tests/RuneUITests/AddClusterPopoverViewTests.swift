import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class AddClusterPopoverViewTests: XCTestCase {
    func testManualTokenReadinessRequiresAllRequiredFields() {
        XCTAssertTrue(
            AddClusterPopoverStatePresentation.canImportManualToken(
                contextName: "development",
                serverURL: "https://cluster.example.invalid",
                bearerToken: "synthetic-token"
            )
        )
        XCTAssertFalse(
            AddClusterPopoverStatePresentation.canImportManualToken(
                contextName: "   ",
                serverURL: "https://cluster.example.invalid",
                bearerToken: "synthetic-token"
            )
        )
        XCTAssertFalse(
            AddClusterPopoverStatePresentation.canImportManualToken(
                contextName: "development",
                serverURL: "\n",
                bearerToken: "synthetic-token"
            )
        )
        XCTAssertFalse(
            AddClusterPopoverStatePresentation.canImportManualToken(
                contextName: "development",
                serverURL: "https://cluster.example.invalid",
                bearerToken: "\t"
            )
        )
    }

    func testDiscoveryStatusCoversLoadingEmptySingularAndPluralStates() {
        XCTAssertEqual(
            AddClusterPopoverStatePresentation.discoveryStatusText(
                kubeConfigSourceCount: 0,
                contextCount: 0,
                isLoading: true
            ),
            "Reading cluster contexts..."
        )
        XCTAssertEqual(
            AddClusterPopoverStatePresentation.discoveryStatusText(
                kubeConfigSourceCount: 0,
                contextCount: 0,
                isLoading: false
            ),
            "Watching for kubeconfig sources"
        )
        XCTAssertEqual(
            AddClusterPopoverStatePresentation.discoveryStatusText(
                kubeConfigSourceCount: 1,
                contextCount: 1,
                isLoading: false
            ),
            "1 context available from 1 source"
        )
        XCTAssertEqual(
            AddClusterPopoverStatePresentation.discoveryStatusText(
                kubeConfigSourceCount: 2,
                contextCount: 3,
                isLoading: false
            ),
            "3 contexts available from 2 sources"
        )
        XCTAssertEqual(
            AddClusterPopoverStatePresentation.discoveryStatusText(
                kubeConfigSourceCount: 2,
                contextCount: 0,
                isLoading: false
            ),
            "2 kubeconfig sources loaded"
        )
    }

    func testPopoverStateProjectionKPI() {
        let started = ContinuousClock.now
        var projectedCharacterCount = 0
        var readyDraftCount = 0

        for index in 0..<50_000 {
            projectedCharacterCount += AddClusterPopoverStatePresentation.discoveryStatusText(
                kubeConfigSourceCount: index % 4,
                contextCount: index % 7,
                isLoading: index.isMultiple(of: 19)
            ).count
            if AddClusterPopoverStatePresentation.canImportManualToken(
                contextName: index.isMultiple(of: 5) ? "" : "synthetic-\(index)",
                serverURL: "https://cluster.example.invalid",
                bearerToken: index.isMultiple(of: 11) ? " " : "synthetic-token"
            ) {
                readyDraftCount += 1
            }
        }

        let duration = started.duration(to: .now).components
        let elapsedSeconds = Double(duration.seconds) + Double(duration.attoseconds) / 1e18
        XCTAssertGreaterThan(projectedCharacterCount, 0)
        XCTAssertGreaterThan(readyDraftCount, 0)
        XCTAssertLessThan(
            elapsedSeconds,
            0.15,
            "KPI: 50k Add Cluster status and manual-field projections should stay below 150ms in debug."
        )
    }

    func testPopoverKeepsBoundedWidthAndHeightWhenManualSectionExpands() {
        for isExpanded in [false, true] {
            let host = NSHostingView(rootView: makePopover(isManualTokenExpanded: isExpanded))
            host.layoutSubtreeIfNeeded()

            XCTAssertEqual(
                host.fittingSize.width,
                RuneUILayoutMetrics.addClusterPopoverWidth,
                accuracy: 1
            )
            XCTAssertLessThanOrEqual(
                host.fittingSize.height,
                RuneUILayoutMetrics.addClusterPopoverMaxHeight + 1
            )
            XCTAssertGreaterThan(host.fittingSize.height, 300)
        }
    }

    func testProviderAndLocalToolsShareOneAdaptiveGrid() throws {
        let source = try String(contentsOf: addClusterPopoverSourceURL, encoding: .utf8)
        let providerRegion = try XCTUnwrap(source.slice(
            from: "private var providerToolsSection: some View",
            to: "private var manualTokenSection: some View"
        ))
        let gridRegion = try XCTUnwrap(source.slice(
            from: "private var gridColumns: [GridItem]",
            to: "private func sectionLabel"
        ))

        XCTAssertTrue(providerRegion.contains("sectionLabel(\"Cloud providers & local clusters\")"))
        XCTAssertTrue(providerRegion.contains("ForEach(AddClusterProviderIdentifier.allCases)"))
        XCTAssertEqual(AddClusterProviderIdentifier.allCases.count, 4)
        XCTAssertFalse(source.contains("sectionLabel(\"Provider Login\")"))
        XCTAssertFalse(source.contains("sectionLabel(\"Local Tools\")"))

        XCTAssertTrue(gridRegion.contains("if dynamicTypeSize.isAccessibilitySize"))
        XCTAssertTrue(gridRegion.contains("GridItem(.flexible()"))
        XCTAssertTrue(gridRegion.contains("GridItem(.adaptive(minimum: 170)"))

        let contentWidth = RuneUILayoutMetrics.addClusterPopoverWidth
            - RuneUILayoutMetrics.addClusterPopoverPadding * 2
        let defaultColumnWidth = (contentWidth - 8) / 2
        XCTAssertGreaterThanOrEqual(defaultColumnWidth, 170)
        XCTAssertLessThan((contentWidth - 16) / 3, 170)
    }

    private func makePopover(isManualTokenExpanded: Bool) -> some View {
        AddClusterPopoverView(
            kubeConfigSourceCount: 2,
            contextCount: 3,
            isLoading: false,
            externalCommandsAllowed: true,
            favoriteImportedContexts: .constant(false),
            isManualTokenExpanded: .constant(isManualTokenExpanded),
            manualContextName: .constant("development"),
            manualServerURL: .constant("https://cluster.example.invalid"),
            manualNamespace: .constant("default"),
            manualBearerToken: .constant("synthetic-token"),
            onRefresh: {},
            onImportFile: {},
            onPasteKubeconfig: {},
            onImportFolder: {},
            onUseDefaultKubeconfig: {},
            onSelectProvider: { _ in },
            onImportManualToken: {}
        )
    }

    private var addClusterPopoverSourceURL: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/AddClusterPopoverView.swift")
    }
}

private extension String {
    func slice(from start: String, to end: String) -> String? {
        guard let startRange = range(of: start),
              let endRange = range(of: end, range: startRange.upperBound..<endIndex) else {
            return nil
        }
        return String(self[startRange.lowerBound..<endRange.lowerBound])
    }
}
