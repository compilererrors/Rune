import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class RuneDisclosureSectionTests: XCTestCase {
    func testArrowNavigationOnlyChangesStateInItsUsefulDirection() {
        XCTAssertTrue(RuneDisclosureNavigation.shouldToggle(for: .left, isExpanded: true))
        XCTAssertFalse(RuneDisclosureNavigation.shouldToggle(for: .left, isExpanded: false))
        XCTAssertTrue(RuneDisclosureNavigation.shouldToggle(for: .right, isExpanded: false))
        XCTAssertFalse(RuneDisclosureNavigation.shouldToggle(for: .right, isExpanded: true))
        XCTAssertFalse(RuneDisclosureNavigation.shouldToggle(for: .up, isExpanded: false))
        XCTAssertFalse(RuneDisclosureNavigation.shouldToggle(for: .down, isExpanded: true))
        XCTAssertFalse(
            RuneDisclosureNavigation.shouldToggle(
                for: .right,
                isExpanded: false,
                isEnabled: false
            )
        )
    }

    func testRootPaneNavigationSuspendsForEveryModalAndPopoverSurface() {
        XCTAssertFalse(navigationContext().hasBlockingPresentation)
        XCTAssertFalse(navigationContext().isSuspended)

        let blockingContexts = [
            navigationContext(commandPalettePresented: true),
            navigationContext(yamlEditorSheetPresented: true),
            navigationContext(yamlManifestIsEditing: true),
            navigationContext(kubeConfigImportReviewPresented: true),
            navigationContext(addClusterPopoverPresented: true),
            navigationContext(addClusterProviderPresented: true),
            navigationContext(manualNamespaceSheetPresented: true),
            navigationContext(pendingWriteConfirmationPresented: true),
            navigationContext(launchExperiencePresented: true),
            navigationContext(appSheetAttached: true)
        ]

        XCTAssertTrue(blockingContexts.allSatisfy(\.hasBlockingPresentation))
        XCTAssertTrue(blockingContexts.allSatisfy(\.isSuspended))

        let textEditingContext = navigationContext(editableTextResponderActive: true)
        XCTAssertFalse(textEditingContext.hasBlockingPresentation)
        XCTAssertTrue(textEditingContext.isSuspended)
    }

    @MainActor
    func testDisclosureRowFillsItsProposalAndKeepsMinimumTargetAtLargeTextSizes() {
        for width in [240.0, 320.0] {
            for dynamicTypeSize in [DynamicTypeSize.large, .accessibility3] {
                let host = NSHostingView(rootView: RuneDisclosureRow(
                    "Synthetic Command Details",
                    isExpanded: false,
                    action: {}
                ) {
                    Text("Synthetic Command Details With A Deliberately Long Label")
                        .font(.caption.weight(.semibold))
                        .fixedSize(horizontal: false, vertical: true)
                }
                .environment(\.dynamicTypeSize, dynamicTypeSize)
                .frame(width: width))

                host.layoutSubtreeIfNeeded()

                XCTAssertEqual(host.fittingSize.width, width, accuracy: 0.5)
                XCTAssertGreaterThanOrEqual(
                    host.fittingSize.height,
                    RuneDisclosureMetrics.headerMinimumHeight
                )
            }
        }
    }

    @MainActor
    func testCompactControlLabelKeepsTwentyEightPointTarget() {
        let host = NSHostingView(rootView: Button(action: {}) {
            Label("All Prefixes", systemImage: "questionmark.circle")
                .font(.caption)
                .runeMinimumInteractiveTarget()
        }
        .buttonStyle(.plain)
        .controlSize(.small))

        host.layoutSubtreeIfNeeded()

        XCTAssertGreaterThanOrEqual(host.fittingSize.height, RuneUILayoutMetrics.iconButtonSize)
    }

    @MainActor
    func testDisclosurePointerTargetIncludesLeadingAndTrailingRowEdges() throws {
        var activationCount = 0
        let host = NSHostingView(rootView: RuneDisclosureRow(
            "Synthetic Command Details",
            isExpanded: false,
            action: { activationCount += 1 }
        ) {
            Text("Synthetic Command Details")
        }
        .frame(width: 320, height: RuneDisclosureMetrics.headerMinimumHeight))
        host.frame = NSRect(
            x: 0,
            y: 0,
            width: 320,
            height: RuneDisclosureMetrics.headerMinimumHeight
        )

        let window = NSWindow(
            contentRect: host.frame,
            styleMask: [.borderless],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentView = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }
        settle(host)

        try click(NSPoint(x: 3, y: 16), in: host, window: window)
        settle(host)
        XCTAssertEqual(activationCount, 1)

        try click(NSPoint(x: 317, y: 16), in: host, window: window)
        settle(host)
        XCTAssertEqual(activationCount, 2)
    }

    @MainActor
    func testDisclosureIsOneFullWidthAccessibleButtonAndPressRevealsContent() throws {
        let host = NSHostingView(rootView: RuneDisclosureSection(
            "Synthetic Command Details",
            accessibilityIdentifier: "rune.synthetic.command-details"
        ) {
            Text("Synthetic expanded command output")
                .accessibilityLabel("Synthetic expanded command output")
                .padding(.top, 8)
        } label: {
            Label("Synthetic Command Details", systemImage: "terminal")
                .font(.caption.weight(.semibold))
        }
        .frame(width: 320)
        .frame(minHeight: 120, alignment: .topLeading))
        host.frame = NSRect(x: 0, y: 0, width: 320, height: 120)

        let window = NSWindow(
            contentRect: host.frame,
            styleMask: [.borderless],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentView = host
        window.makeKeyAndOrderFront(nil)
        defer { window.orderOut(nil) }

        settle(host)

        var nodes = accessibilityNodes(in: host)
        guard !nodes.compactMap(\.label).isEmpty else {
            throw XCTSkip("SwiftUI accessibility proxies are unavailable in this host process.")
        }
        let collapsed = try XCTUnwrap(nodes.first {
            $0.label == "Synthetic Command Details" && $0.role == .button
        })
        XCTAssertEqual(collapsed.value, "Collapsed")
        XCTAssertGreaterThanOrEqual(collapsed.frame.width, 319)
        XCTAssertGreaterThanOrEqual(
            collapsed.frame.height,
            RuneDisclosureMetrics.headerMinimumHeight
        )
        XCTAssertTrue(collapsed.performPress())

        settle(host)
        nodes = accessibilityNodes(in: host)

        let expanded = try XCTUnwrap(nodes.first {
            $0.label == "Synthetic Command Details" && $0.role == .button
        })
        XCTAssertEqual(expanded.value, "Expanded")
        XCTAssertTrue(nodes.contains { $0.label == "Synthetic expanded command output" })
    }

    func testAllExpandableSurfacesUseTheSharedFullRowControl() throws {
        let root = try source("Sources/RuneUI/Views/RuneRootView.swift")
        let addCluster = try source("Sources/RuneUI/Views/AddClusterPopoverView.swift")
        let importReview = try source("Sources/RuneUI/Views/KubeConfigImportReviewPanel.swift")
        let logs = try source("Sources/RuneUI/Views/ResourceLogsInspectorView.swift")
        let overview = try source("Sources/RuneUI/Views/OverviewClusterSignalsPanelView.swift")
        let yaml = try source("Sources/RuneUI/Views/ResourceYAMLInspectorView.swift")
        let disclosure = try source("Sources/RuneUI/Layout/RuneDisclosureSection.swift")

        for sourceURL in try swiftSourceFiles(in: "Sources/RuneUI") {
            let adopter = try String(contentsOf: sourceURL, encoding: .utf8)
            XCTAssertFalse(adopter.contains("DisclosureGroup"), sourceURL.path)
            XCTAssertFalse(adopter.contains(".onTapGesture"), sourceURL.path)
        }

        XCTAssertTrue(root.contains("RuneDisclosureSection(\n                            \"Command Details\""))
        XCTAssertTrue(root.contains("RuneDisclosureSection(\n            \"Login Output\""))
        XCTAssertTrue(root.contains("rune.add-cluster.provider.command-details"))
        XCTAssertTrue(root.contains("rune.add-cluster.provider.login-output"))
        XCTAssertTrue(root.contains("isAddClusterProviderCommandDetailsExpanded = false"))
        XCTAssertTrue(root.contains("isAddClusterProviderLoginOutputExpanded = false"))
        XCTAssertTrue(addCluster.contains("RuneDisclosureSection(\n            \"Manual Token Server\""))
        XCTAssertEqual(importReview.components(separatedBy: "RuneDisclosureSection(").count - 1, 2)
        XCTAssertTrue(logs.contains("isExpanded: $isCompactSummaryExpanded"))
        XCTAssertTrue(overview.contains("RuneDisclosureRow("))
        XCTAssertTrue(yaml.contains("RuneDisclosureRow("))

        XCTAssertTrue(disclosure.contains(".contentShape(Rectangle())"))
        XCTAssertTrue(disclosure.contains(".accessibilityValue(isExpanded ? \"Expanded\" : \"Collapsed\")"))
        XCTAssertTrue(disclosure.contains(".onKeyPress(.leftArrow)"))
        XCTAssertTrue(disclosure.contains(".onKeyPress(.rightArrow)"))
        XCTAssertTrue(disclosure.contains("return .ignored"))
        XCTAssertTrue(disclosure.contains(".accessibilityHidden(true)"))
    }

    func testAuditedCompactNavigationTargetsUseSharedMinimums() throws {
        let onboarding = try source("Sources/RuneUI/Views/KubernetesConnectionOnboardingView.swift")
        let commandPalette = try source("Sources/RuneUI/Views/CommandPaletteView.swift")
        let inspectorFind = try source("Sources/RuneUI/Views/InspectorFind.swift")
        let terminal = try source("Sources/RuneUI/Views/TerminalShellPanelView.swift")
        let root = try source("Sources/RuneUI/Views/RuneRootView.swift")

        XCTAssertEqual(onboarding.components(separatedBy: ".runeMinimumInteractiveTarget()").count - 1, 2)
        XCTAssertTrue(commandPalette.contains("Label(\"All Prefixes\", systemImage: \"questionmark.circle\")\n                .runeMinimumInteractiveTarget()"))
        XCTAssertTrue(inspectorFind.contains(".runeMinimumInteractiveTarget(minWidth: 74, alignment: .trailing)"))
        XCTAssertFalse(terminal.contains("height: 26"))
        XCTAssertTrue(root.contains("RuneIconButton(\n                        \"Previous operator resource page\""))
        XCTAssertTrue(root.contains("RuneIconButton(\n                        \"Next operator resource page\""))
        XCTAssertTrue(root.contains(".frame(minHeight: RuneUILayoutMetrics.iconButtonSize)\n                .background(Capsule()"))
    }

    @MainActor
    private func settle(_ view: NSView) {
        for _ in 0..<4 {
            view.layoutSubtreeIfNeeded()
            RunLoop.main.run(until: Date().addingTimeInterval(0.04))
        }
    }

    @MainActor
    private func click(_ point: NSPoint, in view: NSView, window: NSWindow) throws {
        let location = view.convert(point, to: nil)
        let down = try XCTUnwrap(NSEvent.mouseEvent(
            with: .leftMouseDown,
            location: location,
            modifierFlags: [],
            timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber,
            context: nil,
            eventNumber: 1,
            clickCount: 1,
            pressure: 1
        ))
        let up = try XCTUnwrap(NSEvent.mouseEvent(
            with: .leftMouseUp,
            location: location,
            modifierFlags: [],
            timestamp: ProcessInfo.processInfo.systemUptime,
            windowNumber: window.windowNumber,
            context: nil,
            eventNumber: 2,
            clickCount: 1,
            pressure: 0
        ))
        window.sendEvent(down)
        window.sendEvent(up)
    }

    @MainActor
    private func accessibilityNodes(in rootView: NSView) -> [DisclosureAccessibilityNode] {
        var visited: Set<ObjectIdentifier> = []
        var result: [DisclosureAccessibilityNode] = []

        func visit(_ candidate: Any) {
            guard let object = candidate as AnyObject? else { return }
            guard visited.insert(ObjectIdentifier(object)).inserted else { return }

            if let view = candidate as? NSView {
                result.append(DisclosureAccessibilityNode(
                    label: view.accessibilityLabel(),
                    value: view.accessibilityValue() as? String,
                    role: view.accessibilityRole(),
                    frame: view.accessibilityFrame(),
                    performPress: view.accessibilityPerformPress
                ))
                view.accessibilityChildren()?.forEach(visit)
                view.subviews.forEach(visit)
            } else if let element = candidate as? NSAccessibilityElement {
                result.append(DisclosureAccessibilityNode(
                    label: element.accessibilityLabel(),
                    value: element.accessibilityValue() as? String,
                    role: element.accessibilityRole(),
                    frame: element.accessibilityFrame(),
                    performPress: element.accessibilityPerformPress
                ))
                element.accessibilityChildren()?.forEach(visit)
            }
        }

        visit(rootView)
        return result
    }

    private func source(_ relativePath: String) throws -> String {
        try String(
            contentsOf: repositoryRoot.appendingPathComponent(relativePath),
            encoding: .utf8
        )
    }

    private func swiftSourceFiles(in relativeDirectory: String) throws -> [URL] {
        let directory = repositoryRoot.appendingPathComponent(relativeDirectory, isDirectory: true)
        let keys: [URLResourceKey] = [.isRegularFileKey]
        guard let enumerator = FileManager.default.enumerator(
            at: directory,
            includingPropertiesForKeys: keys,
            options: [.skipsHiddenFiles]
        ) else {
            XCTFail("Could not enumerate \(directory.path)")
            return []
        }

        return try enumerator.compactMap { candidate in
            guard let url = candidate as? URL, url.pathExtension == "swift" else { return nil }
            return try url.resourceValues(forKeys: Set(keys)).isRegularFile == true ? url : nil
        }
    }

    private func navigationContext(
        commandPalettePresented: Bool = false,
        yamlEditorSheetPresented: Bool = false,
        yamlManifestIsEditing: Bool = false,
        kubeConfigImportReviewPresented: Bool = false,
        addClusterPopoverPresented: Bool = false,
        addClusterProviderPresented: Bool = false,
        manualNamespaceSheetPresented: Bool = false,
        pendingWriteConfirmationPresented: Bool = false,
        launchExperiencePresented: Bool = false,
        appSheetAttached: Bool = false,
        editableTextResponderActive: Bool = false
    ) -> RuneRootKeyboardNavigationContext {
        RuneRootKeyboardNavigationContext(
            commandPalettePresented: commandPalettePresented,
            yamlEditorSheetPresented: yamlEditorSheetPresented,
            yamlManifestIsEditing: yamlManifestIsEditing,
            kubeConfigImportReviewPresented: kubeConfigImportReviewPresented,
            addClusterPopoverPresented: addClusterPopoverPresented,
            addClusterProviderPresented: addClusterProviderPresented,
            manualNamespaceSheetPresented: manualNamespaceSheetPresented,
            pendingWriteConfirmationPresented: pendingWriteConfirmationPresented,
            launchExperiencePresented: launchExperiencePresented,
            appSheetAttached: appSheetAttached,
            editableTextResponderActive: editableTextResponderActive
        )
    }

    private var repositoryRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}

private struct DisclosureAccessibilityNode {
    let label: String?
    let value: String?
    let role: NSAccessibility.Role?
    let frame: NSRect
    let performPress: () -> Bool
}
