import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class OverviewStatCardTests: XCTestCase {
    func testCompactAndDefaultCardsRenderAtGridWidthsWithoutGeometryDrift() throws {
        let compact = renderHost(card(), width: 160)
        let regular = renderHost(card(), width: 240)

        XCTAssertEqual(compact.frame.width, 160, accuracy: 0.5)
        XCTAssertEqual(regular.frame.width, 240, accuracy: 0.5)
        XCTAssertGreaterThanOrEqual(compact.frame.height, 80)
        XCTAssertLessThanOrEqual(compact.frame.height, 130)
        XCTAssertEqual(compact.frame.height, regular.frame.height, accuracy: 0.5)
        XCTAssertGreaterThan(try renderedPNG(from: compact).count, 1_500)
        XCTAssertGreaterThan(try renderedPNG(from: regular).count, 1_500)
    }

    func testLoadingAndRefreshingStatesRenderWithoutMaterialHeightShift() throws {
        let idle = renderHost(card(count: 0), width: 160)
        let loading = renderHost(card(count: 0, isLoading: true), width: 160)
        let refreshing = renderHost(card(count: 12, isLoading: true), width: 160)

        XCTAssertLessThanOrEqual(abs(idle.frame.height - loading.frame.height), 6)
        XCTAssertEqual(idle.frame.width, loading.frame.width, accuracy: 0.5)
        XCTAssertTrue(descendants(of: loading).contains { $0 is NSProgressIndicator })
        XCTAssertTrue(descendants(of: refreshing).contains { $0 is NSProgressIndicator })
        XCTAssertGreaterThan(try renderedPNG(from: loading).count, 1_500)
        XCTAssertGreaterThan(try renderedPNG(from: refreshing).count, 1_500)
    }

    func testKeyboardFocusChangesRenderedChromeWithoutChangingGeometry() throws {
        let idle = renderHost(card(isKeyboardFocused: false), width: 160)
        let focused = renderHost(card(isKeyboardFocused: true), width: 160)

        XCTAssertEqual(idle.frame.size, focused.frame.size)
        XCTAssertNotEqual(
            try renderedPNG(from: idle),
            try renderedPNG(from: focused),
            "Keyboard focus must remain visibly distinguishable."
        )
    }

    func testCardExposesOneNamedButtonWithCountAndInvokableAction() throws {
        var openCount = 0
        let readyCard = OverviewStatCard(
            title: "Pods",
            count: 12,
            symbol: "cube.box.fill",
            tint: .cyan,
            showsHoverHelp: true,
            onOpen: { openCount += 1 }
        )
        let loadingCard = OverviewStatCard(
            title: "Pods",
            count: 0,
            symbol: "cube.box.fill",
            tint: .cyan,
            isLoading: true,
            showsHoverHelp: true,
            onOpen: {}
        )
        let refreshingCard = OverviewStatCard(
            title: "Pods",
            count: 12,
            symbol: "cube.box.fill",
            tint: .cyan,
            isLoading: true,
            showsHoverHelp: true,
            onOpen: {}
        )

        XCTAssertEqual(readyCard.accessibilityValueText, "12")
        XCTAssertEqual(loadingCard.accessibilityValueText, "Loading")
        XCTAssertEqual(refreshingCard.accessibilityValueText, "12, refreshing")
        readyCard.onOpen()
        XCTAssertEqual(openCount, 1)

        let componentSource = try String(
            contentsOf: repoRoot.appendingPathComponent("Sources/RuneUI/Views/OverviewStatCard.swift"),
            encoding: .utf8
        )
        XCTAssertTrue(componentSource.contains(".accessibilityElement(children: .ignore)"))
        XCTAssertTrue(componentSource.contains(".accessibilityLabel(title)"))
        XCTAssertTrue(componentSource.contains(".accessibilityValue(accessibilityValueText)"))
        XCTAssertTrue(componentSource.contains(".accessibilityAddTraits(isKeyboardFocused ? .isSelected : [])"))

        let host = renderHost(readyCard.runeAppearanceTheme(RuneAppearanceTheme.native.resolvedTheme), width: 160)
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

        host.layoutSubtreeIfNeeded()
        RunLoop.main.run(until: Date().addingTimeInterval(0.02))

        let nodes = accessibilityNodes(in: host)
        if !nodes.compactMap(\.label).isEmpty {
            let buttons = nodes.filter {
                $0.label == "Pods" && $0.role == .button
            }
            let button = try XCTUnwrap(buttons.first)
            XCTAssertEqual(buttons.count, 1)
            XCTAssertEqual(button.value, "12")
            XCTAssertTrue(button.performPress())
            XCTAssertEqual(openCount, 2)
        }
    }

    func testRootKeepsOverviewModuleOrderingAndNavigationWhileUsingCardComponent() throws {
        let source = try String(
            contentsOf: repoRoot.appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift"),
            encoding: .utf8
        )

        XCTAssertEqual(source.components(separatedBy: "OverviewStatCard(").count - 1, 1)
        XCTAssertTrue(source.contains("private func overviewStatCard(module: OverviewModule, index: Int)"))
        XCTAssertTrue(source.contains(
            "var modules: [OverviewModule] = [.pods, .deployments, .services, .ingresses, .configMaps, .cronJobs, .nodes]"
        ))
        XCTAssertTrue(source.contains("modules.append(.events)"))
        XCTAssertTrue(source.contains("overviewCardSelectionIndex = index"))
        XCTAssertTrue(source.contains("viewModel.openOverviewModule(module)"))
        XCTAssertTrue(source.contains("RuneBalancedOverviewGrid("))
    }

    private func card(
        count: Int = 12,
        isLoading: Bool = false,
        isKeyboardFocused: Bool = false,
        onOpen: @escaping () -> Void = {}
    ) -> some View {
        OverviewStatCard(
            title: "Pods",
            count: count,
            symbol: "cube.box.fill",
            tint: .cyan,
            isLoading: isLoading,
            isKeyboardFocused: isKeyboardFocused,
            showsHoverHelp: true,
            onOpen: onOpen
        )
        .runeAppearanceTheme(RuneAppearanceTheme.native.resolvedTheme)
    }

    private func renderHost<Content: View>(_ content: Content, width: CGFloat) -> NSView {
        let host = NSHostingView(rootView: content.frame(width: width))
        host.layoutSubtreeIfNeeded()
        host.frame = NSRect(origin: .zero, size: host.fittingSize)
        host.layoutSubtreeIfNeeded()
        return host
    }

    private func renderedPNG(from view: NSView) throws -> Data {
        view.layoutSubtreeIfNeeded()
        let bitmap = try XCTUnwrap(view.bitmapImageRepForCachingDisplay(in: view.bounds))
        view.cacheDisplay(in: view.bounds, to: bitmap)
        return try XCTUnwrap(bitmap.representation(using: .png, properties: [:]))
    }

    private func descendants(of view: NSView) -> [NSView] {
        view.subviews.flatMap { [$0] + descendants(of: $0) }
    }

    private func accessibilityNodes(in rootView: NSView) -> [AccessibilityNode] {
        var visited: Set<ObjectIdentifier> = []
        var result: [AccessibilityNode] = []

        func visit(_ candidate: Any) {
            guard let object = candidate as AnyObject? else { return }
            guard visited.insert(ObjectIdentifier(object)).inserted else { return }

            if let view = candidate as? NSView {
                result.append(AccessibilityNode(
                    label: view.accessibilityLabel(),
                    value: view.accessibilityValue() as? String,
                    role: view.accessibilityRole(),
                    performPress: view.accessibilityPerformPress
                ))
                view.accessibilityChildren()?.forEach(visit)
                view.subviews.forEach(visit)
            } else if let element = candidate as? NSAccessibilityElement {
                result.append(AccessibilityNode(
                    label: element.accessibilityLabel(),
                    value: element.accessibilityValue() as? String,
                    role: element.accessibilityRole(),
                    performPress: element.accessibilityPerformPress
                ))
                element.accessibilityChildren()?.forEach(visit)
            }
        }

        visit(rootView)
        return result
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}

private struct AccessibilityNode {
    let label: String?
    let value: String?
    let role: NSAccessibility.Role?
    let performPress: () -> Bool
}
