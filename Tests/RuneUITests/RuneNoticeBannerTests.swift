import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

final class RuneNoticeBannerTests: XCTestCase {
    private let savedFileURL = URL(fileURLWithPath: "/tmp/synthetic-logs/session.log")

    @MainActor
    func testSavedFileActionsFitNarrowAndEnlargedTextBanners() throws {
        for (width, fontSize) in [(320.0, 12.0), (320.0, 20.0), (520.0, 12.0)] {
            let notice = makeNotice(savedFileURL: savedFileURL)
            let host = makeHost(notice: notice, width: width, fontSize: fontSize, onOpenFolder: { _ in }, onOpenFile: { _ in })
            let plainHost = makeHost(notice: notice, width: width, fontSize: fontSize)
            let window = show(host)
            defer { window.orderOut(nil) }

            XCTAssertEqual(host.fittingSize.width, width, accuracy: 0.5)
            XCTAssertGreaterThanOrEqual(host.fittingSize.height, plainHost.fittingSize.height + RuneUILayoutMetrics.iconButtonSize)
            let bounds = window.convertToScreen(host.convert(host.bounds, to: nil))
            for button in accessibilityNodes(in: host).filter({ $0.identifier?.hasPrefix("saved-log-") == true }) {
                XCTAssertGreaterThanOrEqual(button.frame.minX, bounds.minX)
                XCTAssertLessThanOrEqual(button.frame.maxX, bounds.maxX)
                XCTAssertGreaterThanOrEqual(button.frame.minY, bounds.minY)
                XCTAssertLessThanOrEqual(button.frame.maxY, bounds.maxY)
                XCTAssertGreaterThanOrEqual(button.frame.height, RuneUILayoutMetrics.iconButtonSize)
            }

            let png = try renderedPNG(from: host)
            XCTAssertGreaterThan(png.count, 1_000)
            if let path = ProcessInfo.processInfo.environment["RUNE_NOTICE_SNAPSHOT_DIR"] {
                let directory = URL(fileURLWithPath: path, isDirectory: true)
                try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
                try png.write(to: directory.appendingPathComponent("saved-log-notice-\(Int(width))-font-\(Int(fontSize)).png"))
            }
        }
    }

    @MainActor
    func testNoticesWithoutSavedFileOrCallbacksDoNotReserveActionSpace() {
        for severity in [RuneUserNoticeSeverity.info, .warning, .error] {
            let plain = makeNotice(severity: severity)
            let baseline = makeHost(notice: plain)
            let callbacksWithoutFile = makeHost(notice: plain, onOpenFolder: { _ in }, onOpenFile: { _ in })
            let fileWithoutCallbacks = makeHost(notice: makeNotice(severity: severity, savedFileURL: savedFileURL))
            XCTAssertEqual(callbacksWithoutFile.fittingSize.height, baseline.fittingSize.height, accuracy: 0.5)
            XCTAssertEqual(fileWithoutCallbacks.fittingSize.height, baseline.fittingSize.height, accuracy: 0.5)
        }
    }

    @MainActor
    func testSavedFileButtonsInvokeTheirOwnCallbackAndKeepDismissIndependent() throws {
        var openedFolders: [URL] = []
        var openedFiles: [URL] = []
        var dismissCount = 0
        let host = makeHost(
            notice: makeNotice(savedFileURL: savedFileURL),
            width: 320,
            fontSize: 20,
            onOpenFolder: { openedFolders.append($0) },
            onOpenFile: { openedFiles.append($0) },
            onDismiss: { dismissCount += 1 }
        )
        let window = show(host)
        defer { window.orderOut(nil) }
        let nodes = accessibilityNodes(in: host)
        guard nodes.contains(where: { $0.role == .button }) else {
            throw XCTSkip("SwiftUI accessibility button proxies are unavailable in this host process.")
        }
        let folderButtons = nodes.filter { $0.identifier == "saved-log-open-folder" && $0.role == .button }
        let fileButtons = nodes.filter { $0.identifier == "saved-log-open-file" && $0.role == .button }
        XCTAssertEqual(folderButtons.count, 1)
        XCTAssertEqual(fileButtons.count, 1)
        XCTAssertTrue(try XCTUnwrap(folderButtons.first).performPress())
        XCTAssertEqual(openedFolders, [savedFileURL])
        XCTAssertTrue(openedFiles.isEmpty)
        XCTAssertEqual(dismissCount, 0)
        XCTAssertTrue(try XCTUnwrap(fileButtons.first).performPress())
        XCTAssertEqual(openedFiles, [savedFileURL])
        XCTAssertEqual(openedFolders, [savedFileURL])
        XCTAssertEqual(dismissCount, 0)
        let dismiss = try XCTUnwrap(nodes.first { $0.label == "Dismiss notice" && $0.role == .button })
        XCTAssertTrue(dismiss.performPress())
        XCTAssertEqual(dismissCount, 1)
    }

    private func makeNotice(severity: RuneUserNoticeSeverity = .info, savedFileURL: URL? = nil) -> RuneUserNotice {
        RuneUserNotice(
            severity: severity,
            title: "Logs saved",
            message: "Saved session.log in the configured export folder.",
            savedFileURL: savedFileURL
        )
    }

    @MainActor
    private func makeHost(
        notice: RuneUserNotice,
        width: CGFloat = 320,
        fontSize: Double = 12,
        onOpenFolder: ((URL) -> Void)? = nil,
        onOpenFile: ((URL) -> Void)? = nil,
        onDismiss: @escaping () -> Void = {}
    ) -> NSHostingView<AnyView> {
        let host = NSHostingView(rootView: AnyView(
            RuneNoticeBanner(notice: notice, onOpenFolder: onOpenFolder, onOpenFile: onOpenFile, onDismiss: onDismiss)
                .runeInterfaceTypography(configuredFontSize: fontSize, systemDynamicTypeSize: fontSize > 12 ? .accessibility3 : .large)
                .runeAppearanceTheme(RuneAppearanceTheme.graphiteBlue.resolvedTheme)
                .frame(width: width)
        ))
        host.layoutSubtreeIfNeeded()
        host.setFrameSize(host.fittingSize)
        return host
    }

    @MainActor
    private func show(_ host: NSView) -> NSWindow {
        let window = NSWindow(contentRect: host.frame, styleMask: [.borderless], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentView = host
        window.makeKeyAndOrderFront(nil)
        for _ in 0..<4 {
            host.layoutSubtreeIfNeeded()
            RunLoop.main.run(until: Date().addingTimeInterval(0.04))
        }
        return window
    }

    @MainActor
    private func renderedPNG(from view: NSView) throws -> Data {
        let representation = try XCTUnwrap(view.bitmapImageRepForCachingDisplay(in: view.bounds))
        view.cacheDisplay(in: view.bounds, to: representation)
        return try XCTUnwrap(representation.representation(using: .png, properties: [:]))
    }

    @MainActor
    private func accessibilityNodes(in rootView: NSView) -> [NoticeAccessibilityNode] {
        var visited: Set<ObjectIdentifier> = []
        var result: [NoticeAccessibilityNode] = []
        func visit(_ candidate: Any) {
            guard let object = candidate as AnyObject?, visited.insert(ObjectIdentifier(object)).inserted else { return }
            if let view = candidate as? NSView {
                result.append(.init(identifier: view.accessibilityIdentifier(), label: view.accessibilityLabel(), role: view.accessibilityRole(), frame: view.accessibilityFrame(), performPress: view.accessibilityPerformPress))
                view.accessibilityChildren()?.forEach(visit)
                view.subviews.forEach(visit)
            } else if let element = candidate as? NSAccessibilityElement {
                result.append(.init(identifier: element.accessibilityIdentifier(), label: element.accessibilityLabel(), role: element.accessibilityRole(), frame: element.accessibilityFrame(), performPress: element.accessibilityPerformPress))
                element.accessibilityChildren()?.forEach(visit)
            }
        }
        visit(rootView)
        return result
    }
}

@MainActor
private struct NoticeAccessibilityNode {
    let identifier: String?
    let label: String?
    let role: NSAccessibility.Role?
    let frame: NSRect
    let performPress: () -> Bool
}
