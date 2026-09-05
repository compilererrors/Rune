import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class RunePendingWriteConfirmationModifierTests: XCTestCase {
    func testRootUsesOneSharedPendingWritePresentationForMainAndEditorSurfaces() throws {
        let rootSource = try String(contentsOfFile: runeRootViewPath, encoding: .utf8)
        let modifierSource = try String(contentsOfFile: modifierPath, encoding: .utf8)

        XCTAssertEqual(
            rootSource.components(separatedBy: ".runePendingWriteConfirmation(").count - 1,
            2,
            "The main window and YAML sheet should share the same pending-write presentation."
        )
        XCTAssertFalse(rootSource.contains(".confirmationDialog("))
        XCTAssertTrue(rootSource.contains("showsCopyCommandAction: !viewModel.pendingWriteActionKubectlCommand.isEmpty"))
        XCTAssertTrue(rootSource.contains("onConfirm: confirmPendingWriteActionFromDialog"))
        XCTAssertTrue(rootSource.contains("onCancel: cancelPendingWriteActionFromDialog"))
        XCTAssertTrue(rootSource.contains("onCopyCommand: viewModel.copyPendingWriteActionKubectlCommand"))

        XCTAssertFalse(modifierSource.contains(".confirmationDialog("), "Preview actions must not implicitly dismiss the pending operation.")
        XCTAssertTrue(modifierSource.contains("content.sheet(isPresented:"))
        XCTAssertTrue(rootSource.contains("isPresented: rootPendingWriteActionPresentedBinding"))
    }

    @MainActor
    func testCopyKeepsOperationAvailableAndNeverConfirmsOrCancels() async throws {
        try requireSwiftUIAccessibility()
        var copied = 0
        var confirmed = 0
        var cancelled = 0
        let rendered = hostSheet(
            onConfirm: { confirmed += 1 },
            onCancel: { cancelled += 1 },
            onCopy: { copied += 1 }
        )
        defer { rendered.window.close() }
        let host = rendered.host
        let copy = try XCTUnwrap(buttons(in: host).first { $0.label == "Copy command" })
        XCTAssertTrue(copy.press())
        await Task.yield()
        XCTAssertEqual(copied, 1)
        XCTAssertEqual(confirmed, 0)
        XCTAssertEqual(cancelled, 0)

        let confirm = try XCTUnwrap(buttons(in: host).first { $0.identifier == "rune.write-review.confirm" })
        XCTAssertTrue(confirm.press())
        await Task.yield()
        XCTAssertEqual(confirmed, 1)
        XCTAssertEqual(cancelled, 0)
        _ = confirm.press()
        await Task.yield()
        XCTAssertEqual(confirmed, 1, "A rapid second click must not confirm a second production step.")
        try await Task.sleep(for: .seconds(NSEvent.doubleClickInterval + 0.15))
        let nextConfirm = try XCTUnwrap(buttons(in: host).first { $0.identifier == "rune.write-review.confirm" })
        XCTAssertTrue(nextConfirm.press())
        await Task.yield()
        XCTAssertEqual(confirmed, 2, "A deliberate later confirmation must remain available.")
    }

    @MainActor
    func testCancelOnlyCancelsAndCopyCanBeOmitted() async throws {
        try requireSwiftUIAccessibility()
        var confirmed = 0
        var cancelled = 0
        let rendered = hostSheet(
            showsCopy: false,
            onConfirm: { confirmed += 1 },
            onCancel: { cancelled += 1 }
        )
        defer { rendered.window.close() }
        let host = rendered.host
        XCTAssertFalse(buttons(in: host).contains { $0.label == "Copy command" })
        let cancel = try XCTUnwrap(buttons(in: host).first { $0.label == "Cancel" })
        XCTAssertTrue(cancel.press())
        await Task.yield()
        XCTAssertEqual(cancelled, 1)
        XCTAssertEqual(confirmed, 0)
    }

    @MainActor
    func testLongReviewStaysBoundedAtCompactAndLargeTextSizes() throws {
        for width in [420.0, 560.0] {
            for textSize in [DynamicTypeSize.large, .accessibility3] {
                let rendered = hostSheet(width: width, textSize: textSize, longContent: true)
                defer { rendered.window.close() }
                let host = rendered.host
                XCTAssertEqual(host.fittingSize.width, width, accuracy: 1)
                XCTAssertLessThanOrEqual(host.fittingSize.height, RuneUILayoutMetrics.providerDialogMaxHeight + 1)
                if let artifactPath = ProcessInfo.processInfo.environment["RUNE_WRITE_REVIEW_ARTIFACT_DIR"] {
                    let directory = URL(fileURLWithPath: artifactPath, isDirectory: true)
                    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
                    let bitmap = try XCTUnwrap(host.bitmapImageRepForCachingDisplay(in: host.bounds))
                    host.cacheDisplay(in: host.bounds, to: bitmap)
                    let png = try XCTUnwrap(bitmap.representation(using: .png, properties: [:]))
                    try png.write(to: directory.appendingPathComponent("review-\(Int(width))-\(textSize).png"))
                }
            }
        }
    }

    @MainActor
    func testLongReviewExposesActionsAtCompactAndLargeTextSizes() throws {
        try requireSwiftUIAccessibility()
        for width in [420.0, 560.0] {
            for textSize in [DynamicTypeSize.large, .accessibility3] {
                let rendered = hostSheet(width: width, textSize: textSize, longContent: true)
                defer { rendered.window.close() }
                XCTAssertTrue(buttons(in: rendered.host).contains { $0.identifier == "rune.write-review.confirm" })
                XCTAssertTrue(buttons(in: rendered.host).contains { $0.label == "Cancel" })
            }
        }
    }

    @MainActor
    func testSharedDialogActionsStackInsteadOfClippingLongLabels() {
        func actionBar(width: CGFloat) -> NSHostingView<some View> {
            NSHostingView(rootView: RuneDialogActionBar {
                Button {} label: { RuneDialogButtonLabel("Cancel this operation") }
                Button {} label: { RuneDialogButtonLabel("Confirm changes to selected resources") }
            }
            .frame(width: width))
        }
        let compact = actionBar(width: 240)
        let wide = actionBar(width: 760)
        compact.layoutSubtreeIfNeeded()
        wide.layoutSubtreeIfNeeded()
        XCTAssertEqual(compact.fittingSize.width, 240, accuracy: 1)
        XCTAssertGreaterThan(compact.fittingSize.height, wide.fittingSize.height + 10)
    }

    @MainActor
    private func hostSheet(
        width: CGFloat = 560,
        textSize: DynamicTypeSize = .large,
        longContent: Bool = false,
        showsCopy: Bool = true,
        onConfirm: @escaping () -> Void = {},
        onCancel: @escaping () -> Void = {},
        onCopy: @escaping () -> Void = {}
    ) -> (host: NSView, window: NSWindow) {
        let message = "Update the selected deployment in the synthetic namespace. Review the affected resources before continuing."
        let host = NSHostingView(rootView: RunePendingWriteConfirmationSheet(
            title: "Review deployment update",
            confirmLabel: "Confirm update",
            isDestructive: false,
            message: longContent ? Array(repeating: message, count: 20).joined(separator: "\n\n") : message,
            targetSummary: "Context: synthetic-context\nNamespace: synthetic-namespace",
            commandPreview: "kubectl --context synthetic-context --namespace synthetic-namespace scale deployment synthetic-deployment --replicas=3",
            showsCopyCommandAction: showsCopy,
            onConfirm: onConfirm,
            onCancel: onCancel,
            onCopyCommand: onCopy
        )
        .environment(\.dynamicTypeSize, textSize)
        .runeInterfaceTypography(configuredFontSize: 13, systemDynamicTypeSize: textSize)
        .environment(\.colorScheme, .light)
        .background(Color(nsColor: .windowBackgroundColor))
        .frame(width: width))
        host.appearance = NSAppearance(named: .aqua)
        host.layoutSubtreeIfNeeded()
        host.frame = NSRect(origin: .zero, size: host.fittingSize)
        let window = NSWindow(contentRect: host.frame, styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentView = host
        window.makeKeyAndOrderFront(nil)
        for _ in 0..<4 {
            host.layoutSubtreeIfNeeded()
            RunLoop.main.run(until: Date().addingTimeInterval(0.04))
        }
        window.displayIfNeeded()
        return (host, window)
    }

    @MainActor
    private func requireSwiftUIAccessibility() throws {
        // Probe a standard SwiftUI button separately: absence of a specific
        // review action must fail, not be mistaken for unavailable host AX.
        let probe = NSHostingView(rootView: Button("Synthetic accessibility probe") {}.frame(width: 260, height: 50))
        probe.frame = NSRect(x: 0, y: 0, width: 260, height: 50)
        let window = NSWindow(contentRect: probe.frame, styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentView = probe
        window.makeKeyAndOrderFront(nil)
        defer { window.close() }
        for _ in 0..<4 {
            probe.layoutSubtreeIfNeeded()
            RunLoop.main.run(until: Date().addingTimeInterval(0.04))
        }
        guard buttons(in: probe).contains(where: { $0.label == "Synthetic accessibility probe" }) else {
            throw XCTSkip("The host does not expose accessibility proxies for a standard SwiftUI button; verify review actions in the macOS accessibility smoke test.")
        }
    }

    @MainActor
    private func buttons(in root: NSView) -> [ReviewButton] {
        var visited = Set<ObjectIdentifier>()
        var result: [ReviewButton] = []
        func visit(_ candidate: Any) {
            let object = candidate as AnyObject
            guard visited.insert(ObjectIdentifier(object)).inserted else { return }
            if let view = candidate as? NSView {
                if view.accessibilityRole() == .button {
                    result.append(.init(label: view.accessibilityLabel(), identifier: view.accessibilityIdentifier(), press: view.accessibilityPerformPress))
                }
                view.accessibilityChildren()?.forEach(visit)
                view.subviews.forEach(visit)
            } else if let element = candidate as? NSAccessibilityElement {
                if element.accessibilityRole() == .button {
                    result.append(.init(label: element.accessibilityLabel(), identifier: element.accessibilityIdentifier(), press: element.accessibilityPerformPress))
                }
                element.accessibilityChildren()?.forEach(visit)
            }
        }
        visit(root)
        return result
    }

    private var runeRootViewPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift").path
    }

    private var modifierPath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/RunePendingWriteConfirmationModifier.swift").path
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}

@MainActor
private struct ReviewButton {
    let label: String?
    let identifier: String?
    let press: () -> Bool
}
