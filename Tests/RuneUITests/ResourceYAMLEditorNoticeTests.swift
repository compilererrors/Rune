import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class ResourceYAMLEditorNoticeTests: XCTestCase {
    func testOperationFailureAppearsInsideOpenEditorAndCanBeDismissedWithoutClosing() async throws {
        try requireSwiftUIAccessibility()
        let state = RuneAppState()
        var closeCount = 0
        let host = NSHostingView(rootView: EditorNoticeHarness(state: state) { closeCount += 1 })
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 980, height: 760),
            styleMask: [.titled], backing: .buffered, defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentView = host
        window.makeKeyAndOrderFront(nil)
        defer { window.close() }

        state.setError(RuneError.invalidInput(message: "Synthetic apply failed after confirmation"))
        try await waitUntil {
            host.layoutSubtreeIfNeeded()
            return self.elements(in: host).contains {
                $0.text.contains("Synthetic apply failed after confirmation")
            }
        }
        let dismiss = try XCTUnwrap(elements(in: host).first { $0.text == "Dismiss notice" })
        XCTAssertTrue(dismiss.press())
        try await waitUntil { state.activeNotice == nil }

        XCTAssertNil(state.lastError)
        XCTAssertEqual(closeCount, 0)
        XCTAssertTrue(window.isVisible)
    }

    func testEditorPresentationReceivesSharedNoticeAndDismissalFromAppState() throws {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
        let source = try String(
            contentsOf: root.appendingPathComponent("Sources/RuneUI/Views/RuneRootView.swift"),
            encoding: .utf8
        )
        let start = try XCTUnwrap(source.range(of: "private func yamlManifestEditorSheet()"))
        let initializer = source[start.lowerBound...].prefix(4_000)
        XCTAssertTrue(initializer.contains("notice: viewModel.state.activeNotice,"))
        XCTAssertTrue(initializer.contains("onDismissNotice: { viewModel.state.clearError() },"))
    }

    private func requireSwiftUIAccessibility() throws {
        // Host capability is measured on an unrelated standard control. A missing
        // notice or dismiss action in the editor still fails the regression.
        let probe = NSHostingView(rootView: Button("Synthetic editor accessibility probe") {}.frame(width: 280, height: 50))
        probe.frame = NSRect(x: 0, y: 0, width: 280, height: 50)
        let window = NSWindow(contentRect: probe.frame, styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentView = probe
        window.makeKeyAndOrderFront(nil)
        defer { window.close() }
        for _ in 0..<4 {
            probe.layoutSubtreeIfNeeded()
            RunLoop.main.run(until: Date().addingTimeInterval(0.04))
        }
        guard elements(in: probe).contains(where: {
            $0.role == .button && $0.text == "Synthetic editor accessibility probe"
        }) else {
            throw XCTSkip("The host does not expose accessibility proxies for a standard SwiftUI button; verify editor notice dismissal in the macOS accessibility smoke test.")
        }
    }

    private func waitUntil(_ condition: () -> Bool) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(3))
        while !condition() {
            guard ContinuousClock.now < deadline else {
                throw RuneError.invalidInput(message: "Synthetic editor notice did not appear")
            }
            try await Task.sleep(for: .milliseconds(10))
        }
    }

    private func elements(in root: NSView) -> [NoticeElement] {
        var visited = Set<ObjectIdentifier>()
        var result: [NoticeElement] = []
        func visit(_ candidate: Any) {
            guard visited.insert(ObjectIdentifier(candidate as AnyObject)).inserted else { return }
            if let view = candidate as? NSView {
                result.append(.init(
                    role: view.accessibilityRole(),
                    text: view.accessibilityLabel() ?? (view.accessibilityValue() as? String) ?? "",
                    press: view.accessibilityPerformPress
                ))
                view.accessibilityChildren()?.forEach(visit)
                view.subviews.forEach(visit)
            } else if let element = candidate as? NSAccessibilityElement {
                result.append(.init(
                    role: element.accessibilityRole(),
                    text: element.accessibilityLabel() ?? (element.accessibilityValue() as? String) ?? "",
                    press: element.accessibilityPerformPress
                ))
                element.accessibilityChildren()?.forEach(visit)
            }
        }
        visit(root)
        return result
    }
}

@MainActor
private struct NoticeElement {
    let role: NSAccessibility.Role?
    let text: String
    let press: () -> Bool
}

@MainActor
private struct EditorNoticeHarness: View {
    @ObservedObject var state: RuneAppState
    let onClose: () -> Void

    var body: some View {
        ResourceYAMLEditorSheetView(
            resourceReference: "configmap/synthetic-config",
            yamlText: .constant("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: synthetic-config\n"),
            yamlFooterText: "Synthetic manifest",
            canApplyMutations: true,
            hasUnsavedEdits: true,
            validationIssues: [],
            isValidating: false,
            canUndoEdit: false,
            canReapplySnapshot: false,
            canDryRun: false,
            isRunningDryRun: false,
            dryRunStatus: nil,
            onApply: {}, onDryRun: {}, onReapplySnapshot: {}, onUndoEdit: {}, onRevert: {},
            onImport: {}, onExport: {}, onExportToExportFolder: {}, onExportAndOpen: {},
            onClose: onClose,
            notice: state.activeNotice,
            onDismissNotice: { state.clearError() }
        )
    }
}
