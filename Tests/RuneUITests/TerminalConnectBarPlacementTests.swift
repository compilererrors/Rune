import AppKit
import RuneCore
@testable import RuneUI
import SwiftUI
import XCTest

/// Connect and Reconnect belong to the terminal but must never sit on the
/// transcript: the transcript's text view owns the cursor (an I-beam) and
/// the click, and a prompt that moved between its centre and its bottom
/// made the action jump. The transcript keeps the same frame and nothing
/// covers it, whatever the session's state.
@MainActor
final class TerminalConnectBarPlacementTests: XCTestCase {
    private let pod = PodSummary(name: "api-0", namespace: "default", status: "Running")

    func testNothingCoversTheTranscriptAndItNeverMoves() async throws {
        var frames: [String: NSRect] = [:]
        let cases: [(String, PodTerminalSessionStatus, String)] = [
            ("empty failed", .failed, ""),
            ("finished failed", .failed, "$ ls\nbin  etc  usr\n$ exit\n"),
            ("empty disconnected", .disconnected, ""),
            ("finished disconnected", .disconnected, "$ uptime\n 10:00 up 1 day\n"),
            ("connecting", .connecting, ""),
            ("connected", .connected, "$ ls\nbin  etc  usr\n")
        ]
        for (name, status, transcript) in cases {
            let window = try await mount(session(status: status, transcript: transcript))
            defer { RuneTestWindow.close(window) }
            let scrollView = try XCTUnwrap(transcriptScrollView(in: window), name)
            let frame = scrollView.convert(scrollView.bounds, to: nil)
            frames[name] = frame

            for fx in [0.2, 0.5, 0.8] {
                for y in stride(from: 6.0, through: frame.height - 6, by: 12) {
                    let point = NSPoint(x: frame.minX + frame.width * fx, y: frame.minY + y)
                    let hit = window.contentView?.hitTest(point)
                    XCTAssertTrue(
                        hit.map { $0 === scrollView || $0.isDescendant(of: scrollView) } ?? false,
                        "\(name): \(String(describing: hit.map { type(of: $0) })) covers the transcript at \(point)"
                    )
                }
            }
        }
        let reference = try XCTUnwrap(frames["connected"])
        for (name, frame) in frames {
            XCTAssertEqual(frame, reference, "\(name): the transcript moved")
        }
    }

    func session(status: PodTerminalSessionStatus, transcript: String) -> PodTerminalSession {
        PodTerminalSession(
            id: "shell-1",
            contextName: "sample",
            namespace: "default",
            podName: pod.name,
            shell: "sh",
            transcript: transcript,
            status: status,
            lastDiagnostic: status == .failed
                ? PodTerminalSessionDiagnostic(category: .unknown, summary: "The pod stopped accepting exec.", recoveryHint: "Reconnect")
                : nil
        )
    }

    private func mount(_ session: PodTerminalSession) async throws -> NSWindow {
        let view = Self.panel(session, pod: pod)
            .padding(12)
            .frame(width: 720, height: 520, alignment: .top)

        let host = NSHostingController(rootView: view)
        host.sizingOptions = []
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 720, height: 520), styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentViewController = host
        window.setContentSize(NSSize(width: 720, height: 520))
        window.orderFront(nil)
        try await Task.sleep(nanoseconds: 500_000_000)
        return window
    }

    static func panel(_ session: PodTerminalSession, pod: PodSummary) -> TerminalShellPanelView {
        TerminalShellPanelView(
            session: session,
            sessions: [session],
            activeSessionID: session.id,
            isComposingNewSession: false,
            selectedPod: pod,
            availablePods: [pod],
            canApplyMutations: true,
            transcriptHeight: 260,
            selectedShellPodID: .constant(pod.id),
            terminalInput: .constant(""),
            onStartSession: { _, _ in },
            onReconnectSession: { _, _, _ in },
            onSend: {},
            onSendControlSequence: { _ in },
            onResizeSession: { _, _, _ in },
            onDisconnect: {},
            onSelectSession: { _ in },
            onCloseSession: { _ in },
            onComposeNewSession: {},
            onClearTranscript: {},
            onSaveActiveTranscript: {},
            onSaveAllTranscripts: {},
            onSaveActiveTranscriptToExportFolder: {},
            onSaveActiveTranscriptAndOpen: {},
            onSaveAllTranscriptsToExportFolder: {},
            onSaveAllTranscriptsAndOpen: {},
            isFavoritePod: { _ in false },
            onToggleFavoritePod: { _ in }
        )
    }

    private func transcriptScrollView(in window: NSWindow) -> NSScrollView? {
        func find(_ view: NSView) -> NSScrollView? {
            if let scrollView = view as? NSScrollView,
               let textView = scrollView.documentView as? NSTextView,
               String(describing: type(of: textView)).contains("TerminalTranscript") {
                return scrollView
            }
            for subview in view.subviews {
                if let found = find(subview) { return found }
            }
            return nil
        }
        return window.contentView.flatMap(find)
    }
}
