import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class TerminalPromptRenderingTests: XCTestCase {
    func testLayoutMetricsCoverEverySupportedTerminalFontSize() {
        let supportedSizes = stride(
            from: Int(RuneSettingsKeys.terminalFontSizeMinimum),
            through: Int(RuneSettingsKeys.terminalFontSizeMaximum),
            by: 1
        ).map(CGFloat.init)

        var previousControlHeight: CGFloat = 0
        for fontSize in supportedSizes {
            let metrics = TerminalPromptLayoutMetrics(fontSize: fontSize)

            XCTAssertEqual(metrics.fontSize, fontSize)
            XCTAssertGreaterThanOrEqual(metrics.lineHeight, 1)
            XCTAssertEqual(metrics.textContainerHeight, metrics.lineHeight)
            XCTAssertGreaterThanOrEqual(
                metrics.controlHeight,
                metrics.lineHeight + TerminalPromptLayoutMetrics.verticalInset * 2
            )
            XCTAssertGreaterThanOrEqual(metrics.controlHeight, TerminalPromptLayoutMetrics.minimumControlHeight)
            XCTAssertGreaterThanOrEqual(metrics.controlHeight, previousControlHeight)
            previousControlHeight = metrics.controlHeight
        }
    }

    func testPromptRendersWithoutClippingAtMinimumDefaultAndMaximumSizesInLightAndDarkAppearances() async throws {
        let appearances: [(name: String, value: NSAppearance.Name)] = [
            ("light", .aqua),
            ("dark", .darkAqua)
        ]
        let fontSizes = [
            CGFloat(RuneSettingsKeys.terminalFontSizeMinimum),
            CGFloat(RuneSettingsKeys.terminalFontSizeDefault),
            CGFloat(RuneSettingsKeys.terminalFontSizeMaximum)
        ]

        for appearance in appearances {
            for fontSize in fontSizes {
                for isEnabled in [true, false] {
                    let metrics = TerminalPromptLayoutMetrics(fontSize: fontSize)
                    let (host, window) = makePromptHost(
                        fontSize: fontSize,
                        isEnabled: isEnabled,
                        appearance: appearance.value
                    )
                    defer { window.orderOut(nil) }

                    try await settle(window: window)

                    let scrollView = try XCTUnwrap(findPromptScrollView(in: host.view))
                    let textView = try XCTUnwrap(scrollView.documentView as? NSTextView)
                    let textContainer = try XCTUnwrap(textView.textContainer)
                    let layoutManager = try XCTUnwrap(textView.layoutManager)
                    let caseName = "\(appearance.name), \(Int(fontSize))pt, \(isEnabled ? "enabled" : "disabled")"

                    XCTAssertEqual(scrollView.bounds.height, metrics.controlHeight, accuracy: 0.5, caseName)
                    XCTAssertEqual(textView.frame.height, metrics.controlHeight, accuracy: 0.5, caseName)
                    XCTAssertEqual(textContainer.containerSize.height, metrics.textContainerHeight, accuracy: 0.5, caseName)
                    XCTAssertEqual(try XCTUnwrap(textView.font).pointSize, metrics.fontSize, accuracy: 0.01, caseName)
                    XCTAssertEqual(textView.isEditable, isEnabled, caseName)
                    XCTAssertTrue(textView.isSelectable, caseName)

                    let textColor = try XCTUnwrap(textView.textColor)
                    textView.effectiveAppearance.performAsCurrentDrawingAppearance {
                        XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(textColor, over: .textBackgroundColor), 4.5, caseName)
                    }
                    XCTAssertEqual(
                        textView.selectedTextAttributes[.foregroundColor] as? NSColor,
                        TerminalPromptPalette.selectedInputTextColor,
                        caseName
                    )
                    XCTAssertEqual(
                        textView.selectedTextAttributes[.backgroundColor] as? NSColor,
                        TerminalPromptPalette.selectionBackgroundColor,
                        caseName
                    )
                    XCTAssertEqual(textView.insertionPointColor, TerminalPromptPalette.insertionPointColor, caseName)

                    layoutManager.ensureLayout(for: textContainer)
                    let glyphRange = layoutManager.glyphRange(for: textContainer)
                    let glyphRect = layoutManager
                        .boundingRect(forGlyphRange: glyphRange, in: textContainer)
                        .offsetBy(dx: textView.textContainerOrigin.x, dy: textView.textContainerOrigin.y)

                    XCTAssertGreaterThan(glyphRange.length, 0, caseName)
                    XCTAssertGreaterThan(glyphRect.height, 0, caseName)
                    XCTAssertGreaterThanOrEqual(glyphRect.minY, textView.bounds.minY - 0.5, caseName)
                    XCTAssertLessThanOrEqual(glyphRect.maxY, textView.bounds.maxY + 0.5, caseName)
                }
            }
        }
    }

    private func makePromptHost(
        fontSize: CGFloat,
        isEnabled: Bool,
        appearance: NSAppearance.Name
    ) -> (NSHostingController<AnyView>, NSWindow) {
        let metrics = TerminalPromptLayoutMetrics(fontSize: fontSize)
        let prompt = Binding.constant("kubectl get pods")
        let focus = Binding.constant(false)
        let editor = TerminalPromptTextEditor(
            text: prompt,
            fontSize: fontSize,
            isEnabled: isEnabled,
            isFocused: focus,
            onSubmit: {},
            onHistoryUp: {},
            onHistoryDown: {},
            onSendControlSequence: { _ in },
            onClearTranscript: {}
        )
        let host = NSHostingController(rootView: AnyView(
            editor.frame(width: 360, height: metrics.controlHeight)
        ))
        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 360, height: metrics.controlHeight),
            styleMask: [.borderless],
            backing: .buffered,
            defer: false
        )
        window.appearance = NSAppearance(named: appearance)
        window.contentViewController = host
        window.orderFront(nil)
        return (host, window)
    }

    private func settle(window: NSWindow) async throws {
        for _ in 0..<4 {
            window.contentView?.layoutSubtreeIfNeeded()
            try await Task.sleep(nanoseconds: 10_000_000)
        }
    }

    private func findPromptScrollView(in view: NSView) -> NSScrollView? {
        if let scrollView = view as? NSScrollView,
           scrollView.documentView is NSTextView {
            return scrollView
        }

        for subview in view.subviews {
            if let match = findPromptScrollView(in: subview) {
                return match
            }
        }
        return nil
    }
}
