import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

/// Design rule: controls in the same cluster share one height (and the same
/// radius and chrome, which the components guarantee). This measures every
/// kind of toolbar control side by side at several text sizes.
@MainActor
final class RuneToolbarRowConsistencyTests: XCTestCase {
    func testEveryControlInAToolbarRowSharesOneHeight() throws {
        for (fontSize, dynamicType) in [(12.0, DynamicTypeSize.large), (16.0, .large), (12.0, .accessibility2)] {
            let heights = ControlHeights()
            let host = NSHostingView(rootView:
                ToolbarRowHarness(heights: heights)
                    .padding(12)
                    .runeInterfaceTypography(configuredFontSize: fontSize, systemDynamicTypeSize: dynamicType)
                    .environment(\.dynamicTypeSize, dynamicType)
                    .runeAppearanceTheme(RuneAppearanceTheme.graphiteBlue.resolvedTheme)
            )
            host.setFrameSize(NSSize(width: 1_200, height: 120))
            let window = NSWindow(contentRect: host.frame, styleMask: [.borderless], backing: .buffered, defer: false)
            window.isReleasedWhenClosed = false
            window.contentView = host
            window.makeKeyAndOrderFront(nil)
            defer { window.orderOut(nil) }
            for _ in 0..<4 {
                host.layoutSubtreeIfNeeded()
                RunLoop.main.run(until: Date().addingTimeInterval(0.03))
            }

            XCTAssertEqual(Set(heights.values.keys), Set(ToolbarRowHarness.controls), "every control was measured")
            let reference = try XCTUnwrap(heights.values["button"])
            for (control, height) in heights.values.sorted(by: { $0.key < $1.key }) {
                XCTAssertEqual(height, reference, accuracy: 0.5, "\(control) must match the button height at \(fontSize) pt, \(dynamicType)")
            }
        }
    }
}

@MainActor
private final class ControlHeights {
    var values: [String: CGFloat] = [:]
}

private struct ToolbarRowHarness: View {
    static let controls = ["field", "button", "menu", "toggle", "group", "icon", "chip"]
    let heights: ControlHeights
    @State private var text = ""
    @State private var isOn = true

    var body: some View {
        HStack(spacing: RuneSpacing.xs) {
            TextField("Filter", text: $text)
                .textFieldStyle(RuneControlTextFieldStyle())
                .frame(width: 220)
                .measured("field", into: heights)
            Button { } label: { Label("Select Visible", systemImage: "checklist") }
                .buttonStyle(RuneToolbarButtonStyle())
                .measured("button", into: heights)
            RuneToolbarMenu {
                Button("Name") {}
            } label: {
                Label("Sort", systemImage: "arrow.up.arrow.down")
            }
            .measured("menu", into: heights)
            Toggle(isOn: $isOn) { Image(systemName: "pause") }
                .toggleStyle(RuneToolbarToggleStyle(isIconOnly: true))
                .measured("toggle", into: heights)
            RuneToolbarButtonGroup(isSelected: true) {
                RuneToolbarGroupLabel { Text("3 selected") }
                RuneToolbarGroupDivider()
                Button { } label: { Image(systemName: "xmark") }
                    .buttonStyle(RuneToolbarSegmentButtonStyle())
            }
            .measured("group", into: heights)
            Button { } label: { Image(systemName: "square.and.arrow.up") }
                .buttonStyle(RuneToolbarButtonStyle(isIconOnly: true))
                .measured("icon", into: heights)
            RuneBadge("Updated 12:00", systemImage: "clock", inControlRow: true)
                .measured("chip", into: heights)
        }
    }
}

private extension View {
    func measured(_ id: String, into heights: ControlHeights) -> some View {
        background(GeometryReader { proxy in
            Color.clear
                .onAppear { heights.values[id] = proxy.size.height }
                .onChange(of: proxy.size.height) { _, height in heights.values[id] = height }
        })
    }
}
