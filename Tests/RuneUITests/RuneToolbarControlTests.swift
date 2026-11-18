import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class RuneToolbarControlTests: XCTestCase {
    @MainActor
    func testThemeControlColorsStayReadableThroughEveryInteractionState() throws {
        for appearance in RuneAppearanceTheme.allCases where appearance != .native {
            let palette = try XCTUnwrap(appearance.resolvedTheme.palette)
            for tint in [nil, palette.danger] as [Color?] {
                for isEnabled in [true, false] {
                    for isSelected in [true, false] {
                        for isPressed in [true, false] {
                            for isHovered in [true, false] {
                                for role in [RuneButtonRole.primary, .secondary, .plain, .destructive] {
                                    let colors = RuneToolbarControlColors(
                                        palette: palette, tint: tint, isEnabled: isEnabled,
                                        isSelected: isSelected, isPressed: isPressed,
                                        isHovered: isHovered, role: role
                                    )
                                    XCTAssertGreaterThanOrEqual(RuneThemeContrast.ratio(NSColor(colors.foreground), over: NSColor(colors.contrastSurface)), 4.5, "\(appearance.title) \(role)")
                                }
                            }
                        }
                    }
                }
            }
            // Default (secondary) controls carry the theme accent as their label.
            let colors = RuneToolbarControlColors(palette: palette)
            let source = try XCTUnwrap(NSColor(palette.accentFill).usingColorSpace(.sRGB))
            let foreground = try XCTUnwrap(NSColor(colors.foreground).usingColorSpace(.sRGB))
            XCTAssertEqual(foreground.hueComponent, source.hueComponent, accuracy: 0.001, appearance.title)
            XCTAssertGreaterThanOrEqual(foreground.saturationComponent, source.saturationComponent * 0.9, appearance.title)
            let prominent = RuneToolbarControlColors(palette: palette, isProminent: true)
            XCTAssertEqual(RuneThemeColorParser.rgbaHex(NSColor(prominent.background)), RuneThemeColorParser.rgbaHex(source), appearance.title)
        }
    }

    @MainActor
    func testButtonsTogglesAndMenusShareVisibleHeightAcrossThemesAndTextSizes() throws {
        for theme in [RuneAppearanceTheme.paper, .graphiteBlue] {
            for (fontSize, dynamicType, expectedHeight) in [(12.0, DynamicTypeSize.large, 28.0), (20.0, .large, 32.0), (20.0, .accessibility3, 44.0)] {
                let measurements = ToolbarMeasurements()
                let host = NSHostingView(rootView:
                    ToolbarControlHarness(measurements: measurements)
                        .padding(12)
                        .runeInterfaceTypography(configuredFontSize: fontSize, systemDynamicTypeSize: dynamicType)
                        .environment(\.dynamicTypeSize, dynamicType)
                        .runeAppearanceTheme(theme.resolvedTheme)
                )
                host.setFrameSize(host.fittingSize)
                let window = NSWindow(contentRect: host.frame, styleMask: [.borderless], backing: .buffered, defer: false)
                window.isReleasedWhenClosed = false
                window.contentView = host
                window.makeKeyAndOrderFront(nil)
                defer { RuneTestWindow.close(window) }
                for _ in 0..<4 {
                    host.layoutSubtreeIfNeeded()
                    RunLoop.main.run(until: Date().addingTimeInterval(0.03))
                }

                let sizes = measurements.sizes
                XCTAssertEqual(sizes.count, 9)
                for (name, size) in sizes {
                    XCTAssertEqual(size.height, expectedHeight, accuracy: 0.5, "\(name), \(theme), \(fontSize)")
                }
                XCTAssertEqual(try XCTUnwrap(sizes["play"]).width, expectedHeight, accuracy: 0.5)
                XCTAssertEqual(try XCTUnwrap(sizes["quick-save"]).width, expectedHeight, accuracy: 0.5)
                XCTAssertEqual(try XCTUnwrap(sizes["previous"]).width, expectedHeight, accuracy: 0.5)

                if let directory = ProcessInfo.processInfo.environment["RUNE_TOOLBAR_CONTROL_SNAPSHOT_DIR"] {
                    let url = URL(fileURLWithPath: directory, isDirectory: true)
                    try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
                    let bitmap = try XCTUnwrap(host.bitmapImageRepForCachingDisplay(in: host.bounds))
                    host.cacheDisplay(in: host.bounds, to: bitmap)
                    let data = try XCTUnwrap(bitmap.representation(using: .png, properties: [:]))
                    try data.write(to: url.appendingPathComponent("controls-\(theme)-\(Int(fontSize))-\(Int(expectedHeight)).png"))
                }
            }
        }
    }
}

@MainActor
private final class ToolbarMeasurements {
    var sizes: [String: CGSize] = [:]
}

private struct ToolbarSizeKey: PreferenceKey {
    static let defaultValue: [String: CGSize] = [:]
    static func reduce(value: inout [String: CGSize], nextValue: () -> [String: CGSize]) {
        value.merge(nextValue(), uniquingKeysWith: { _, new in new })
    }
}

private extension View {
    func measuredToolbarControl(_ name: String) -> some View {
        background {
            GeometryReader { geometry in
                Color.clear.preference(key: ToolbarSizeKey.self, value: [name: geometry.size])
            }
        }
    }
}

private struct ToolbarControlHarness: View {
    let measurements: ToolbarMeasurements
    @State private var previous = true

    var body: some View {
        HStack(spacing: 8) {
            Button {} label: { Image(systemName: "play.fill") }
                .buttonStyle(RuneToolbarButtonStyle(isIconOnly: true))
                .measuredToolbarControl("play")
            Button {} label: { Label("Reload", systemImage: "arrow.clockwise") }
                .buttonStyle(RuneToolbarButtonStyle())
                .measuredToolbarControl("reload")
            Toggle(isOn: $previous) { Image(systemName: "clock.arrow.circlepath") }
                .toggleStyle(RuneToolbarToggleStyle(isIconOnly: true))
                .measuredToolbarControl("previous")
            Button {} label: { Label("Save Logs", systemImage: "square.and.arrow.down") }
                .buttonStyle(RuneToolbarButtonStyle())
                .measuredToolbarControl("save")
            RuneBorderedIconButton("Quick Save", systemImage: "folder.badge.plus") {}
                .measuredToolbarControl("quick-save")
            RuneToolbarMenu {
                Button("Copy All") {}
            } label: {
                Label("More", systemImage: "ellipsis.circle")
            }
            .measuredToolbarControl("menu")
            RuneToolbarPicker(title: "Order", selection: .constant("Name"), options: [("Name", "Name"), ("Date", "Date")])
                .frame(width: 120)
                .measuredToolbarControl("picker")
            TextField("Limit", text: .constant("100"))
                .textFieldStyle(RuneControlTextFieldStyle())
                .frame(width: 100)
                .measuredToolbarControl("field")
            Button("Apply") {}
                .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                .measuredToolbarControl("apply")
        }
        .onPreferenceChange(ToolbarSizeKey.self) { measurements.sizes = $0 }
    }
}
