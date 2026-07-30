import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class RuneCursorRegionTests: XCTestCase {
    func testNestedCursorRegionsKeepIndependentBoundsAndIntent() throws {
        let host = NSHostingView(
            rootView: ZStack(alignment: .topLeading) {
                Color.clear
                    .frame(width: 300, height: 180)
                    .runeTextInputCursor()

                Color.clear
                    .frame(width: 120, height: 60)
                    .runePointerCursor()
            }
            .frame(width: 300, height: 180)
        )
        host.frame = NSRect(x: 0, y: 0, width: 300, height: 180)
        host.layoutSubtreeIfNeeded()

        let regions = cursorRegions(in: host)
        let textRegion = try XCTUnwrap(regions.first { $0.intent == .textInput })
        let pointerRegion = try XCTUnwrap(regions.first { $0.intent == .pointer })

        XCTAssertEqual(regions.count, 2)
        XCTAssertEqual(
            textRegion.convert(textRegion.bounds, to: host).size.width,
            300,
            accuracy: 0.5
        )
        XCTAssertEqual(
            textRegion.convert(textRegion.bounds, to: host).size.height,
            180,
            accuracy: 0.5
        )
        XCTAssertEqual(
            pointerRegion.convert(pointerRegion.bounds, to: host).size.width,
            120,
            accuracy: 0.5
        )
        XCTAssertEqual(
            pointerRegion.convert(pointerRegion.bounds, to: host).size.height,
            60,
            accuracy: 0.5
        )
        XCTAssertNil(
            pointerRegion.hitTest(NSPoint(x: 20, y: 20)),
            "Cursor regions must not intercept clicks from the controls they describe."
        )
    }

    private func cursorRegions(in root: NSView) -> [RuneCursorRectView] {
        var result: [RuneCursorRectView] = []
        if let region = root as? RuneCursorRectView {
            result.append(region)
        }
        for subview in root.subviews {
            result.append(contentsOf: cursorRegions(in: subview))
        }
        return result
    }
}
