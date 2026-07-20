import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

final class RuneSegmentedPickerInScrollTests: XCTestCase {
    @MainActor
    func testLongSegmentGroupStaysInsideCompactInspectorWidth() {
        let host = NSHostingView(rootView: RuneSegmentedPickerInScroll(
            "Inspector",
            selection: .constant(0),
            labelsHidden: true
        ) {
            ForEach(0..<8, id: \.self) { index in
                Text("Long choice \(index)").tag(index)
            }
        }
        .frame(width: 320))

        host.layoutSubtreeIfNeeded()
        XCTAssertEqual(host.fittingSize.width, 320, accuracy: 0.5)
        XCTAssertGreaterThanOrEqual(host.fittingSize.height, 20)
    }

    func testOverflowIsDiscoverableWithoutArtificialLeadingDrift() throws {
        let source = try String(contentsOfFile: sourcePath, encoding: .utf8)

        XCTAssertTrue(source.contains("ScrollView(.horizontal, showsIndicators: true)"))
        XCTAssertTrue(source.contains("Scroll horizontally to reveal additional choices"))
        XCTAssertFalse(source.contains(".padding(.leading"))
        XCTAssertTrue(source.contains(".frame(maxWidth: .infinity, alignment: .leading)"))
    }

    private var sourcePath: String {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/RuneSegmentedPickerInScroll.swift")
            .path
    }
}
