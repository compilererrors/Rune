import AppKit
@testable import RuneUI
import SwiftUI
import XCTest

/// The log search accessories are AppKit controls inside SwiftUI. An update
/// with unchanged inputs must not touch them: a new title or hidden state
/// changes their size, which re-lays out the SwiftUI host, which updates
/// them again. That loop pinned the CPU and AppKit aborted the app ("more
/// Update Constraints passes than there are views").
@MainActor
final class ResourceLogSearchAccessoriesStabilityTests: XCTestCase {
    func testUpdatingWithUnchangedValuesLeavesTheControlsAlone() {
        for compact in [false, true] {
            for statusColor in [Color.secondary, Color.accentColor, Color(nsColor: .systemRed), Color(red: 0.4, green: 0.6, blue: 0.8)] {
                let controls = ResourceLogSearchAccessories.Controls(compact: compact)
                let value = accessories(compact: compact, status: "3/12", statusColor: statusColor)
                controls.update(from: value)
                let title = controls.statusButton.attributedTitle
                let tints = controls.buttons.mapValues { $0.contentTintColor }

                controls.update(from: value)
                controls.update(from: accessories(compact: compact, status: "3/12", statusColor: statusColor))

                XCTAssertTrue(controls.statusButton.attributedTitle === title, "status title rewritten with no change (compact \(compact), \(statusColor))")
                for (action, button) in controls.buttons {
                    XCTAssertTrue(button.contentTintColor === tints[action] ?? nil, "tint of \(action) reassigned with no change")
                }
            }
        }
    }

    /// A pane resize only shows the other form; no controls are created.
    func testResizingSwitchesFormsWithinOneView() {
        let view = ResourceLogSearchAccessories.Adaptive()
        let regular = view.regular, compact = view.compact
        for width in [ResourceLogSearchAccessories.regularWidth + 40, ResourceLogSearchAccessories.compactWidth, ResourceLogSearchAccessories.regularWidth] {
            view.frame = NSRect(x: 0, y: 0, width: width, height: RuneUILayoutMetrics.iconButtonSize)
            view.layoutSubtreeIfNeeded()
            let fits = width >= ResourceLogSearchAccessories.regularWidth
            XCTAssertEqual(view.showsRegular, fits, "width \(width)")
            XCTAssertEqual(compact.isHidden, fits, "width \(width)")
            XCTAssertEqual(view.frame.maxX, (fits ? regular : compact).frame.maxX + view.frame.minX, "trailing at width \(width)")
        }
        XCTAssertTrue(view.regular === regular && view.compact === compact)
    }

    private func accessories(compact: Bool, status: String, statusColor: Color) -> ResourceLogSearchAccessories {
        ResourceLogSearchAccessories(
            hasQuery: true,
            matchCase: false,
            hasMatches: true,
            status: status,
            statusLabel: "Match 3 of 12",
            statusColor: statusColor,
            matchCaseHelp: "Match case"
        ) { _ in }
    }
}
