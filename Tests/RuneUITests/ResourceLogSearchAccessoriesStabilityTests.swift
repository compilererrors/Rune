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

    private func accessories(compact: Bool, status: String, statusColor: Color) -> ResourceLogSearchAccessories {
        ResourceLogSearchAccessories(
            compact: compact,
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
