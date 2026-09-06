import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class ResourceLogSearchAccessoriesTests: XCTestCase {
    func testResultUpdatesKeepControlsAndInvokeOnlyTheLatestResourceAction() throws {
        let view = ResourceLogSearchAccessories.Controls(compact: false)
        var oldActions: [ResourceLogSearchAccessories.Action] = []
        var currentActions: [ResourceLogSearchAccessories.Action] = []
        view.update(from: value { oldActions.append($0) })
        let controls = view.buttons
        for index in 0..<100 {
            view.update(from: value(matches: index.isMultiple(of: 2), status: "\(index + 1) of 100") { currentActions.append($0) })
        }
        for (action, button) in controls { XCTAssertTrue(view.buttons[action] === button) }
        XCTAssertEqual(view.statusButton.title, "100 of 100")
        XCTAssertEqual(view.statusButton.attributedTitle.attribute(.foregroundColor, at: 0, effectiveRange: nil) as? NSColor, NSColor(Color.secondary))
        XCTAssertFalse(try XCTUnwrap(view.buttons[.next]).isEnabled)
        view.update(from: value { currentActions.append($0) })
        for action in [ResourceLogSearchAccessories.Action.clear, .matchCase, .previous, .next, .jump] {
            try XCTUnwrap(view.buttons[action]).performClick(nil)
        }
        XCTAssertTrue(oldActions.isEmpty)
        XCTAssertEqual(currentActions, [.clear, .matchCase, .previous, .next, .jump])
        view.invalidate()
        try XCTUnwrap(view.buttons[.next]).performClick(nil)
        XCTAssertEqual(currentActions, [.clear, .matchCase, .previous, .next, .jump])
    }

    func testCompactMenuTracksPendingResultsAndCaseStateWithoutReplacingTheMenu() throws {
        let view = ResourceLogSearchAccessories.Controls(compact: true)
        var actions: [ResourceLogSearchAccessories.Action] = []
        view.update(from: value(compact: true, matches: false, matchCase: true, status: "…") { actions.append($0) })
        let menu = try XCTUnwrap(view.menuButton?.menu)
        let next = try XCTUnwrap(menu.items.first { $0.tag == ResourceLogSearchAccessories.Action.next.rawValue })
        let matchCase = try XCTUnwrap(menu.items.dropFirst().first { $0.tag == ResourceLogSearchAccessories.Action.matchCase.rawValue })
        XCTAssertFalse(next.isEnabled)
        XCTAssertEqual(matchCase.state, .on)
        XCTAssertFalse(view.statusButton.isEnabled)
        view.update(from: value(compact: true) { actions.append($0) })
        XCTAssertTrue(view.menuButton?.menu === menu)
        XCTAssertTrue(next.isEnabled)
        XCTAssertEqual(matchCase.state, .off)
        XCTAssertTrue(view.statusButton.isEnabled)
        NSApp.sendAction(try XCTUnwrap(next.action), to: next.target, from: next)
        XCTAssertEqual(actions, [.next])
    }

    private func value(compact: Bool = false, matches: Bool = true, matchCase: Bool = false, status: String = "1 of 100", action: @escaping (ResourceLogSearchAccessories.Action) -> Void) -> ResourceLogSearchAccessories {
        ResourceLogSearchAccessories(compact: compact, hasQuery: true, matchCase: matchCase, hasMatches: matches,
                                     status: status, statusLabel: "Search match \(status)", statusColor: .secondary,
                                     matchCaseHelp: "Match case", action: action)
    }
}
