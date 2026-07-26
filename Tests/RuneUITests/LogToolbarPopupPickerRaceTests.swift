import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class LogToolbarPopupPickerRaceTests: XCTestCase {
    func testTrackedSelectionUsesDisplayedValueAcrossReorderedStaleProjection() throws {
        let state = PopupSelectionState("a")
        let initialOptions = options(["a", "b"])
        let initialPicker = picker(state: state, options: initialOptions)
        let coordinator = LogToolbarPopupPicker<String>.Coordinator(parent: initialPicker)
        let popup = NSPopUpButton(frame: .zero, pullsDown: false)
        coordinator.attach(to: popup)

        let menu = try XCTUnwrap(popup.menu)
        coordinator.menuWillOpen(menu)

        let reorderedOptions = options(["b", "a"])
        coordinator.update(
            parent: picker(state: state, options: reorderedOptions),
            popup: popup
        )

        XCTAssertEqual(popup.itemTitles, initialOptions.map(\.title))
        XCTAssertEqual(coordinator.displayedOptions.map(\.value), ["a", "b"])

        // AppKit may close its menu before sending the popup's target/action.
        popup.selectItem(at: 1)
        coordinator.menuDidClose(menu)
        coordinator.selectionChanged(popup)

        XCTAssertEqual(state.publishedValues, ["b"])

        coordinator.finishDeferredProjection(on: popup)

        XCTAssertEqual(coordinator.displayedOptions.map(\.value), ["b", "a"])
        XCTAssertEqual(selectedValue(in: popup, coordinator: coordinator), "b")

        // A normal SwiftUI acknowledgement releases the temporary user-intent protection.
        state.projectedValue = "b"
        coordinator.update(
            parent: picker(state: state, options: reorderedOptions),
            popup: popup
        )
        state.projectedValue = "a"
        coordinator.update(
            parent: picker(state: state, options: reorderedOptions),
            popup: popup
        )

        XCTAssertEqual(selectedValue(in: popup, coordinator: coordinator), "a")
    }

    func testSixtyFourRapidTrackedSelectionsAlwaysPublishAndDisplayLatestValue() throws {
        let state = PopupSelectionState("0")
        var projectedOrder = Array(0..<8).map(String.init)
        var currentValue = "0"
        let initialPicker = picker(state: state, options: options(projectedOrder))
        let coordinator = LogToolbarPopupPicker<String>.Coordinator(parent: initialPicker)
        let popup = NSPopUpButton(frame: .zero, pullsDown: false)
        coordinator.attach(to: popup)
        let menu = try XCTUnwrap(popup.menu)

        for cycle in 0..<64 {
            state.projectedValue = currentValue
            coordinator.update(
                parent: picker(state: state, options: options(projectedOrder)),
                popup: popup
            )
            coordinator.menuWillOpen(menu)

            let displayedValues = coordinator.displayedOptions.map(\.value)
            let currentIndex = try XCTUnwrap(displayedValues.firstIndex(of: currentValue))
            let targetIndex = (currentIndex + 1 + cycle % 3) % displayedValues.count
            let targetValue = displayedValues[targetIndex]

            let rotation = (cycle * 3 + 1) % projectedOrder.count
            projectedOrder = Array(projectedOrder[rotation...] + projectedOrder[..<rotation])
            if cycle.isMultiple(of: 2) {
                projectedOrder.reverse()
            }

            // This is a stale SwiftUI projection arriving inside AppKit's nested menu loop.
            state.projectedValue = currentValue
            coordinator.update(
                parent: picker(state: state, options: options(projectedOrder)),
                popup: popup
            )
            XCTAssertEqual(
                coordinator.displayedOptions.map(\.value),
                displayedValues,
                "Cycle \(cycle) changed the options under the tracked menu."
            )

            popup.selectItem(at: targetIndex)
            if cycle.isMultiple(of: 2) {
                coordinator.selectionChanged(popup)
                coordinator.menuDidClose(menu)
            } else {
                coordinator.menuDidClose(menu)
                coordinator.selectionChanged(popup)
            }
            coordinator.finishDeferredProjection(on: popup)

            XCTAssertEqual(state.publishedValues.last, targetValue, "Cycle \(cycle)")
            XCTAssertEqual(selectedValue(in: popup, coordinator: coordinator), targetValue, "Cycle \(cycle)")

            state.projectedValue = targetValue
            coordinator.update(
                parent: picker(state: state, options: options(projectedOrder)),
                popup: popup
            )
            XCTAssertEqual(selectedValue(in: popup, coordinator: coordinator), targetValue, "Cycle \(cycle)")
            currentValue = targetValue
        }

        XCTAssertEqual(state.publishedValues.count, 64)
        XCTAssertEqual(state.publishedValues.last, currentValue)
    }

    private func picker(
        state: PopupSelectionState<String>,
        options: [LogToolbarPopupPicker<String>.Option]
    ) -> LogToolbarPopupPicker<String> {
        LogToolbarPopupPicker(
            "Synthetic picker",
            selection: Binding(
                get: { state.projectedValue },
                set: { state.publishedValues.append($0) }
            ),
            options: options
        )
    }

    private func options(
        _ values: [String]
    ) -> [LogToolbarPopupPicker<String>.Option] {
        values.map {
            LogToolbarPopupPicker<String>.Option(
                value: $0,
                title: "Option \($0)"
            )
        }
    }

    private func selectedValue(
        in popup: NSPopUpButton,
        coordinator: LogToolbarPopupPicker<String>.Coordinator
    ) -> String? {
        let index = popup.indexOfSelectedItem
        guard coordinator.displayedOptions.indices.contains(index) else { return nil }
        return coordinator.displayedOptions[index].value
    }
}

@MainActor
private final class PopupSelectionState<Value> {
    var projectedValue: Value
    var publishedValues: [Value] = []

    init(_ projectedValue: Value) {
        self.projectedValue = projectedValue
    }
}
