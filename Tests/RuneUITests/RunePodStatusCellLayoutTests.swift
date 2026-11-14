import AppKit
import RuneCore
@testable import RuneUI
import SwiftUI
import XCTest

@MainActor
final class RunePodStatusCellLayoutTests: XCTestCase {
    func testLongStatusesStayInsideTheirPillCellAtEveryTableWidth() throws {
        let reasons: [String?] = [
            nil, "ContainerStatusUnknown", "Init:CrashLoopBackOff", "ExitCode:137", "SchedulingGated",
            "NotReady", "Evicted", "Error", "CreateContainerConfigError", "Init:0/3"
        ]
        let pods = reasons.enumerated().map { index, reason in
            PodSummary(
                name: "sample-pod-\(index)",
                namespace: "default",
                status: index == 0 ? "Running" : "Pending",
                statusReason: reason,
                ageDescription: "2y"
            )
        }

        for width in [1200.0, 900.0, 640.0, 420.0] {
            let host = NSHostingView(rootView: AppKitPodTableView(
                pods: pods,
                selectedPodID: nil,
                selectedPodIDs: [],
                sortColumn: .name,
                sortAscending: true,
                nameColumnWidth: 600,
                canApplyClusterMutations: false,
                isFavorite: { _ in false },
                onSelectPod: { _ in },
                onToggleBulkSelection: { _ in },
                onToggleSort: { _ in },
                onNameColumnWidthChanged: { _ in },
                onToggleFavorite: { _ in },
                onOpenLogs: { _ in },
                onOpenExec: { _ in },
                onOpenDescribe: { _ in },
                onOpenYAML: { _ in },
                onDelete: { _ in }
            ).frame(width: width, height: 420))
            host.frame = NSRect(x: 0, y: 0, width: width, height: 420)
            for _ in 0..<4 {
                host.layoutSubtreeIfNeeded()
                RunLoop.main.run(until: Date().addingTimeInterval(0.02))
            }

            let tableView = try XCTUnwrap(firstTableView(in: host))
            let statusColumn = tableView.column(withIdentifier: NSUserInterfaceItemIdentifier("status"))
            XCTAssertGreaterThanOrEqual(statusColumn, 0, "status column missing at \(width)pt")
            guard statusColumn >= 0 else { continue }
            let visibleMaxX = tableView.enclosingScrollView?.contentView.bounds.maxX ?? tableView.bounds.maxX
            XCTAssertLessThanOrEqual(
                tableView.rect(ofColumn: statusColumn).maxX,
                visibleMaxX + 0.5,
                "status column must stay in view at \(width)pt even with a wide saved name column"
            )
            for row in 0..<tableView.numberOfRows {
                let cell = try XCTUnwrap(tableView.view(atColumn: statusColumn, row: row, makeIfNecessary: true))
                cell.layoutSubtreeIfNeeded()
                XCTAssertTrue(cell.clipsToBounds, "status cell must clip at \(width)pt")
                let pill = try XCTUnwrap(cell.subviews.first, "missing status pill at \(width)pt row \(row)")
                XCTAssertGreaterThanOrEqual(pill.frame.minX, -0.5, "pill escaped left at \(width)pt row \(row)")
                XCTAssertLessThanOrEqual(pill.frame.maxX, cell.bounds.width + 0.5, "pill escaped right at \(width)pt row \(row)")
                XCTAssertGreaterThan(pill.frame.width, 0, "pill collapsed at \(width)pt row \(row)")
                XCTAssertNotNil(pill.toolTip, "pill needs full-text help at \(width)pt row \(row)")
            }
        }
    }

    private func firstTableView(in view: NSView) -> NSTableView? {
        if let tableView = view as? NSTableView { return tableView }
        for subview in view.subviews {
            if let found = firstTableView(in: subview) { return found }
        }
        return nil
    }
}
