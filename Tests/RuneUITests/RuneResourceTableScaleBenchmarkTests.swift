import AppKit
import RuneCore
@testable import RuneUI
import SwiftUI
import XCTest

@MainActor
final class RuneResourceTableScaleBenchmarkTests: XCTestCase {
    private final class TableInput: ObservableObject {
        @Published var pods: [PodSummary]
        init(pods: [PodSummary]) { self.pods = pods }
    }

    private struct HostedPodTable: View {
        @ObservedObject var input: TableInput

        var body: some View {
            AppKitPodTableView(
                pods: input.pods,
                selectedPodID: input.pods.first?.id,
                selectedPodIDs: [],
                sortColumn: .name,
                sortAscending: true,
                nameColumnWidth: 260,
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
            )
        }
    }

    func testLargePodTableMountFilterAndLiveResizeBenchmarkKPI() throws {
        let statuses = ["Running", "Pending", "Failed", "Succeeded"]
        let reasons: [String?] = [nil, nil, "CrashLoopBackOff", "ContainerStatusUnknown", nil, "Init:0/2"]
        let allPods = (0..<5_000).map { index in
            PodSummary(
                name: "sample-workload-\(index)-7c9db",
                namespace: "team-\(index % 7)",
                status: statuses[index % statuses.count],
                statusReason: reasons[index % reasons.count],
                totalRestarts: index % 9,
                ageDescription: "\(index % 30 + 1)d",
                cpuUsage: "\(index % 400)m",
                memoryUsage: "\(64 + index % 512)Mi"
            )
        }
        let filtered = allPods.filter { $0.name.hasSuffix("7-7c9db") && $0.namespace == "team-3" }
        let input = TableInput(pods: allPods)

        let window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1200, height: 800),
            styleMask: [.titled, .resizable],
            backing: .buffered,
            defer: false
        )
        let host = NSHostingView(rootView: HostedPodTable(input: input))
        host.frame = NSRect(x: 0, y: 0, width: 1200, height: 800)
        window.contentView = host
        window.orderFront(nil)
        defer { window.orderOut(nil) }

        let mountStarted = ContinuousClock.now
        pump(host) { (self.firstTableView(in: host)?.numberOfRows ?? 0) == allPods.count }
        let mountSeconds = mountStarted.duration(to: .now).benchmarkSeconds
        let tableView = try XCTUnwrap(firstTableView(in: host))
        XCTAssertEqual(tableView.numberOfRows, allPods.count)

        let filterStarted = ContinuousClock.now
        let swapRounds = Int(ProcessInfo.processInfo.environment["RUNE_TABLE_SWAP_ROUNDS"] ?? "") ?? 5
        for _ in 0..<swapRounds {
            input.pods = filtered
            pump(host) { tableView.numberOfRows == filtered.count }
            XCTAssertEqual(tableView.numberOfRows, filtered.count)
            input.pods = allPods
            pump(host) { tableView.numberOfRows == allPods.count }
            XCTAssertEqual(tableView.numberOfRows, allPods.count)
        }
        let filterSeconds = filterStarted.duration(to: .now).benchmarkSeconds

        let nameColumn = tableView.column(withIdentifier: NSUserInterfaceItemIdentifier("name"))
        let probeCell = tableView.view(atColumn: max(nameColumn, 0), row: 0, makeIfNecessary: false)
        let widths: [CGFloat] = stride(from: 1200, through: 700, by: -10).map { CGFloat($0) }
            + stride(from: 710, through: 1200, by: 10).map { CGFloat($0) }
        let resizeStarted = ContinuousClock.now
        for width in widths {
            host.setFrameSize(NSSize(width: width, height: 800))
            host.layoutSubtreeIfNeeded()
            tableView.displayIfNeeded()
        }
        let resizeSeconds = resizeStarted.duration(to: .now).benchmarkSeconds
        let cellAfterResize = tableView.view(atColumn: max(nameColumn, 0), row: 0, makeIfNecessary: false)

        print(
            "KPI large pod table: mount \(String(format: "%.3f", mountSeconds))s, "
                + "10 filter swaps \(String(format: "%.3f", filterSeconds))s, "
                + "\(widths.count) live resize steps \(String(format: "%.3f", resizeSeconds))s"
        )
        XCTAssertNotNil(probeCell)
        XCTAssertTrue(probeCell === cellAfterResize, "Live resize must reuse visible cells instead of reloading the table.")
        XCTAssertEqual(tableView.numberOfRows, allPods.count)

        let leadingCells = visibleCellIdentities(in: tableView)
        tableView.scrollRowToVisible(2_400)
        pump(host) { true }
        let distantCells = visibleCellIdentities(in: tableView)
        XCTAssertFalse(leadingCells.isEmpty)
        XCTAssertFalse(
            leadingCells.isDisjoint(with: distantCells),
            "After filtering, scrolling must still reuse cells instead of building new ones."
        )

        #if DEBUG
        let budgets = (mount: 0.8, filter: 1.5, resizePerStep: 0.006)
        #else
        let budgets = (mount: 0.5, filter: 1.2, resizePerStep: 0.005)
        #endif
        XCTAssertLessThan(mountSeconds, budgets.mount, "KPI: mounting 5,000 pods must stay snappy.")
        XCTAssertLessThan(filterSeconds, budgets.filter, "KPI: filtering 5,000 pods to 50 and back must stay snappy.")
        XCTAssertLessThan(
            resizeSeconds / Double(widths.count),
            budgets.resizePerStep,
            "KPI: each live resize step of a 5,000-row table must fit well inside a frame."
        )
    }

    private func pump(_ view: NSView, until isSettled: () -> Bool) {
        let deadline = Date().addingTimeInterval(3)
        repeat {
            view.layoutSubtreeIfNeeded()
            RunLoop.main.run(until: Date().addingTimeInterval(0.001))
        } while !isSettled() && Date() < deadline
        view.layoutSubtreeIfNeeded()
        view.displayIfNeeded()
    }

    private func visibleCellIdentities(in tableView: NSTableView) -> Set<ObjectIdentifier> {
        var identities = Set<ObjectIdentifier>()
        let rows = tableView.rows(in: tableView.visibleRect)
        for row in rows.location..<(rows.location + rows.length) {
            for column in 0..<tableView.numberOfColumns {
                if let cell = tableView.view(atColumn: column, row: row, makeIfNecessary: false) {
                    identities.insert(ObjectIdentifier(cell))
                }
            }
        }
        return identities
    }

    private func firstTableView(in view: NSView) -> NSTableView? {
        if let tableView = view as? NSTableView { return tableView }
        for subview in view.subviews {
            if let found = firstTableView(in: subview) { return found }
        }
        return nil
    }
}

private extension Duration {
    var benchmarkSeconds: Double {
        let parts = components
        return Double(parts.seconds) + Double(parts.attoseconds) / 1e18
    }
}
