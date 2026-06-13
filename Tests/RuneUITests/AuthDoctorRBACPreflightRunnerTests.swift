import XCTest
@testable import RuneUI

final class AuthDoctorRBACPreflightRunnerTests: XCTestCase {
    func testPreservesTargetOrderAndResolvesNamespaces() async {
        let targets = [
            target(id: "namespace-first", resource: "pods", scope: .namespace),
            target(id: "cluster-middle", resource: "nodes", scope: .cluster),
            target(id: "namespace-last", resource: "deployments", scope: .namespace)
        ]

        let results = await AuthDoctorRBACPreflightRunner.run(
            targets: targets,
            activeNamespace: "synthetic"
        ) { target, namespace in
            target.id != "cluster-middle" && namespace != nil
        }

        XCTAssertEqual(results.map { $0.target.id }, targets.map(\.id))
        XCTAssertEqual(results.map(\.namespace), ["synthetic", nil, "synthetic"])
        XCTAssertEqual(results.map(\.allowed), [true, false, true])
        XCTAssertEqual(results.map(\.errorMessage), [nil, nil, nil])
    }

    func testRespectsMaxConcurrentChecksAndCapturesErrors() async {
        let targets = (0..<12).map {
            target(id: "target-\($0)", resource: "resource-\($0)", scope: .namespace)
        }
        let tracker = ConcurrencyTracker()

        let results = await AuthDoctorRBACPreflightRunner.run(
            targets: targets,
            activeNamespace: "synthetic",
            maxConcurrentChecks: 3
        ) { target, _ in
            await tracker.started()
            try? await Task.sleep(nanoseconds: 1_000_000)
            await tracker.finished()
            if target.id == "target-5" {
                throw SyntheticPreflightError.denied
            }
            return true
        }

        let maximumObserved = await tracker.maximumObserved()

        XCTAssertEqual(results.count, targets.count)
        XCTAssertEqual(results.map { $0.target.id }, targets.map(\.id))
        XCTAssertLessThanOrEqual(maximumObserved, 3)
        XCTAssertEqual(results[5].allowed, nil)
        XCTAssertEqual(results[5].errorMessage, "synthetic preflight denial")
    }

    func testHandlesEmptyTargetsAndClampsConcurrency() async {
        let empty = await AuthDoctorRBACPreflightRunner.run(
            targets: [],
            activeNamespace: "synthetic"
        ) { _, _ in
            XCTFail("Empty target lists should not schedule checks.")
            return true
        }
        XCTAssertTrue(empty.isEmpty)

        let targets = (0..<3).map {
            target(id: "clamped-\($0)", resource: "resource-\($0)", scope: .namespace)
        }
        let tracker = ConcurrencyTracker()
        let results = await AuthDoctorRBACPreflightRunner.run(
            targets: targets,
            activeNamespace: "synthetic",
            maxConcurrentChecks: 0
        ) { _, _ in
            await tracker.started()
            try? await Task.sleep(nanoseconds: 1_000_000)
            await tracker.finished()
            return true
        }
        let maximumObserved = await tracker.maximumObserved()

        XCTAssertEqual(results.count, targets.count)
        XCTAssertEqual(maximumObserved, 1)
    }

    private func target(
        id: String,
        resource: String,
        scope: AuthDoctorRBACPreflightScope
    ) -> AuthDoctorRBACPreflightTarget {
        AuthDoctorRBACPreflightTarget(
            id: id,
            title: id,
            resource: resource,
            actionTitle: "Open",
            systemImage: "checkmark.circle",
            help: "Synthetic test target.",
            destination: .section(.workloads),
            scope: scope
        )
    }
}

private actor ConcurrencyTracker {
    private var current = 0
    private var maximum = 0

    func started() {
        current += 1
        maximum = max(maximum, current)
    }

    func finished() {
        current -= 1
    }

    func maximumObserved() -> Int {
        maximum
    }
}

private enum SyntheticPreflightError: LocalizedError {
    case denied

    var errorDescription: String? {
        "synthetic preflight denial"
    }
}
