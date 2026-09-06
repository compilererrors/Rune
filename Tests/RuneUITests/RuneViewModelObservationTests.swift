import Combine
import XCTest
@testable import RuneCore
@testable import RuneUI

@MainActor
final class RuneViewModelObservationTests: XCTestCase {
    func testWriteDialogsPublishBeforeOpeningConfirmingAndCancelling() {
        let key = RuneSettingsKeys.writeSafetyRequireProductionSecondConfirmation
        let previous = UserDefaults.standard.object(forKey: key)
        UserDefaults.standard.set(true, forKey: key)
        defer {
            if let previous { UserDefaults.standard.set(previous, forKey: key) }
            else { UserDefaults.standard.removeObject(forKey: key) }
        }
        let state = RuneAppState()
        state.selectedContext = KubeContext(name: "synthetic-production")
        state.setSelectedPod(PodSummary(name: "synthetic-pod", namespace: "default", status: "Running"))
        let model = RuneAppViewModel(state: state)
        var snapshots: [(PendingWriteAction?, PendingWriteAction?)] = []
        let subscription = model.objectWillChange.sink {
            snapshots.append((model.pendingWriteAction, model.pendingProductionDestructiveConfirmation))
        }
        let action = PendingWriteAction.delete(kind: .pod, name: "synthetic-pod")
        model.requestDeleteSelectedResource()
        XCTAssertEqual(model.pendingWriteAction, action)
        XCTAssertFalse(snapshots.isEmpty)
        XCTAssertNil(snapshots.first?.0)
        snapshots.removeAll()
        model.confirmPendingWriteAction()
        XCTAssertEqual(model.pendingProductionDestructiveConfirmation, action)
        XCTAssertEqual(snapshots.first?.0, action)
        XCTAssertNil(snapshots.first?.1)
        snapshots.removeAll()
        model.cancelPendingWriteAction()
        XCTAssertNil(model.pendingWriteAction)
        XCTAssertNil(model.pendingProductionDestructiveConfirmation)
        XCTAssertEqual(snapshots.first?.0, action)
        XCTAssertEqual(snapshots.first?.1, action)
        withExtendedLifetime(subscription) {}
    }

    func testStableStateStillForwardsChangesBeforeMutation() {
        let state = RuneAppState()
        let model = RuneAppViewModel(state: state)
        var observedNamespaces: [String] = []
        let subscription = model.objectWillChange.sink { observedNamespaces.append(state.selectedNamespace) }
        let original = state.selectedNamespace
        state.selectedNamespace = "synthetic-space"
        XCTAssertTrue(model.state === state)
        XCTAssertEqual(observedNamespaces.first, original)
        XCTAssertEqual(model.state.selectedNamespace, "synthetic-space")
        withExtendedLifetime(subscription) {}
    }

    func testResourceAndSearchChangesInvalidateAlreadyReadProjection() {
        let state = RuneAppState()
        let model = RuneAppViewModel(state: state)
        let first = PodSummary(name: "synthetic-alpha", namespace: "default", status: "Running")
        let second = PodSummary(name: "synthetic-beta", namespace: "default", status: "Running")
        state.setPods([first])
        XCTAssertEqual(model.visiblePods, [first])
        state.setPods([first, second])
        XCTAssertEqual(model.visiblePods, [first, second])
        state.resourceSearchQuery = "synthetic-beta"
        XCTAssertEqual(model.visiblePods, [second])
        state.resourceSearchQuery = ""
        XCTAssertEqual(model.visiblePods, [first, second])
    }

    func testObservationDoesNotKeepViewModelAliveWithSurvivingState() {
        let state = RuneAppState()
        var model: RuneAppViewModel? = RuneAppViewModel(state: state)
        weak var observed = model
        model = nil
        XCTAssertNil(observed)
        state.resourceSearchQuery = "synthetic-query"
        XCTAssertNil(observed)
    }
}
