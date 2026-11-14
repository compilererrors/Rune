import Foundation

/// Tracks whether the user is scrolling a resource list right now. Live list
/// updates wait for the scroll to settle, so watch events from busy clusters
/// never reload a table or re-render the workspace under the user's pointer.
@MainActor
final class RuneLiveScrollActivity {
    static let shared = RuneLiveScrollActivity()

    /// A scroll ends once no scroll event arrived for this long.
    var idleDelay: TimeInterval = 0.3
    private(set) var isScrolling = false
    private var idleWorkItem: DispatchWorkItem?
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private var pendingActions: [ObjectIdentifier: () -> Void] = [:]
    private var pendingOrder: [ObjectIdentifier] = []

    func noteScrollEvent() {
        isScrolling = true
        idleWorkItem?.cancel()
        let workItem = DispatchWorkItem { [weak self] in
            self?.endScrolling()
        }
        idleWorkItem = workItem
        DispatchQueue.main.asyncAfter(deadline: .now() + idleDelay, execute: workItem)
    }

    func endScrolling() {
        idleWorkItem?.cancel()
        idleWorkItem = nil
        guard isScrolling else { return }
        isScrolling = false
        let continuations = waiters
        waiters.removeAll()
        let order = pendingOrder
        let actions = pendingActions
        pendingOrder.removeAll()
        pendingActions.removeAll()
        for key in order {
            actions[key]?()
        }
        continuations.forEach { $0.resume() }
    }

    func waitUntilIdle() async {
        guard isScrolling else { return }
        await withCheckedContinuation { waiters.append($0) }
    }

    /// Runs `action` on the next main-queue turn, or once scrolling ends. Only
    /// the latest action per owner is kept, so a burst of updates applies once.
    func performWhenIdle(owner: AnyObject, _ action: @escaping () -> Void) {
        DispatchQueue.main.async { [weak self] in
            guard let self else { return }
            guard self.isScrolling else {
                action()
                return
            }
            let key = ObjectIdentifier(owner)
            if self.pendingActions.updateValue(action, forKey: key) == nil {
                self.pendingOrder.append(key)
            }
        }
    }
}
