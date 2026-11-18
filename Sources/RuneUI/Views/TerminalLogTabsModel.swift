import Foundation
import RuneCore

struct TerminalPodLogTab: Identifiable, Hashable {
    let id: String
    var podID: String
    var namespace: String
    var podName: String

    init(pod: PodSummary, id: String? = nil) {
        // A log tab is unique by pod within the current terminal workspace. Using
        // the stable pod identity avoids spending most of rapid add/reconcile
        // workflows formatting UUID strings and makes restoration deterministic.
        self.id = id ?? "pod:\(pod.id)"
        self.podID = pod.id
        self.namespace = pod.namespace
        self.podName = pod.name
    }

    init(snapshot: TerminalWorkspaceLogTabSnapshot) {
        self.id = snapshot.id
        self.podID = snapshot.podID
        self.namespace = snapshot.namespace
        self.podName = snapshot.podName
    }

    var snapshot: TerminalWorkspaceLogTabSnapshot {
        TerminalWorkspaceLogTabSnapshot(
            id: id,
            podID: podID,
            namespace: namespace,
            podName: podName
        )
    }

    mutating func update(to pod: PodSummary) {
        podID = pod.id
        namespace = pod.namespace
        podName = pod.name
    }
}

struct TerminalPodLogTabState: Equatable {
    var tabs: [TerminalPodLogTab] = []
    var activeTabID: String?
    var selectedPodID = ""
    /// Closed tabs, newest last, for Cmd-Shift-T.
    private(set) var recentlyClosed: [TerminalPodLogTab] = []
    static let recentlyClosedLimit = 10

    var isEmpty: Bool {
        tabs.isEmpty
    }

    func activePod(in pods: [PodSummary], fallback: PodSummary?) -> PodSummary? {
        if let activeTabID,
           let tab = tabs.first(where: { $0.id == activeTabID }),
           let pod = pods.first(where: { $0.id == tab.podID }) {
            return pod
        }
        return pods.first { $0.id == selectedPodID } ?? fallback
    }

    func presentations(
        pods: [PodSummary],
        isFavorite: (PodSummary) -> Bool
    ) -> [TerminalLogTabPresentation] {
        let podsByID = Dictionary(uniqueKeysWithValues: pods.map { ($0.id, $0) })
        // Pinned tabs sit first, as pinned tabs do in Safari and Xcode.
        let ordered = Self.pinnedFirst(tabs) { tab in podsByID[tab.podID].map(isFavorite) ?? false }
        return ordered.map { tab in
            let pod = podsByID[tab.podID]
            let title = pod?.name ?? tab.podName
            let namespace = pod?.namespace ?? tab.namespace
            return TerminalLogTabPresentation(
                id: tab.id,
                podID: tab.podID,
                title: title,
                subtitle: namespace,
                isFavorite: pod.map(isFavorite) ?? false,
                accessibilityLabel: "\(title), namespace \(namespace)",
                helpText: "Logs for \(namespace)/\(title)"
            )
        }
    }

    static func pinnedFirst(
        _ tabs: [TerminalPodLogTab],
        isPinned: (TerminalPodLogTab) -> Bool
    ) -> [TerminalPodLogTab] {
        let pinned = tabs.filter(isPinned)
        guard !pinned.isEmpty, pinned.count < tabs.count else { return tabs }
        return pinned + tabs.filter { !isPinned($0) }
    }

    /// Keeps the stored order equal to the shown order (pinned first), so
    /// closing and reordering act on what the user sees.
    mutating func groupPinnedFirst(isPinned: (TerminalPodLogTab) -> Bool) {
        tabs = Self.pinnedFirst(tabs, isPinned: isPinned)
    }

    /// Drag-and-drop reorder; a pinned tab never drops behind unpinned ones.
    mutating func move(id: String, onto targetID: String, isPinned: (TerminalPodLogTab) -> Bool) {
        groupPinnedFirst(isPinned: isPinned)
        tabs.moveElement(id: id, onto: targetID)
        groupPinnedFirst(isPinned: isPinned)
    }

    mutating func ensureTab(for pod: PodSummary) {
        guard tabs.isEmpty else {
            if activeTabID == nil {
                activeTabID = tabs.first?.id
            }
            return
        }
        let tab = TerminalPodLogTab(pod: pod)
        tabs = [tab]
        activeTabID = tab.id
        selectedPodID = pod.id
    }

    mutating func reconcile(availablePods: [PodSummary], fallbackPod: PodSummary?) {
        guard !availablePods.isEmpty else { return }
        let availableIDs = Set(availablePods.map(\.id))
        tabs.removeAll { !availableIDs.contains($0.podID) }

        if let activeTabID,
           !tabs.contains(where: { $0.id == activeTabID }) {
            self.activeTabID = tabs.first?.id
        }

        if let activeTabID,
           let tab = tabs.first(where: { $0.id == activeTabID }) {
            selectedPodID = tab.podID
        } else if selectedPodID.isEmpty || !availableIDs.contains(selectedPodID) {
            selectedPodID = fallbackPod?.id ?? availablePods.first?.id ?? ""
        }
    }

    mutating func add(preferredPod: PodSummary) {
        if let existing = tabs.first(where: { $0.podID == preferredPod.id }) {
            activeTabID = existing.id
            selectedPodID = preferredPod.id
            return
        }
        let tab = TerminalPodLogTab(pod: preferredPod)
        tabs.append(tab)
        activeTabID = tab.id
        selectedPodID = preferredPod.id
    }

    mutating func activateOrRetarget(for pod: PodSummary) {
        if let existing = tabs.first(where: { $0.podID == pod.id }) {
            activeTabID = existing.id
            selectedPodID = pod.id
            return
        }
        updateActive(to: pod)
    }

    mutating func select(id: String, availablePods: [PodSummary]) -> PodSummary? {
        guard let tab = tabs.first(where: { $0.id == id }),
              let pod = availablePods.first(where: { $0.id == tab.podID }) else {
            return nil
        }
        activeTabID = id
        selectedPodID = pod.id
        return pod
    }

    mutating func close(id: String, availablePods: [PodSummary], fallbackPod: PodSummary?) -> PodSummary? {
        guard let index = tabs.firstIndex(where: { $0.id == id }) else { return nil }
        let closingActiveTab = activeTabID == id
        recentlyClosed.removeAll { $0.podID == tabs[index].podID }
        recentlyClosed.append(tabs[index])
        if recentlyClosed.count > Self.recentlyClosedLimit { recentlyClosed.removeFirst() }
        tabs.remove(at: index)
        guard closingActiveTab else { return nil }

        if tabs.isEmpty {
            activeTabID = nil
            selectedPodID = fallbackPod?.id ?? ""
            return fallbackPod
        }

        let nextIndex = min(index, tabs.count - 1)
        return select(id: tabs[nextIndex].id, availablePods: availablePods)
    }

    /// Reopens the most recently closed tab whose pod still exists, and
    /// selects it; nil when there is none. A tab whose pod is not listed
    /// stays closed but is kept: the pod list may only be reloading.
    mutating func reopenLastClosed(availablePods: [PodSummary]) -> PodSummary? {
        for index in recentlyClosed.indices.reversed() {
            guard let pod = availablePods.first(where: { $0.id == recentlyClosed[index].podID }) else { continue }
            recentlyClosed.remove(at: index)
            add(preferredPod: pod)
            return pod
        }
        return nil
    }

    /// Selects the tab next to the active one, wrapping at the ends.
    mutating func selectAdjacent(offset: Int, availablePods: [PodSummary]) -> PodSummary? {
        guard let id = TerminalTabCycle.adjacentID(in: tabs.map(\.id), active: activeTabID, offset: offset) else { return nil }
        return select(id: id, availablePods: availablePods)
    }

    mutating func updateActive(to pod: PodSummary) {
        if let id = activeTabID,
           let index = tabs.firstIndex(where: { $0.id == id }) {
            tabs[index].update(to: pod)
        } else {
            ensureTab(for: pod)
        }
        selectedPodID = pod.id
    }

    mutating func restore(
        tabs snapshots: [TerminalWorkspaceLogTabSnapshot],
        activeTabID: String?,
        selectedPodID: String?
    ) {
        tabs = snapshots.map(TerminalPodLogTab.init(snapshot:))
        self.activeTabID = activeTabID.flatMap { id in
            tabs.contains(where: { $0.id == id }) ? id : tabs.first?.id
        } ?? tabs.first?.id
        self.selectedPodID = selectedPodID ?? tabs.first(where: { $0.id == self.activeTabID })?.podID ?? ""
    }

    var snapshotTabs: [TerminalWorkspaceLogTabSnapshot] {
        tabs.map(\.snapshot)
    }

    func preferredPodForNewTab(
        pods: [PodSummary],
        fallbackPod: PodSummary?,
        isFavorite: (PodSummary) -> Bool
    ) -> PodSummary? {
        let openPodIDs = Set(tabs.map(\.podID))
        if let fallbackPod, !openPodIDs.contains(fallbackPod.id) {
            return fallbackPod
        }

        return pods.lazy
            .filter { !openPodIDs.contains($0.id) }
            .min { lhs, rhs in
                let lhsFavorite = isFavorite(lhs)
                let rhsFavorite = isFavorite(rhs)
                if lhsFavorite != rhsFavorite {
                    return lhsFavorite && !rhsFavorite
                }
                return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
            }
            ?? fallbackPod
            ?? pods.first
    }
}
