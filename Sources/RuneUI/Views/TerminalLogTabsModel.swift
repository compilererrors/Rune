import Foundation
import RuneCore

struct TerminalPodLogTab: Identifiable, Hashable {
    let id: String
    var podID: String
    var namespace: String
    var podName: String

    init(pod: PodSummary, id: String = UUID().uuidString) {
        self.id = id
        self.podID = pod.id
        self.namespace = pod.namespace
        self.podName = pod.name
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
        tabs.map { tab in
            let pod = pods.first { $0.id == tab.podID }
            let title = pod?.name ?? tab.podName
            let namespace = pod?.namespace ?? tab.namespace
            return TerminalLogTabPresentation(
                id: tab.id,
                title: title,
                subtitle: namespace,
                isFavorite: pod.map(isFavorite) ?? false,
                accessibilityLabel: "\(title), namespace \(namespace)",
                helpText: "Logs for \(namespace)/\(title)"
            )
        }
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
        let tab = TerminalPodLogTab(pod: preferredPod)
        tabs.append(tab)
        activeTabID = tab.id
        selectedPodID = preferredPod.id
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

    mutating func updateActive(to pod: PodSummary) {
        if let id = activeTabID,
           let index = tabs.firstIndex(where: { $0.id == id }) {
            tabs[index].update(to: pod)
        } else {
            ensureTab(for: pod)
        }
        selectedPodID = pod.id
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

        return pods
            .sorted { lhs, rhs in
                let lhsFavorite = isFavorite(lhs)
                let rhsFavorite = isFavorite(rhs)
                if lhsFavorite != rhsFavorite {
                    return lhsFavorite && !rhsFavorite
                }
                return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
            }
            .first { !openPodIDs.contains($0.id) }
            ?? fallbackPod
            ?? pods.first
    }
}
