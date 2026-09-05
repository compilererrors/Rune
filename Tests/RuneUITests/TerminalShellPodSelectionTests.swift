import XCTest
@testable import RuneCore
@testable import RuneUI

final class TerminalShellPodSelectionTests: XCTestCase {
    func testVisibleTerminalSessionsAreStrictlyScopedToContextAndNamespace() {
        let allSessions = [
            PodTerminalSession(id: "matching", contextName: "context-alpha", namespace: "namespace-alpha", podName: "pod-alpha", shell: "sh", status: .connected),
            PodTerminalSession(id: "other-context", contextName: "context-beta", namespace: "namespace-alpha", podName: "pod-alpha", shell: "sh", status: .connected),
            PodTerminalSession(id: "other-namespace", contextName: "context-alpha", namespace: "namespace-beta", podName: "pod-alpha", shell: "sh", status: .connected)
        ]

        let scoped = TerminalShellPodSelectionPolicy.sessions(
            in: allSessions,
            contextName: "context-alpha",
            namespace: "namespace-alpha"
        )

        XCTAssertEqual(scoped.map(\.id), ["matching"])
    }

    func testNewShellCompositionPreservesDropdownPodChoiceWithoutExistingSession() {
        let api = pod("api")
        let worker = pod("worker")
        let sessions = [
            session(id: "shell-api", pod: api)
        ]

        let preferred = TerminalShellPodSelectionPolicy.preferredPodIDForNewShell(
            selectedPod: api,
            availablePods: [api, worker],
            sessions: sessions,
            currentSelectionID: worker.id
        )

        XCTAssertEqual(
            preferred,
            worker.id,
            "Choosing worker in the terminal shell dropdown must not be overwritten by the active/selected api pod or by terminal log tab state."
        )
    }

    func testTerminalShellDropdownSelectionIsIndependentFromTerminalLogTabSelection() {
        let api = pod("api")
        let worker = pod("worker")
        var logTabs = TerminalPodLogTabState()
        logTabs.add(preferredPod: api)

        let shellPreferred = TerminalShellPodSelectionPolicy.preferredPodIDForNewShell(
            selectedPod: api,
            availablePods: [api, worker],
            sessions: [session(id: "shell-api", pod: api)],
            currentSelectionID: worker.id
        )

        XCTAssertEqual(logTabs.selectedPodID, api.id)
        XCTAssertEqual(
            shellPreferred,
            worker.id,
            "Terminal shell dropdown state must be independent from the right-panel terminal log tab pod selection."
        )
    }

    func testNewShellButtonStillFallsBackToNextPodWhenCurrentSelectionAlreadyHasSession() {
        let api = pod("api")
        let worker = pod("worker")
        let sessions = [
            session(id: "shell-api", pod: api)
        ]

        let preferred = TerminalShellPodSelectionPolicy.preferredPodIDForNewShell(
            selectedPod: api,
            availablePods: [api, worker],
            sessions: sessions,
            currentSelectionID: api.id
        )

        XCTAssertEqual(preferred, worker.id)
    }

    func testLogHandoffPrefersMatchingContainerThenDefaultSessionForExactPodScope() {
        let api = pod("api")
        let defaultSession = session(id: "shell-api-default", pod: api)
        let appSession = PodTerminalSession(
            id: "shell-api-app",
            contextName: "test-context",
            namespace: api.namespace,
            podName: api.name,
            containerName: "app",
            shell: "/bin/sh",
            status: .connected
        )
        let otherContext = PodTerminalSession(
            id: "shell-other-context",
            contextName: "other-context",
            namespace: api.namespace,
            podName: api.name,
            containerName: "app",
            shell: "/bin/sh",
            status: .connected
        )

        XCTAssertEqual(
            TerminalShellPodSelectionPolicy.preferredSessionIDForHandoff(
                pod: api,
                sessions: [defaultSession, otherContext, appSession],
                contextName: "test-context",
                preferredContainer: " app "
            ),
            appSession.id
        )
        XCTAssertEqual(
            TerminalShellPodSelectionPolicy.preferredSessionIDForHandoff(
                pod: api,
                sessions: [otherContext, appSession, defaultSession],
                contextName: "test-context",
                preferredContainer: "missing"
            ),
            defaultSession.id
        )
        XCTAssertNil(
            TerminalShellPodSelectionPolicy.preferredSessionIDForHandoff(
                pod: api,
                sessions: [defaultSession],
                contextName: "missing-context",
                preferredContainer: nil
            )
        )
    }

    func testTerminalAndRightPanelLogFavoriteTogglesShareMarkAndUnmarkState() {
        let api = pod("api")
        let worker = pod("worker")
        let pods = [api, worker]
        var favoritePodIDs = Set<String>()
        let isFavorite: (PodSummary) -> Bool = { favoritePodIDs.contains($0.id) }
        let toggleFavorite: (PodSummary) -> Void = { pod in
            if favoritePodIDs.contains(pod.id) {
                favoritePodIDs.remove(pod.id)
            } else {
                favoritePodIDs.insert(pod.id)
            }
        }

        XCTAssertEqual(
            FavoritePodPickerPresentation.selectedFavoriteIcon(in: pods, selection: api.id, isFavoritePod: isFavorite),
            "star"
        )

        toggleFavorite(api)

        XCTAssertEqual(
            FavoritePodPickerPresentation.selectedFavoriteIcon(in: pods, selection: api.id, isFavoritePod: isFavorite),
            "star.fill",
            "Terminal picker should show the selected pod as favorited after toggling the terminal star on."
        )

        toggleFavorite(api)

        XCTAssertEqual(
            FavoritePodPickerPresentation.selectedFavoriteIcon(in: pods, selection: api.id, isFavoritePod: isFavorite),
            "star",
            "Terminal picker should unmark the selected pod after toggling the terminal star off."
        )

        toggleFavorite(worker)
        XCTAssertEqual(
            FavoritePodPickerPresentation.selectedFavoriteIcon(in: pods, selection: worker.id, isFavoritePod: isFavorite),
            "star.fill",
            "Right-panel log picker selected-star state should mark on."
        )

        toggleFavorite(worker)
        XCTAssertEqual(
            FavoritePodPickerPresentation.selectedFavoriteIcon(in: pods, selection: worker.id, isFavoritePod: isFavorite),
            "star",
            "Right-panel log picker selected-star state should mark off."
        )
    }

    private func pod(_ name: String) -> PodSummary {
        PodSummary(name: name, namespace: "default", status: "Running", containerNamesLine: "app")
    }

    private func session(id: String, pod: PodSummary) -> PodTerminalSession {
        PodTerminalSession(
            id: id,
            contextName: "test-context",
            namespace: pod.namespace,
            podName: pod.name,
            shell: "/bin/sh",
            status: .connected
        )
    }
}
