import Foundation
import RuneCore
import RuneFakeK8sSupport
import RuneSecurity
import RuneStore
@testable import RuneUI
import XCTest

@MainActor
final class CustomLogSettingsReloadTests: XCTestCase {
    func testChangingActiveCustomLinePresetReloadsLogsWithNewTailCount() async throws {
        let defaults = UserDefaults.standard
        let keys = [
            RuneSettingsKeys.logsCustomPresetOneMode,
            RuneSettingsKeys.logsCustomPresetOneLines,
            RuneSettingsKeys.logsCustomPresetOneTimeValue,
            RuneSettingsKeys.logsCustomPresetOneTimeUnit
        ]
        let savedValues = keys.map { (key: $0, value: defaults.object(forKey: $0)) }
        defer {
            for savedValue in savedValues {
                if let value = savedValue.value {
                    defaults.set(value, forKey: savedValue.key)
                } else {
                    defaults.removeObject(forKey: savedValue.key)
                }
            }
        }

        defaults.set(RuneCustomLogPresetMode.lines.rawValue, forKey: RuneSettingsKeys.logsCustomPresetOneMode)
        defaults.set("17", forKey: RuneSettingsKeys.logsCustomPresetOneLines)
        defaults.set("1", forKey: RuneSettingsKeys.logsCustomPresetOneTimeValue)
        defaults.set(
            RuneCustomLogPresetTimeUnit.minutes.rawValue,
            forKey: RuneSettingsKeys.logsCustomPresetOneTimeUnit
        )

        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }
        let contextDefaultsSuite = "CustomLogSettingsReloadTests.\(UUID().uuidString)"
        let contextDefaults = try XCTUnwrap(
            UserDefaults(suiteName: contextDefaultsSuite)
        )
        defer { contextDefaults.removePersistentDomain(forName: contextDefaultsSuite) }

        do {
            let state = RuneAppState()
            let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
            let pod = PodSummary(
                name: "orbit-lens-6f58d7d89b-hx9q2",
                namespace: "alpha-zone",
                status: "Running",
                containerNamesLine: "lens"
            )
            state.setSources([KubeConfigSource(url: kubeconfig)])
            state.selectedContext = context
            state.selectedNamespace = "alpha-zone"
            state.selectedWorkloadKind = .pod
            state.setPods([pod])
            state.setSelectedPod(pod)

            let viewModel = RuneAppViewModel(
                state: state,
                bookmarkManager: BookmarkManager(store: EmptyCustomLogBookmarkStore()),
                contextPreferences: UserDefaultsContextPreferencesStore(defaults: contextDefaults),
                savedWorkspaceStore: EmptyCustomLogSavedWorkspaceStore(),
                overviewSnapshotPersistence: NoopOverviewSnapshotCacheStore(),
                namespaceListPersistence: NoopNamespaceListPersistenceStore()
            )
            viewModel.selectedLogContainer = "lens"
            viewModel.selectedLogPreset = .customOne

            try await waitUntil {
                server.requestLines().contains {
                    $0.contains("/pods/\(pod.name)/log?")
                        && $0.contains("tailLines=17")
                }
            }

            server.resetRequestLines()
            defaults.set("23", forKey: RuneSettingsKeys.logsCustomPresetOneLines)
            NotificationCenter.default.post(
                name: UserDefaults.didChangeNotification,
                object: defaults
            )

            try await waitUntil {
                server.requestLines().contains {
                    $0.contains("/pods/\(pod.name)/log?")
                        && $0.contains("tailLines=23")
                }
            }
        }

    }

    private func writeKubeconfig(_ contents: String) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("custom-log-settings-\(UUID().uuidString).yaml")
        try contents.write(to: url, atomically: true, encoding: .utf8)
        return url
    }

    private func waitUntil(
        timeoutNanoseconds: UInt64 = 4_000_000_000,
        condition: @escaping @MainActor () -> Bool
    ) async throws {
        let start = ContinuousClock.now
        while !condition() {
            if start.duration(to: .now) > .nanoseconds(Int64(timeoutNanoseconds)) {
                XCTFail("Timed out waiting for the active custom log preset to reload.")
                return
            }
            try await Task.sleep(nanoseconds: 20_000_000)
        }
    }
}

private struct EmptyCustomLogBookmarkStore: BookmarkStore {
    func loadRecords() throws -> [BookmarkRecord] { [] }
    func saveRecords(_ records: [BookmarkRecord]) throws {}
}

private struct EmptyCustomLogSavedWorkspaceStore: SavedWorkspaceStoring {
    func loadSavedWorkspaces() -> [SavedWorkspaceSnapshot] { [] }
    func saveSavedWorkspaces(_ workspaces: [SavedWorkspaceSnapshot]) {}
}
