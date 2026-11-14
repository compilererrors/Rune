import Foundation

/// Source text of the root view, which is split across `Sources/RuneUI/Views/Root`.
/// Files are joined in their reading order so block slices keep working.
enum RuneRootViewSource {
    static let fileNames = [
        "RuneRootLayoutDebug.swift",
        "RuneInspectorTabs.swift",
        "RuneAddClusterSupport.swift",
        "RuneHelmInspectorTabs.swift",
        "RuneInspectorRefreshRouting.swift",
        "RuneInspectorTabPresentation.swift",
        "PodTableLayout.swift",
        "RuneRootKeyboardSupport.swift",
        "RuneRootView.swift",
        "RuneRootView+Lifecycle.swift",
        "RuneRootView+WindowScaffold.swift",
        "RuneRootView+LiveDebugScenario.swift",
        "RuneRootView+SplitLayout.swift",
        "RuneRootView+Sidebar.swift",
        "RuneRootView+AddClusterProvider.swift",
        "RuneRootView+ContentHeader.swift",
        "RuneRootView+SectionPanes.swift",
        "RuneRootView+DetailPane.swift",
        "RuneRootView+ResourceInspectors.swift",
        "RuneRootView+ManifestInspector.swift",
        "RuneRootView+Terminal.swift",
        "RuneRootView+ResourceList.swift",
        "RuneRootView+KeyboardNavigation.swift",
        "RuneRootView+KeyBindingActions.swift",
        "RuneRootView+Export.swift",
        "RuneRootView+SelectionActions.swift",
        "RuneRootView+LayoutPersistence.swift",
        "RuneRootView+StatusPanels.swift",
        "RuneRootView+InspectorHelpers.swift"
    ]

    static var directory: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .appendingPathComponent("Sources/RuneUI/Views/Root", isDirectory: true)
    }

    static func text() throws -> String {
        try fileNames
            .map { try String(contentsOf: directory.appendingPathComponent($0), encoding: .utf8) }
            .joined(separator: "\n")
    }
}
