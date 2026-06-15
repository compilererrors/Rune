import AppIntents
import Foundation
import RuneCore

struct OpenRuneIntent: AppIntent {
    static let title: LocalizedStringResource = "Open Rune"
    static let description = IntentDescription("Open Rune.")
    static let openAppWhenRun = true

    func perform() async throws -> some IntentResult {
        UserDefaults.standard.runePendingLaunchAction = Optional<RuneLaunchAction>.none
        return .result()
    }
}

struct OpenRuneCommandPaletteIntent: AppIntent {
    static let title: LocalizedStringResource = "Open Rune Command Palette"
    static let description = IntentDescription("Open Rune and use the command palette.")
    static let openAppWhenRun = true

    func perform() async throws -> some IntentResult {
        UserDefaults.standard.runePendingLaunchAction = .commandPalette
        return .result()
    }
}

struct OpenRuneAuthDoctorIntent: AppIntent {
    static let title: LocalizedStringResource = "Run Rune Auth Doctor"
    static let description = IntentDescription("Open Rune and start Auth Doctor.")
    static let openAppWhenRun = true

    func perform() async throws -> some IntentResult {
        UserDefaults.standard.runePendingLaunchAction = .authDoctor
        return .result()
    }
}

struct OpenRuneSavedWorkspacesIntent: AppIntent {
    static let title: LocalizedStringResource = "Open Rune Saved Workspaces"
    static let description = IntentDescription("Open Rune and show saved workspaces.")
    static let openAppWhenRun = true

    func perform() async throws -> some IntentResult {
        UserDefaults.standard.runePendingLaunchAction = .savedWorkspaces
        return .result()
    }
}

struct OpenRuneRecentContextsIntent: AppIntent {
    static let title: LocalizedStringResource = "Open Rune Recent Contexts"
    static let description = IntentDescription("Open Rune and show contexts.")
    static let openAppWhenRun = true

    func perform() async throws -> some IntentResult {
        UserDefaults.standard.runePendingLaunchAction = .recentContexts
        return .result()
    }
}

struct OpenRuneContextIntent: AppIntent {
    static let title: LocalizedStringResource = "Open Rune Context"
    static let description = IntentDescription("Open Rune and search for a Kubernetes context.")
    static let openAppWhenRun = true

    @Parameter(title: "Context Name")
    var contextName: String

    static var parameterSummary: some ParameterSummary {
        Summary("Open \(\.$contextName) in Rune")
    }

    func perform() async throws -> some IntentResult {
        UserDefaults.standard.setRunePendingLaunchRequest(
            RunePendingLaunchRequest(action: .recentContexts, query: contextName)
        )
        return .result()
    }
}

struct OpenRuneWorkspaceIntent: AppIntent {
    static let title: LocalizedStringResource = "Open Rune Workspace"
    static let description = IntentDescription("Open Rune and search for a saved workspace.")
    static let openAppWhenRun = true

    @Parameter(title: "Workspace Name")
    var workspaceName: String

    static var parameterSummary: some ParameterSummary {
        Summary("Open \(\.$workspaceName) in Rune")
    }

    func perform() async throws -> some IntentResult {
        UserDefaults.standard.setRunePendingLaunchRequest(
            RunePendingLaunchRequest(action: .savedWorkspaces, query: workspaceName)
        )
        return .result()
    }
}

struct RuneShortcutsProvider: AppShortcutsProvider {
    static var appShortcuts: [AppShortcut] {
        AppShortcut(
            intent: OpenRuneIntent(),
            phrases: [
                "Open \(.applicationName)",
                "Show \(.applicationName)"
            ],
            shortTitle: "Open Rune",
            systemImageName: "shippingbox"
        )

        AppShortcut(
            intent: OpenRuneCommandPaletteIntent(),
            phrases: [
                "Search \(.applicationName)",
                "Open command palette in \(.applicationName)"
            ],
            shortTitle: "Search Rune",
            systemImageName: "magnifyingglass"
        )

        AppShortcut(
            intent: OpenRuneAuthDoctorIntent(),
            phrases: [
                "Run auth doctor in \(.applicationName)",
                "Check access in \(.applicationName)"
            ],
            shortTitle: "Auth Doctor",
            systemImageName: "stethoscope"
        )

        AppShortcut(
            intent: OpenRuneSavedWorkspacesIntent(),
            phrases: [
                "Open saved workspaces in \(.applicationName)",
                "Show workspaces in \(.applicationName)"
            ],
            shortTitle: "Workspaces",
            systemImageName: "rectangle.stack.badge.play"
        )

        AppShortcut(
            intent: OpenRuneRecentContextsIntent(),
            phrases: [
                "Open recent contexts in \(.applicationName)",
                "Show contexts in \(.applicationName)"
            ],
            shortTitle: "Contexts",
            systemImageName: "network"
        )

        AppShortcut(
            intent: OpenRuneContextIntent(),
            phrases: [
                "Open \(\.$contextName) in \(.applicationName)",
                "Show context \(\.$contextName) in \(.applicationName)"
            ],
            shortTitle: "Open Context",
            systemImageName: "network"
        )

        AppShortcut(
            intent: OpenRuneWorkspaceIntent(),
            phrases: [
                "Open workspace \(\.$workspaceName) in \(.applicationName)",
                "Show workspace \(\.$workspaceName) in \(.applicationName)"
            ],
            shortTitle: "Open Workspace",
            systemImageName: "rectangle.stack.badge.play"
        )
    }
}
