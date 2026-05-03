import AppIntents
import Foundation

struct OpenRuneIntent: AppIntent {
    static let title: LocalizedStringResource = "Open Rune"
    static let description = IntentDescription("Open Rune.")
    static let openAppWhenRun = true

    func perform() async throws -> some IntentResult {
        .result()
    }
}

struct OpenRuneCommandPaletteIntent: AppIntent {
    static let title: LocalizedStringResource = "Open Rune Command Palette"
    static let description = IntentDescription("Open Rune and use the command palette.")
    static let openAppWhenRun = true

    func perform() async throws -> some IntentResult {
        .result()
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
    }
}
