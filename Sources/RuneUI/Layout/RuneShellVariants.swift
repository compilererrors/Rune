import Foundation

enum RuneRootShellVariant: String, CaseIterable, Sendable {
    case navigationSplitView = "navigation"
    case appKitSplitView = "appkit"

    var debugLabel: String {
        switch self {
        case .navigationSplitView:
            return "NavigationSplitView"
        case .appKitSplitView:
            return "HSplitView(NSSplitView)"
        }
    }

    static func resolved(override: RuneRootShellVariant?) -> RuneRootShellVariant {
        if let override {
            return override
        }

        guard let envValue = ProcessInfo.processInfo.environment["RUNE_LAYOUT_SHELL"]?
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
        else {
            // Default to the AppKit-backed split shell: it matches the desired three-column
            // structure but has proven stable in live runs where NavigationSplitView still drifts.
            return .appKitSplitView
        }

        return RuneRootShellVariant(rawValue: envValue) ?? .appKitSplitView
    }
}

enum ManifestInlineEditorImplementation: String, CaseIterable, Sendable {
    case readOnlyScroll = "read-only-scroll"
    case swiftUITextEditor = "swiftui-texteditor"
    case appKitTextView = "appkit-textview"

    var debugLabel: String {
        switch self {
        case .readOnlyScroll:
            return "ScrollView+Text"
        case .swiftUITextEditor:
            return "SwiftUI TextEditor"
        case .appKitTextView:
            return "AppKit NSTextView"
        }
    }

    var supportsInlineEditing: Bool {
        switch self {
        case .readOnlyScroll:
            return false
        case .swiftUITextEditor, .appKitTextView:
            return true
        }
    }

    static func resolved(override: ManifestInlineEditorImplementation?) -> ManifestInlineEditorImplementation {
        if let override {
            return override
        }

        guard let envValue = ProcessInfo.processInfo.environment["RUNE_MANIFEST_INLINE_EDITOR"]?
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
        else {
            return .swiftUITextEditor
        }

        return ManifestInlineEditorImplementation(rawValue: envValue) ?? .swiftUITextEditor
    }
}
