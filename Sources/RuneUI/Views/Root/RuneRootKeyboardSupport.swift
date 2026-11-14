import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

enum RuneRootKeyboardPane: CaseIterable, Equatable {
    case sidebarSections
    case sidebarContexts
    case content
    case detail
}

struct RuneRootKeyboardPaneNavigation {
    static func availablePanes(
        sidebarVisible: Bool,
        detailVisible: Bool,
        skipsClusterPane: Bool
    ) -> [RuneRootKeyboardPane] {
        var panes: [RuneRootKeyboardPane] = []
        if sidebarVisible {
            panes.append(.sidebarSections)
            if !skipsClusterPane {
                panes.append(.sidebarContexts)
            }
        }
        panes.append(.content)
        if detailVisible {
            panes.append(.detail)
        }
        return panes
    }

    static func advanced(
        from current: RuneRootKeyboardPane,
        forward: Bool,
        in panes: [RuneRootKeyboardPane]
    ) -> RuneRootKeyboardPane {
        guard let index = panes.firstIndex(of: current) else {
            return panes.first ?? .content
        }
        if forward {
            return panes[(index + 1) % panes.count]
        }
        return panes[(index + panes.count - 1) % panes.count]
    }
}

struct RuneRootKeyboardWindowScope {
    static func owns(
        eventWindowNumber: Int,
        workspaceWindowNumber: Int?,
        keyWindowNumber: Int?
    ) -> Bool {
        guard let workspaceWindowNumber else { return false }
        if eventWindowNumber == workspaceWindowNumber {
            return true
        }
        // Quartz/Accessibility-posted key events can arrive without a window number.
        // In that case, only the workspace that owns the key window may handle them.
        return eventWindowNumber == 0 && keyWindowNumber == workspaceWindowNumber
    }
}

enum RuneRootTextInputFocus: Hashable {
    case contextSearch
    case resourceFilter
}

/// Keeps the hosting window identity out of SwiftUI's render state. Window attachment can
/// happen after the first layout pass; publishing that identity as `@State` needlessly
/// invalidates the entire three-pane workspace while it is settling.
final class RuneWorkspaceWindowReference {
    var windowNumber: Int?
    var measuredTopInset: CGFloat?
}

struct RuneRootKeyboardNavigationContext {
    let commandPalettePresented: Bool
    let yamlEditorSheetPresented: Bool
    let yamlManifestIsEditing: Bool
    let kubeConfigImportReviewPresented: Bool
    let addClusterPopoverPresented: Bool
    let addClusterProviderPresented: Bool
    let manualNamespaceSheetPresented: Bool
    let pendingWriteConfirmationPresented: Bool
    let launchExperiencePresented: Bool
    let appSheetAttached: Bool
    let editableTextResponderActive: Bool

    var hasBlockingPresentation: Bool {
        commandPalettePresented
            || yamlEditorSheetPresented
            || yamlManifestIsEditing
            || kubeConfigImportReviewPresented
            || addClusterPopoverPresented
            || addClusterProviderPresented
            || manualNamespaceSheetPresented
            || pendingWriteConfirmationPresented
            || launchExperiencePresented
            || appSheetAttached
    }

    var isSuspended: Bool {
        hasBlockingPresentation || editableTextResponderActive
    }
}

@MainActor
final class RuneScopedLatestRequestGate {
    struct Token: Equatable, Sendable {
        let scopeGeneration: UInt64
        let requestGeneration: UInt64
    }

    private(set) var scopeGeneration: UInt64 = 0
    private var requestGeneration: UInt64 = 0
    private var latestToken: Token?

    @discardableResult
    func advanceScope() -> UInt64 {
        scopeGeneration &+= 1
        requestGeneration &+= 1
        latestToken = nil
        return scopeGeneration
    }

    func begin(expectedScopeGeneration: UInt64? = nil) -> Token? {
        if let expectedScopeGeneration,
           expectedScopeGeneration != scopeGeneration {
            return nil
        }
        requestGeneration &+= 1
        let token = Token(
            scopeGeneration: scopeGeneration,
            requestGeneration: requestGeneration
        )
        latestToken = token
        return token
    }

    func isCurrent(_ token: Token) -> Bool {
        latestToken == token && token.scopeGeneration == scopeGeneration
    }
}
