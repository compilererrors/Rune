import Foundation

/// What Cmd-W closes in the Terminal section: the active tab of the strip
/// you are in (the log tabs in the inspector, else the shell tabs), when
/// that strip has more than one tab. With one tab left, Cmd-W closes the
/// window as everywhere else in macOS.
enum TerminalCloseTabPolicy {
    enum Target: Equatable {
        case logTab(String)
        case shellSession(String)
        /// The unsaved "new shell tab" beside existing sessions.
        case shellDraft
    }

    struct Strips: Equatable {
        var logTabIDs: [String]
        var activeLogTabID: String?
        var shellSessionIDs: [String]
        var activeShellSessionID: String?
        var isComposingShellDraft: Bool
    }

    /// Whether Cmd-W is about the log tabs: the focused view is in the
    /// inspector, or nothing has focus and the inspector is the pane last
    /// clicked or tabbed to.
    static func logsHaveFocus(responderIsInInspector: Bool?, paneFocusIsInspector: Bool) -> Bool {
        responderIsInInspector ?? paneFocusIsInspector
    }

    static func target(logsFocused: Bool, logsVisible: Bool, strips: Strips) -> Target? {
        if logsFocused, logsVisible {
            guard strips.logTabIDs.count > 1,
                  let active = strips.activeLogTabID,
                  strips.logTabIDs.contains(active) else { return nil }
            return .logTab(active)
        }
        let hasDraftTab = strips.isComposingShellDraft && !strips.shellSessionIDs.isEmpty
        guard strips.shellSessionIDs.count + (hasDraftTab ? 1 : 0) > 1 else { return nil }
        if hasDraftTab { return .shellDraft }
        guard let active = strips.activeShellSessionID,
              strips.shellSessionIDs.contains(active) else { return nil }
        return .shellSession(active)
    }
}
