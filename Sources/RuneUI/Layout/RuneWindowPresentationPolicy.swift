import AppKit

/// What the app does to its main window when it is activated or its Dock
/// icon is clicked, decided from plain window facts so it can be tested.
///
/// Native rules: activating the app leaves windows as they are; a Dock click
/// brings back a minimized window by deminiaturizing it and nothing else; a
/// window is moved to the active Space only when it is on screen elsewhere.
/// Hiding a window (`orderOut`) while it deminiaturizes left it neither
/// minimized nor visible, so the window could not be brought back.
public enum RuneWindowPresentationPolicy {
    public struct Window: Equatable {
        public var title: String
        public var isPanel: Bool
        public var canBecomeMain: Bool
        public var isVisible: Bool
        public var isMiniaturized: Bool
        public var isOnActiveSpace: Bool
        /// Settings and other windows that are not the app's main scene.
        public var isAuxiliary: Bool

        public init(title: String, isPanel: Bool = false, isAuxiliary: Bool = false, canBecomeMain: Bool = true, isVisible: Bool, isMiniaturized: Bool = false, isOnActiveSpace: Bool = true) {
            self.title = title
            self.isPanel = isPanel
            self.isAuxiliary = isAuxiliary
            self.canBecomeMain = canBecomeMain
            self.isVisible = isVisible
            self.isMiniaturized = isMiniaturized
            self.isOnActiveSpace = isOnActiveSpace
        }

        @MainActor
        public init(_ window: NSWindow) {
            self.init(
                title: window.title,
                isPanel: window is NSPanel,
                isAuxiliary: window.identifier?.rawValue.localizedCaseInsensitiveContains("settings") ?? false,
                canBecomeMain: window.canBecomeMain,
                isVisible: window.isVisible,
                isMiniaturized: window.isMiniaturized,
                isOnActiveSpace: window.isOnActiveSpace
            )
        }
    }

    public enum Action: Equatable {
        /// Leave the windows alone.
        case none
        case deminiaturize(index: Int)
        /// Bring a window that is on screen to the front.
        case makeKey(index: Int)
        /// Bring a window shown on another Space to this one.
        case moveToActiveSpace(index: Int)
        /// No main window exists: open one.
        case openMainWindow
    }

    /// The main window: a regular window that can become main, preferring
    /// the one titled like the app.
    public static func mainWindowIndex(in windows: [Window], title: String) -> Int? {
        let candidates = windows.indices.filter { !windows[$0].isPanel && !windows[$0].isAuxiliary && windows[$0].canBecomeMain }
        return candidates.first { windows[$0].title == title } ?? candidates.first
    }

    /// A Dock click (reopen).
    public static func reopen(windows: [Window], title: String) -> Action {
        guard let index = mainWindowIndex(in: windows, title: title) else { return .openMainWindow }
        return present(windows[index], at: index)
    }

    /// Launch, once the first window should be up.
    public static func launch(windows: [Window], title: String) -> Action {
        reopen(windows: windows, title: title)
    }

    /// Activation (Cmd-Tab, clicking a window): never move or restore
    /// windows; only open one if the app has none at all.
    public static func activate(windows: [Window], title: String) -> Action {
        mainWindowIndex(in: windows, title: title) == nil ? .openMainWindow : .none
    }

    private static func present(_ window: Window, at index: Int) -> Action {
        if window.isMiniaturized { return .deminiaturize(index: index) }
        if window.isVisible, !window.isOnActiveSpace { return .moveToActiveSpace(index: index) }
        return .makeKey(index: index)
    }
}
