import AppKit

/// Takes a test's window down when the test is done with it.
///
/// A window that is only ordered out keeps its whole view tree alive. That
/// tree goes on answering every settings change and timer for the rest of
/// the test process, which slows and skews the tests that run after it.
enum RuneTestWindow {
    static func close(_ window: NSWindow?) {
        MainActor.assumeIsolated {
            guard let window else { return }
            window.orderOut(nil)
            window.contentViewController = nil
            window.contentView = nil
            // Under ARC a window that releases itself on close is released twice.
            window.isReleasedWhenClosed = false
            window.close()
        }
    }
}
