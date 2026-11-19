import AppKit
import XCTest

/// Settings a test reads through `UserDefaults.standard`, kept in the
/// volatile argument domain for the length of one test.
///
/// A persistent write lands in the test runner's own preferences
/// (`com.apple.dt.xctest.tool`), and a run that is killed before the test
/// puts the old value back leaves it there for every later run. The argument
/// domain lives in the process only and is searched first.
@MainActor
final class VolatileTestDefaults {
    private static let scrollBarsKey = "AppleShowScrollBars"
    private let original: [String: Any]
    private let originalScrollerStyle: NSScroller.Style

    init(_ testCase: XCTestCase) {
        original = UserDefaults.standard.volatileDomain(forName: UserDefaults.argumentDomain)
        originalScrollerStyle = NSScroller.preferredScrollerStyle
        testCase.addTeardownBlock { @MainActor [self] in
            replaceArgumentDomain(original)
            try await applyScrollerStyle(originalScrollerStyle)
        }
    }

    func set(_ value: Any, forKey key: String) {
        var domain = UserDefaults.standard.volatileDomain(forName: UserDefaults.argumentDomain)
        domain[key] = value
        replaceArgumentDomain(domain)
    }

    /// "Always" or "WhenScrolling". AppKit reads the setting again only when
    /// told that it changed, so this posts the notification System Settings
    /// posts and waits until the preferred style follows.
    func setScrollBars(_ value: String) async throws {
        set(value, forKey: Self.scrollBarsKey)
        try await applyScrollerStyle(value == "Always" ? .legacy : .overlay)
    }

    private func replaceArgumentDomain(_ domain: [String: Any]) {
        UserDefaults.standard.removeVolatileDomain(forName: UserDefaults.argumentDomain)
        UserDefaults.standard.setVolatileDomain(domain, forName: UserDefaults.argumentDomain)
    }

    private func applyScrollerStyle(_ style: NSScroller.Style) async throws {
        DistributedNotificationCenter.default().postNotificationName(
            Notification.Name("AppleShowScrollBarsSettingChanged"),
            object: nil,
            userInfo: nil,
            deliverImmediately: true
        )
        let deadline = Date().addingTimeInterval(3)
        while NSScroller.preferredScrollerStyle != style, Date() < deadline {
            try await Task.sleep(nanoseconds: 20_000_000)
        }
        XCTAssertEqual(NSScroller.preferredScrollerStyle, style, "the preferred scroller style followed the setting")
    }
}
