import Foundation

/// Preference suites for tests.
///
/// A suite named by an absolute path is kept in that file. macOS leaves an
/// empty plist in ~/Library/Preferences for every named suite, even after its
/// domain is removed, so test suites live in a temporary folder instead and
/// their file is deleted when the test is done.
enum TestDefaultsSuite {
    static let directory: URL = {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("RuneTestDefaults", isDirectory: true)
        try? FileManager.default.createDirectory(at: url, withIntermediateDirectories: true)
        return url
    }()

    /// A suite name whose plist lives in the temporary test folder.
    static func name(_ name: String) -> String {
        directory.appendingPathComponent(name).path
    }

    static func plistURL(forSuiteNamed suiteName: String) -> URL {
        if suiteName.hasPrefix("/") {
            return URL(fileURLWithPath: suiteName + ".plist")
        }
        return FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Preferences/\(suiteName).plist")
    }
}

extension UserDefaults {
    /// Empties a test suite and deletes its plist.
    func removeTestSuite(named suiteName: String) {
        removePersistentDomain(forName: suiteName)
        try? FileManager.default.removeItem(at: TestDefaultsSuite.plistURL(forSuiteNamed: suiteName))
    }
}
