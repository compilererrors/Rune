import Foundation

public enum RuneExecutableSearchPath {
    public static let fallbackDirectories = [
        "/opt/homebrew/bin",
        "/opt/homebrew/sbin",
        "/usr/local/bin",
        "/usr/local/sbin",
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin",
        "/opt/local/bin"
    ]

    public static func directories(
        from environment: [String: String] = ProcessInfo.processInfo.environment
    ) -> [String] {
        let pathDirectories = (environment["PATH"] ?? "")
            .split(separator: ":")
            .map(String.init)
            .filter { !$0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }
        return unique(pathDirectories + fallbackDirectories)
    }

    public static func pathValue(
        from environment: [String: String] = ProcessInfo.processInfo.environment
    ) -> String {
        directories(from: environment).joined(separator: ":")
    }

    private static func unique(_ directories: [String]) -> [String] {
        var seen = Set<String>()
        return directories.filter { directory in
            let expanded = NSString(string: directory).expandingTildeInPath
            return seen.insert(expanded).inserted
        }
    }
}
