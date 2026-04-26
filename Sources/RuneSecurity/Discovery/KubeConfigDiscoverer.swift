import Foundation

public protocol KubeConfigDiscovering {
    func discoverCandidateFiles() -> [URL]
}

public struct KubeConfigDiscoverer: KubeConfigDiscovering {
    private static let disableDefaultConfigDiscoveryVariable = "RUNE_DISABLE_DEFAULT_KUBECONFIG_DISCOVERY"
    private static let isolatedKubeconfigVariable = "RUNE_ISOLATED_KUBECONFIG"
    private let environmentProvider: () -> [String: String]
    private let homeDirectoryProvider: () -> URL
    private let fileExists: (String) -> Bool

    public init(
        environmentProvider: @escaping () -> [String: String] = { ProcessInfo.processInfo.environment },
        homeDirectoryProvider: @escaping () -> URL = { FileManager.default.homeDirectoryForCurrentUser },
        fileExists: @escaping (String) -> Bool = { path in FileManager.default.fileExists(atPath: path) }
    ) {
        self.environmentProvider = environmentProvider
        self.homeDirectoryProvider = homeDirectoryProvider
        self.fileExists = fileExists
    }

    public func discoverCandidateFiles() -> [URL] {
        var candidates: [URL] = []
        let environment = environmentProvider()

        if let isolatedKubeconfig = Self.isolatedKubeconfigPath(environment: environment) {
            let expanded = NSString(string: isolatedKubeconfig).expandingTildeInPath
            guard fileExists(expanded) else { return [] }
            return [URL(fileURLWithPath: expanded).standardizedFileURL]
        }

        if let kubeconfig = environment["KUBECONFIG"], !kubeconfig.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            for path in kubeconfig.split(separator: ":").map(String.init) {
                let expanded = NSString(string: path).expandingTildeInPath
                candidates.append(URL(fileURLWithPath: expanded))
            }
        }

        if environment[Self.disableDefaultConfigDiscoveryVariable] != "1" {
            let homeDirectory = homeDirectoryProvider()
            let defaultConfig = homeDirectory
                .appendingPathComponent(".kube", isDirectory: true)
                .appendingPathComponent("config", isDirectory: false)
            candidates.append(defaultConfig)
        }

        var uniqueByPath: [String: URL] = [:]
        for candidate in candidates {
            let standardized = candidate.standardizedFileURL
            guard fileExists(standardized.path) else { continue }
            uniqueByPath[standardized.path] = standardized
        }

        return uniqueByPath.values.sorted { $0.path < $1.path }
    }

    public static func isIsolatedKubeconfigActive(environment: [String: String] = ProcessInfo.processInfo.environment) -> Bool {
        isolatedKubeconfigPath(environment: environment) != nil
    }

    private static func isolatedKubeconfigPath(environment: [String: String]) -> String? {
        let value = environment[isolatedKubeconfigVariable]?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return value.isEmpty ? nil : value
    }
}
