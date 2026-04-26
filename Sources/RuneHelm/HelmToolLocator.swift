import Foundation
import RuneCore

public enum HelmToolLocator {
    public static func resolve() -> String? {
        if let envPath = executableEnvironmentPath("RUNE_HELM") {
            return envPath
        }

        if let bundlePath = bundledExecutable(named: "helm") {
            return bundlePath
        }

        if RuneRuntimeDependencyPolicy.allowsPATHHelmFallback {
            return "/usr/bin/env"
        }

        return nil
    }

    private static func executableEnvironmentPath(_ name: String) -> String? {
        guard let raw = ProcessInfo.processInfo.environment[name]?.trimmingCharacters(in: .whitespacesAndNewlines),
              !raw.isEmpty,
              FileManager.default.isExecutableFile(atPath: raw) else {
            return nil
        }
        return raw
    }

    private static func bundledExecutable(named name: String) -> String? {
        guard let bundleURL = Bundle.main.executableURL?.deletingLastPathComponent() else {
            return nil
        }
        let candidate = bundleURL.appendingPathComponent(name).path
        return FileManager.default.isExecutableFile(atPath: candidate) ? candidate : nil
    }
}
