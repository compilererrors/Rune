import Foundation
import RuneCore

enum KubeExecCredentialRunner {
    /// Runs the exec plugin (same semantics as kubectl) and returns a bearer token + optional expiry.
    static func fetchToken(
        exec: ExecPluginConfig,
        baseEnvironment: [String: String],
        runner: CommandRunning,
        timeout: TimeInterval
    ) async throws -> (token: String, expiry: Date?) {
        var env = ProcessInfo.processInfo.environment
        for (k, v) in baseEnvironment {
            env[k] = v
        }
        if let extra = exec.env {
            for pair in extra {
                env[pair.name] = pair.value
            }
        }

        let args = [exec.command] + (exec.args ?? [])
        let result = try await runner.run(
            executable: "/usr/bin/env",
            arguments: args,
            environment: env,
            timeout: timeout
        )
        guard result.exitCode == 0 else {
            throw RuneError.commandFailed(
                command: "exec \(exec.command)",
                message: result.stderr.isEmpty ? "exit \(result.exitCode)" : result.stderr
            )
        }

        let trimmed = result.stdout.trimmingCharacters(in: .whitespacesAndNewlines)
        let data = Data(trimmed.utf8)
        let decoded = try JSONDecoder().decode(ExecCredentialResponseJSON.self, from: data)
        guard let token = decoded.status?.token, !token.isEmpty else {
            throw RuneError.parseError(message: "exec-plugin returnerade inget token")
        }

        var expiry: Date?
        if let iso = decoded.status?.expirationTimestamp, !iso.isEmpty {
            let fmt = ISO8601DateFormatter()
            fmt.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
            expiry = fmt.date(from: iso) ?? ISO8601DateFormatter().date(from: iso)
        }
        return (token, expiry)
    }
}
