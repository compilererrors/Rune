import AppKit
import Darwin
import Foundation

private enum RuneAppLaunchError: Error, LocalizedError {
    case invalidArguments
    case invalidProcessIdentifier(String)
    case invalidBundle(URL)
    case launchFailed
    case ownershipMismatch(pid_t, expected: URL, actual: URL?, expectedIdentifier: String, actualIdentifier: String?)
    case terminationFailed(pid_t)

    var errorDescription: String? {
        switch self {
        case .invalidArguments:
            "usage: rune-app-launch <Rune.app> <isolated-home> | rune-app-launch --terminate <Rune.app> <pid>"
        case let .invalidProcessIdentifier(value):
            "invalid Rune process identifier: \(value)"
        case let .invalidBundle(url):
            "invalid Rune application bundle: \(url.path)"
        case .launchFailed:
            "LaunchServices returned no Rune application."
        case let .ownershipMismatch(pid, expected, actual, expectedIdentifier, actualIdentifier):
            "refusing to terminate PID \(pid): expected \(expected.path) [\(expectedIdentifier)], got \(actual?.path ?? "unknown bundle") [\(actualIdentifier ?? "unknown identifier")]"
        case let .terminationFailed(pid):
            "test-owned Rune process did not terminate: \(pid)"
        }
    }
}

@main
private struct RuneAppLauncher {
    private static let forwardedEnvironmentKeys = [
        "KUBECONFIG",
        "RUNE_ISOLATED_KUBECONFIG",
        "RUNE_DEBUG_LAYOUT",
        "RUNE_DEBUG_LAYOUT_LIVE_SCENARIO",
        "RUNE_DEBUG_LAYOUT_LIVE_SCENARIO_STEPS",
        "RUNE_DEBUG_LAYOUT_CONTEXT",
        "RUNE_DEBUG_LAYOUT_NAMESPACE",
        "RUNE_DEBUG_LAYOUT_DETAIL_WIDTH",
        "RUNE_DEBUG_LAYOUT_POD_DWELL_MS",
        "RUNE_DEBUG_LAYOUT_SNAPSHOT_HOLD_MS",
        "RUNE_DEBUG_LAYOUT_INTERACTION_ACK_DIR",
        "RUNE_DIAGNOSTICS_LOGGING",
        "RUNE_LOG_TO_STDERR",
        "RUNE_VERBOSE_DEBUG_TRACE",
    ]

    @MainActor
    static func main() async {
        do {
            if CommandLine.arguments.count == 4, CommandLine.arguments[1] == "--terminate" {
                let bundleURL = URL(fileURLWithPath: CommandLine.arguments[2], isDirectory: true)
                guard let processIdentifier = pid_t(CommandLine.arguments[3]), processIdentifier > 0 else {
                    throw RuneAppLaunchError.invalidProcessIdentifier(CommandLine.arguments[3])
                }
                try await terminateTestOwnedApplication(
                    processIdentifier: processIdentifier,
                    expectedBundleURL: bundleURL
                )
                return
            }

            guard CommandLine.arguments.count == 3 else {
                throw RuneAppLaunchError.invalidArguments
            }

            let bundleURL = URL(fileURLWithPath: CommandLine.arguments[1])
            let isolatedHome = URL(fileURLWithPath: CommandLine.arguments[2], isDirectory: true)
            let configuration = NSWorkspace.OpenConfiguration()
            configuration.activates = true
            configuration.addsToRecentItems = false
            configuration.createsNewApplicationInstance = true
            let enableDemoCluster = ProcessInfo.processInfo.environment[
                "RUNE_APP_LAUNCH_ENABLE_DEMO_CLUSTER"
            ] == "1"
            configuration.arguments = [
                "-rune.settings.enableDemoCluster",
                enableDemoCluster ? "true" : "false",
            ]
            var launchEnvironment = [
                "HOME": isolatedHome.path,
                "CFFIXED_USER_HOME": isolatedHome.path,
                "RUNE_DISABLE_DEFAULT_KUBECONFIG_DISCOVERY": "1",
                "RUNE_DISABLE_BOOKMARKED_KUBECONFIGS": "1",
                "RUNE_K8S_AGENT": "",
                "RUNE_DIAGNOSTICS_LOGGING": "1",
                "RUNE_LOG_TO_STDERR": "0",
                "RUNE_VERBOSE_DEBUG_TRACE": "0",
            ]
            let invokingEnvironment = ProcessInfo.processInfo.environment
            if invokingEnvironment["RUNE_APP_LAUNCH_FORWARD_UI_SMOKE_ENV"] == "1" {
                for key in forwardedEnvironmentKeys {
                    if let value = invokingEnvironment[key] {
                        launchEnvironment[key] = value
                    }
                }
            }
            configuration.environment = launchEnvironment

            let application = try await withCheckedThrowingContinuation { continuation in
                NSWorkspace.shared.openApplication(
                    at: bundleURL,
                    configuration: configuration
                ) { application, error in
                    if let application {
                        continuation.resume(returning: application)
                    } else {
                        continuation.resume(throwing: error ?? RuneAppLaunchError.launchFailed)
                    }
                }
            }
            print(application.processIdentifier)
        } catch {
            FileHandle.standardError.write(Data("\(error.localizedDescription)\n".utf8))
            exit(1)
        }
    }

    @MainActor
    private static func terminateTestOwnedApplication(
        processIdentifier: pid_t,
        expectedBundleURL: URL
    ) async throws {
        let canonicalExpectedURL = canonicalBundleURL(expectedBundleURL)
        guard let expectedBundleIdentifier = Bundle(url: canonicalExpectedURL)?.bundleIdentifier else {
            throw RuneAppLaunchError.invalidBundle(canonicalExpectedURL)
        }
        guard let application = try testOwnedApplication(
            processIdentifier: processIdentifier,
            expectedBundleURL: canonicalExpectedURL,
            expectedBundleIdentifier: expectedBundleIdentifier
        ) else {
            return
        }

        application.terminate()
        if await waitForTermination(of: application) {
            return
        }

        guard let forceTarget = try testOwnedApplication(
            processIdentifier: processIdentifier,
            expectedBundleURL: canonicalExpectedURL,
            expectedBundleIdentifier: expectedBundleIdentifier
        ) else {
            return
        }
        forceTarget.forceTerminate()
        guard await waitForTermination(of: forceTarget) else {
            throw RuneAppLaunchError.terminationFailed(processIdentifier)
        }
    }

    @MainActor
    private static func testOwnedApplication(
        processIdentifier: pid_t,
        expectedBundleURL: URL,
        expectedBundleIdentifier: String
    ) throws -> NSRunningApplication? {
        guard let application = NSRunningApplication(processIdentifier: processIdentifier) else {
            if processExists(processIdentifier) {
                throw RuneAppLaunchError.ownershipMismatch(
                    processIdentifier,
                    expected: expectedBundleURL,
                    actual: nil,
                    expectedIdentifier: expectedBundleIdentifier,
                    actualIdentifier: nil
                )
            }
            return nil
        }
        guard !application.isTerminated else {
            return nil
        }

        let actualBundleURL = application.bundleURL.map(canonicalBundleURL)
        guard actualBundleURL == expectedBundleURL,
              application.bundleIdentifier == expectedBundleIdentifier
        else {
            throw RuneAppLaunchError.ownershipMismatch(
                processIdentifier,
                expected: expectedBundleURL,
                actual: actualBundleURL,
                expectedIdentifier: expectedBundleIdentifier,
                actualIdentifier: application.bundleIdentifier
            )
        }
        return application
    }

    private static func canonicalBundleURL(_ url: URL) -> URL {
        url.standardizedFileURL.resolvingSymlinksInPath()
    }

    private static func processExists(_ processIdentifier: pid_t) -> Bool {
        Darwin.kill(processIdentifier, 0) == 0 || errno == EPERM
    }

    @MainActor
    private static func waitForTermination(of application: NSRunningApplication) async -> Bool {
        for _ in 0..<40 {
            if application.isTerminated {
                return true
            }
            try? await Task.sleep(for: .milliseconds(50))
        }
        return application.isTerminated
    }
}
