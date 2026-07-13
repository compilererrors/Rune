import XCTest
@testable import RuneUI
import RuneCore

final class AuthDoctorKubeconfigInspectorTests: XCTestCase {
    func testAppStoreSupportedNativeExecDoesNotReportContradictoryCLIToolWarnings() throws {
        let fixture = try KubeconfigFixture("""
        apiVersion: v1
        current-context: synthetic-native
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://cluster.example.invalid
        contexts:
        - name: synthetic-native
          context:
            cluster: synthetic-cluster
            user: synthetic-user
        users:
        - name: synthetic-user
          user:
            exec:
              apiVersion: client.authentication.k8s.io/v1
              command: aws
              args: [eks, get-token, --cluster-name, synthetic-cluster, --region, eu-north-1]
              interactiveMode: Never
        """)
        defer { fixture.remove() }

        let checks = AuthDoctorKubeconfigInspector(
            fileExists: { _ in false },
            externalCommandsAllowed: { false }
        ).inspect(sources: [fixture.source], activeContextName: "synthetic-native")

        XCTAssertEqual(checks.first { $0.id == "exec-auth-profile" }?.status, .passed)
        XCTAssertEqual(checks.first { $0.id == "exec-auth-tools" }?.status, .passed)
        XCTAssertFalse(checks.contains { $0.id == "cloud-login-tools" })
        XCTAssertFalse(checks.contains { $0.id == "eks-role-profile" })
        XCTAssertTrue(checks.first { $0.id == "exec-auth-tools" }?.message.contains("No external") == true)
    }

    func testInspectionUsesCurrentContextAndIgnoresInactiveExecAndProxyProfiles() throws {
        let fixture = try KubeconfigFixture(
            """
            apiVersion: v1
            current-context: selected
            clusters:
            - name: selected-cluster
              cluster:
                server: https://selected.example.invalid
            - name: inactive-cluster
              cluster:
                server: https://inactive.example.invalid
                proxy-url: http://proxy.example.invalid
            users:
            - name: selected-user
              user:
                token: placeholder-token
            - name: inactive-user
              user:
                exec:
                  apiVersion: client.authentication.k8s.io/v1beta1
                  command: inactive-credential-helper
                  interactiveMode: Never
            contexts:
            - name: selected
              context:
                cluster: selected-cluster
                user: selected-user
            - name: inactive
              context:
                cluster: inactive-cluster
                user: inactive-user
            """
        )
        defer { fixture.remove() }

        let checks = inspector(fileExists: false, isExecutable: false).inspect(sources: [fixture.source])

        XCTAssertFalse(checks.contains { $0.id == "exec-auth-profile" })
        XCTAssertFalse(checks.contains { $0.id == "exec-auth-tools" })
        XCTAssertFalse(checks.contains { $0.id == "proxy-profile" })
    }

    func testExplicitActiveContextOverridesKubeconfigCurrentContext() throws {
        let fixture = try KubeconfigFixture(
            """
            apiVersion: v1
            current-context: first
            clusters:
            - name: first-cluster
              cluster:
                server: https://first.example.invalid
            - name: second-cluster
              cluster:
                server: https://second.example.invalid
            users:
            - name: first-user
              user:
                token: placeholder-token
            - name: second-user
              user:
                exec:
                  apiVersion: client.authentication.k8s.io/v1beta1
                  command: credential-helper
                  interactiveMode: Never
            contexts:
            - name: first
              context:
                cluster: first-cluster
                user: first-user
            - name: second
              context:
                cluster: second-cluster
                user: second-user
            """
        )
        defer { fixture.remove() }

        let checks = inspector(fileExists: true, isExecutable: true).inspect(
            sources: [fixture.source],
            activeContextName: "second"
        )

        XCTAssertEqual(checks.first { $0.id == "exec-auth-tools" }?.status, .passed)
    }

    func testMissingExecToolUsesBoundedInstallHint() throws {
        let fixture = try KubeconfigFixture(execConfig: """
            apiVersion: client.authentication.k8s.io/v1beta1
            command: credential-helper
            interactiveMode: Never
            installHint: |
              Install the credential helper with the supported package manager.
              Then sign in before retrying.
            """)
        defer { fixture.remove() }

        let check = try XCTUnwrap(inspector(fileExists: false, isExecutable: false)
            .inspect(sources: [fixture.source])
            .first { $0.id == "exec-auth-tools" })

        XCTAssertEqual(check.status, .warning)
        XCTAssertTrue(check.message.contains("Missing exec auth tool"))
        XCTAssertTrue(check.message.contains("Plugin guidance: Install the credential helper"))
        XCTAssertTrue(check.message.contains("Then sign in before retrying."))
    }

    func testExistingNonExecutableToolIsNotReportedAsMissing() throws {
        let fixture = try KubeconfigFixture(execConfig: """
            apiVersion: client.authentication.k8s.io/v1beta1
            command: credential-helper
            interactiveMode: Never
            """)
        defer { fixture.remove() }

        let check = try XCTUnwrap(inspector(fileExists: true, isExecutable: false)
            .inspect(sources: [fixture.source])
            .first { $0.id == "exec-auth-tools" })

        XCTAssertEqual(check.status, .warning)
        XCTAssertTrue(check.message.contains("found but not executable"))
        XCTAssertFalse(check.message.contains("Missing exec auth tool"))
    }

    func testRelativeExecCommandIsResolvedFromSourceDirectory() throws {
        let fixture = try KubeconfigFixture(execConfig: """
            apiVersion: client.authentication.k8s.io/v1beta1
            command: ./plugins/credential-helper
            interactiveMode: Never
            """)
        defer { fixture.remove() }
        let expectedPath = fixture.directory
            .appendingPathComponent("plugins/credential-helper")
            .standardized.path
        let inspector = AuthDoctorKubeconfigInspector(
            fileExists: { $0 == expectedPath },
            isExecutable: { $0 == expectedPath },
            executableSearchPaths: ["/synthetic/bin"]
        )

        let check = try XCTUnwrap(inspector.inspect(sources: [fixture.source])
            .first { $0.id == "exec-auth-tools" })

        XCTAssertEqual(check.status, .passed)
    }

    func testInteractiveModeAlwaysIsReportedAsIncompatible() throws {
        let fixture = try KubeconfigFixture(execConfig: """
            apiVersion: client.authentication.k8s.io/v1
            command: credential-helper
            interactiveMode: Always
            """)
        defer { fixture.remove() }

        let check = try XCTUnwrap(inspector(fileExists: true, isExecutable: true)
            .inspect(sources: [fixture.source])
            .first { $0.id == "exec-auth-interactive-mode" })

        XCTAssertEqual(check.status, .failed)
        XCTAssertTrue(check.message.contains("interactiveMode Always"))
    }

    func testExecCredentialV1RequiresInteractiveMode() throws {
        let fixture = try KubeconfigFixture(execConfig: """
            apiVersion: client.authentication.k8s.io/v1
            command: credential-helper
            """)
        defer { fixture.remove() }

        let checks = inspector(fileExists: true, isExecutable: true).inspect(sources: [fixture.source])

        XCTAssertEqual(checks.first { $0.id == "exec-auth-v1-interactive-mode" }?.status, .failed)
    }

    func testExecCredentialV1Beta1DoesNotRequireInteractiveMode() throws {
        let fixture = try KubeconfigFixture(execConfig: """
            apiVersion: client.authentication.k8s.io/v1beta1
            command: credential-helper
            """)
        defer { fixture.remove() }

        let checks = inspector(fileExists: true, isExecutable: true).inspect(sources: [fixture.source])

        XCTAssertFalse(checks.contains { $0.id == "exec-auth-v1-interactive-mode" })
    }

    func testProxyURLIsDetectedButNeverReportedAsVerified() throws {
        let fixture = try KubeconfigFixture(
            """
            apiVersion: v1
            current-context: selected
            clusters:
            - name: selected-cluster
              cluster:
                server: https://selected.example.invalid
                proxy-url: http://proxy.example.invalid
            users:
            - name: selected-user
              user:
                token: placeholder-token
            contexts:
            - name: selected
              context:
                cluster: selected-cluster
                user: selected-user
            """
        )
        defer { fixture.remove() }

        let check = try XCTUnwrap(inspector(fileExists: false, isExecutable: false)
            .inspect(sources: [fixture.source])
            .first { $0.id == "proxy-profile" })

        XCTAssertEqual(check.status, .warning)
        XCTAssertTrue(check.message.contains("not verified"))
        XCTAssertFalse(check.message.contains("verify that proxy routing works"))
    }

    func testProxyAfterCustomCAIsRetainedWithoutCurrentContext() throws {
        let fixture = try KubeconfigFixture(
            """
            apiVersion: v1
            clusters:
            - name: selected-cluster
              cluster:
                server: https://selected.example.invalid
                certificate-authority-data: PLACEHOLDER_CA
                proxy-url: http://proxy.example.invalid
            users:
            - name: selected-user
              user:
                exec:
                  command: kubelogin
                  args:
                  - get-token
                  - --environment
                  - PublicCloud
            contexts:
            - name: selected
              context:
                cluster: selected-cluster
                user: selected-user
            """
        )
        defer { fixture.remove() }

        let checks = inspector(fileExists: false, isExecutable: false).inspect(sources: [fixture.source])
        let rendered = checks.map(\.message).joined(separator: "\n")

        XCTAssertTrue(checks.contains { $0.id == "proxy-profile" }, rendered)
        XCTAssertTrue(checks.contains { $0.id == "custom-ca-profile" }, rendered)
    }

    func testExecArgumentsAndEnvironmentValuesAreNeverProjected() throws {
        let fixture = try KubeconfigFixture(execConfig: """
            apiVersion: client.authentication.k8s.io/v1beta1
            command: aws
            interactiveMode: Never
            args:
            - eks
            - get-token
            - --cluster-name
            - private-argument-placeholder
            env:
            - name: PROVIDER_PROFILE
              value: private-environment-placeholder
            """)
        defer { fixture.remove() }

        let rendered = inspector(fileExists: false, isExecutable: false)
            .inspect(sources: [fixture.source])
            .map(\.message)
            .joined(separator: "\n")

        XCTAssertFalse(rendered.contains("private-argument-placeholder"))
        XCTAssertFalse(rendered.contains("private-environment-placeholder"))
        XCTAssertFalse(rendered.contains("PROVIDER_PROFILE"))
    }

    private func inspector(fileExists: Bool, isExecutable: Bool) -> AuthDoctorKubeconfigInspector {
        AuthDoctorKubeconfigInspector(
            fileExists: { _ in fileExists },
            isExecutable: { _ in isExecutable },
            executableSearchPaths: ["/synthetic/bin"]
        )
    }
}

private struct KubeconfigFixture {
    let directory: URL
    let source: KubeConfigSource

    init(_ content: String) throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-auth-doctor-tests")
            .appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let url = directory.appendingPathComponent("config.yaml")
        try content.write(to: url, atomically: true, encoding: .utf8)
        self.directory = directory
        self.source = KubeConfigSource(url: url)
    }

    init(execConfig: String) throws {
        try self.init(
            """
            apiVersion: v1
            current-context: selected
            clusters:
            - name: selected-cluster
              cluster:
                server: https://selected.example.invalid
            users:
            - name: selected-user
              user:
                exec:
            \(execConfig.split(whereSeparator: \.isNewline).map { "      " + $0 }.joined(separator: "\n"))
            contexts:
            - name: selected
              context:
                cluster: selected-cluster
                user: selected-user
            """
        )
    }

    func remove() {
        try? FileManager.default.removeItem(at: directory)
    }
}
