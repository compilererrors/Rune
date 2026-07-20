import XCTest
@testable import RuneSecurity

final class KubeConfigNativeImportValidatorTests: XCTestCase {
    func testSupportedNativeExecProfilesDoNotReportMissingExecutables() {
        let validator = KubeConfigImportValidator(
            fileExists: { _ in false },
            executableSearchPaths: []
        )

        for raw in [eksKubeconfig, aksKubeconfig, gkeKubeconfig] {
            let review = validator.validate(raw: raw)
            XCTAssertFalse(review.issues.contains { $0.id.hasPrefix("missing-exec-plugin-") })
        }
    }

    func testUnsupportedExecProfileStillReportsMissingExecutable() {
        let validator = KubeConfigImportValidator(
            fileExists: { _ in false },
            executableSearchPaths: []
        )
        let review = validator.validate(raw: kubeconfig(
            command: "synthetic-auth-helper",
            arguments: []
        ))

        XCTAssertTrue(review.issues.contains { $0.id == "missing-exec-plugin-synthetic-context" })
    }

    private var eksKubeconfig: String {
        kubeconfig(
            command: "aws",
            arguments: ["eks", "get-token", "--cluster-name", "synthetic-cluster", "--region", "eu-north-1"]
        )
    }

    private var aksKubeconfig: String {
        kubeconfig(
            command: "kubelogin",
            arguments: [
                "get-token",
                "--tenant-id", "11111111-1111-4111-8111-111111111111",
                "--server-id", "22222222-2222-4222-8222-222222222222",
                "--client-id", "33333333-3333-4333-8333-333333333333",
                "-l", "spn"
            ]
        )
    }

    private var gkeKubeconfig: String {
        kubeconfig(command: "gke-gcloud-auth-plugin", arguments: ["--use_application_default_credentials"])
    }

    private func kubeconfig(command: String, arguments: [String]) -> String {
        let argumentsYAML = arguments.map { "        - \"\($0)\"" }.joined(separator: "\n")
        return """
        apiVersion: v1
        kind: Config
        current-context: synthetic-context
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://cluster.example.invalid
        contexts:
        - name: synthetic-context
          context:
            cluster: synthetic-cluster
            user: synthetic-user
        users:
        - name: synthetic-user
          user:
            exec:
              apiVersion: client.authentication.k8s.io/v1beta1
              command: \(command)
              args:
        \(argumentsYAML)
              interactiveMode: Never
        """
    }
}
