import XCTest
@testable import RuneCore
@testable import RuneUI

final class AuthDoctorEntryActionResolverTests: XCTestCase {
    func testResolvesResourceEntriesToAppDestinationsWhenTargetExists() {
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("pod-list"), hasPodTarget: false)?.destination,
            .pods
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("pod-logs"), hasPodTarget: true)?.destination,
            .podLogs
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-pod-exec"), hasPodTarget: true)?.destination,
            .podExec
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-port-forward"), hasPodTarget: true)?.destination,
            .podPortForward
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("namespace-list"), hasPodTarget: false)?.destination,
            .pods
        )
    }

    func testResolvesKubeconfigEntriesToImportReview() {
        let action = AuthDoctorEntryActionResolver.resolve(check: check("kubeconfig-files"), hasPodTarget: false)

        XCTAssertEqual(action?.title, "Review Config")
        XCTAssertEqual(action?.destination, .kubeconfigReview)
    }

    func testFallsBackToOfficialDocsWhenNoResourceTargetExists() {
        let podLogs = AuthDoctorEntryActionResolver.resolve(check: check("pod-logs"), hasPodTarget: false)
        XCTAssertEqual(podLogs?.title, "Docs")

        guard case let .documentation(podLogsURL) = podLogs?.destination else {
            return XCTFail("Expected pod logs docs fallback")
        }
        XCTAssertEqual(podLogsURL.host, "kubernetes.io")
        XCTAssertTrue(podLogsURL.absoluteString.contains("kubectl_logs"))

        let execAuth = AuthDoctorEntryActionResolver.resolve(check: check("exec-auth-tools"), hasPodTarget: false)
        guard case let .documentation(execAuthURL) = execAuth?.destination else {
            return XCTFail("Expected exec auth docs fallback")
        }
        XCTAssertEqual(execAuthURL.host, "kubernetes.io")
        XCTAssertTrue(execAuthURL.absoluteString.contains("client-go-credential-plugins"))
    }

    func testUnknownEntryHasNoAction() {
        XCTAssertNil(AuthDoctorEntryActionResolver.resolve(check: check("synthetic-unknown"), hasPodTarget: true))
    }

    private func check(_ id: String) -> RuneHealthCheck {
        RuneHealthCheck(id: id, title: "Synthetic", status: .warning, message: "Synthetic")
    }
}
