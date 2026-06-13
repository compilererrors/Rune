import XCTest
@testable import RuneCore
@testable import RuneUI

final class AuthDoctorEntryActionResolverTests: XCTestCase {
    func testResolvesResourceEntriesToAppDestinationsWhenTargetExists() {
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("pod-list"), hasPodTarget: false)?.destination,
            .resource(section: .workloads, kind: .pod)
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
            .resource(section: .workloads, kind: .pod)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-events-list"), hasPodTarget: false)?.destination,
            .section(.events)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-deployments-list"), hasPodTarget: false)?.destination,
            .resource(section: .workloads, kind: .deployment)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-statefulsets-list"), hasPodTarget: false)?.destination,
            .resource(section: .workloads, kind: .statefulSet)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-daemonsets-list"), hasPodTarget: false)?.destination,
            .resource(section: .workloads, kind: .daemonSet)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-jobs-list"), hasPodTarget: false)?.destination,
            .resource(section: .workloads, kind: .job)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-cronjobs-list"), hasPodTarget: false)?.destination,
            .resource(section: .workloads, kind: .cronJob)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-replicasets-list"), hasPodTarget: false)?.destination,
            .resource(section: .workloads, kind: .replicaSet)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-hpas-list"), hasPodTarget: false)?.destination,
            .resource(section: .workloads, kind: .horizontalPodAutoscaler)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-services-list"), hasPodTarget: false)?.destination,
            .resource(section: .networking, kind: .service)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-ingresses-list"), hasPodTarget: false)?.destination,
            .resource(section: .networking, kind: .ingress)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-networkpolicies-list"), hasPodTarget: false)?.destination,
            .resource(section: .networking, kind: .networkPolicy)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-configmaps-list"), hasPodTarget: false)?.destination,
            .resource(section: .config, kind: .configMap)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-secrets-list"), hasPodTarget: false)?.destination,
            .resource(section: .config, kind: .secret)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-pvcs-list"), hasPodTarget: false)?.destination,
            .resource(section: .storage, kind: .persistentVolumeClaim)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-nodes-list"), hasPodTarget: false)?.destination,
            .resource(section: .storage, kind: .node)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-pvs-list"), hasPodTarget: false)?.destination,
            .resource(section: .storage, kind: .persistentVolume)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-storageclasses-list"), hasPodTarget: false)?.destination,
            .resource(section: .storage, kind: .storageClass)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-roles-list"), hasPodTarget: false)?.destination,
            .resource(section: .rbac, kind: .role)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-rolebindings-list"), hasPodTarget: false)?.destination,
            .resource(section: .rbac, kind: .roleBinding)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-clusterroles-list"), hasPodTarget: false)?.destination,
            .resource(section: .rbac, kind: .clusterRole)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-clusterrolebindings-list"), hasPodTarget: false)?.destination,
            .resource(section: .rbac, kind: .clusterRoleBinding)
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

        let execAuthCache = AuthDoctorEntryActionResolver.resolve(check: check("exec-auth-cache"), hasPodTarget: false)
        guard case let .documentation(execAuthCacheURL) = execAuthCache?.destination else {
            return XCTFail("Expected exec auth cache docs fallback")
        }
        XCTAssertEqual(execAuthCacheURL.host, "kubernetes.io")
        XCTAssertTrue(execAuthCacheURL.absoluteString.contains("client-go-credential-plugins"))

        let clientCertificateAuth = AuthDoctorEntryActionResolver.resolve(check: check("client-certificate-auth"), hasPodTarget: false)
        guard case let .documentation(clientCertificateAuthURL) = clientCertificateAuth?.destination else {
            return XCTFail("Expected client certificate auth docs fallback")
        }
        XCTAssertEqual(clientCertificateAuthURL.host, "kubernetes.io")
        XCTAssertTrue(clientCertificateAuthURL.absoluteString.contains("authentication"))
    }

    func testRBACAccessSummaryLinksToOfficialRBACDocs() {
        let action = AuthDoctorEntryActionResolver.resolve(check: check("rbac-access-summary"), hasPodTarget: true)
        XCTAssertEqual(action?.title, "Docs")

        guard case let .documentation(url) = action?.destination else {
            return XCTFail("Expected RBAC docs fallback")
        }
        XCTAssertEqual(url.host, "kubernetes.io")
        XCTAssertTrue(url.absoluteString.contains("rbac"))
    }

    func testAPIAuthAndAuthorizationDiagnosticsLinkToOfficialDocs() {
        let auth = AuthDoctorEntryActionResolver.resolve(check: check("api-auth"), hasPodTarget: false)
        let authorization = AuthDoctorEntryActionResolver.resolve(check: check("api-authorization"), hasPodTarget: false)

        guard case let .documentation(authURL) = auth?.destination,
              case let .documentation(authorizationURL) = authorization?.destination else {
            return XCTFail("Expected documentation destinations")
        }
        XCTAssertTrue(authURL.absoluteString.contains("authentication"))
        XCTAssertTrue(authorizationURL.absoluteString.contains("rbac"))
    }

    func testUnknownEntryHasNoAction() {
        XCTAssertNil(AuthDoctorEntryActionResolver.resolve(check: check("synthetic-unknown"), hasPodTarget: true))
    }

    private func check(_ id: String) -> RuneHealthCheck {
        RuneHealthCheck(id: id, title: "Synthetic", status: .warning, message: "Synthetic")
    }
}
