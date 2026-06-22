import XCTest
@testable import RuneCore
@testable import RuneUI

final class AuthDoctorRBACProjectorTests: XCTestCase {
    func testPreflightTargetsCoverEmptyViewResourcesWithAPIGroups() {
        let targets = Dictionary(uniqueKeysWithValues: AuthDoctorRBACPreflightTarget.emptyViewTargets.map { ($0.id, $0) })

        XCTAssertEqual(targets["rbac-deployments-list"]?.apiGroup, "apps")
        XCTAssertEqual(targets["rbac-statefulsets-list"]?.apiGroup, "apps")
        XCTAssertEqual(targets["rbac-daemonsets-list"]?.apiGroup, "apps")
        XCTAssertEqual(targets["rbac-jobs-list"]?.apiGroup, "batch")
        XCTAssertEqual(targets["rbac-cronjobs-list"]?.apiGroup, "batch")
        XCTAssertEqual(targets["rbac-replicasets-list"]?.apiGroup, "apps")
        XCTAssertEqual(targets["rbac-hpas-list"]?.apiGroup, "autoscaling")
        XCTAssertEqual(targets["rbac-services-list"]?.apiGroup, nil)
        XCTAssertEqual(targets["rbac-endpoints-list"]?.apiGroup, nil)
        XCTAssertEqual(targets["rbac-endpoints-list"]?.destination, .resource(section: .networking, kind: .endpoint))
        XCTAssertEqual(targets["rbac-ingresses-list"]?.apiGroup, "networking.k8s.io")
        XCTAssertEqual(targets["rbac-networkpolicies-list"]?.apiGroup, "networking.k8s.io")
        XCTAssertEqual(targets["rbac-configmaps-list"]?.apiGroup, nil)
        XCTAssertEqual(targets["rbac-secrets-list"]?.apiGroup, nil)
        XCTAssertEqual(targets["rbac-secrets-list"]?.destination, .resource(section: .config, kind: .secret))
        XCTAssertEqual(targets["rbac-pvcs-list"]?.apiGroup, nil)
        XCTAssertEqual(targets["rbac-nodes-list"]?.scope, .cluster)
        XCTAssertEqual(targets["rbac-pvs-list"]?.scope, .cluster)
        XCTAssertEqual(targets["rbac-storageclasses-list"]?.apiGroup, "storage.k8s.io")
        XCTAssertEqual(targets["rbac-storageclasses-list"]?.scope, .cluster)
        XCTAssertEqual(targets["rbac-serviceaccounts-list"]?.apiGroup, nil)
        XCTAssertEqual(targets["rbac-serviceaccounts-list"]?.destination, .resource(section: .rbac, kind: .serviceAccount))
        XCTAssertEqual(targets["rbac-roles-list"]?.apiGroup, "rbac.authorization.k8s.io")
        XCTAssertEqual(targets["rbac-roles-list"]?.namespace(activeNamespace: "synthetic"), "synthetic")
        XCTAssertEqual(targets["rbac-rolebindings-list"]?.apiGroup, "rbac.authorization.k8s.io")
        XCTAssertEqual(targets["rbac-clusterroles-list"]?.apiGroup, "rbac.authorization.k8s.io")
        XCTAssertEqual(targets["rbac-clusterroles-list"]?.namespace(activeNamespace: "synthetic"), nil)
        XCTAssertEqual(targets["rbac-clusterrolebindings-list"]?.apiGroup, "rbac.authorization.k8s.io")
        XCTAssertEqual(targets["rbac-clusterrolebindings-list"]?.scope, .cluster)
        XCTAssertEqual(targets["rbac-events-list"]?.destination, .section(.events))
        XCTAssertEqual(targets.count, 23)
    }

    func testProjectsCanIMatrixChecks() {
        let allowed = AuthDoctorRBACProjector.check(
            for: AuthDoctorRBACCapability(
                id: "rbac-pods-list",
                title: "RBAC pod list",
                verb: "list",
                resource: "pods",
                allowed: true
            ),
            namespace: "synthetic"
        )
        let denied = AuthDoctorRBACProjector.check(
            for: AuthDoctorRBACCapability(
                id: "rbac-pod-logs",
                title: "RBAC pod logs",
                verb: "get",
                resource: "pods",
                subresource: "log",
                allowed: false
            ),
            namespace: "synthetic"
        )

        XCTAssertEqual(allowed.status, .passed)
        XCTAssertEqual(allowed.message, "RBAC allows list pods in synthetic.")
        XCTAssertEqual(denied.status, .warning)
        XCTAssertEqual(denied.message, "RBAC denied get pods/log in synthetic.")
    }

    func testProjectsAPIGroupInCanIMatrixChecks() {
        let check = AuthDoctorRBACProjector.check(
            for: AuthDoctorRBACCapability(
                id: "rbac-deployments-list",
                title: "RBAC deployments",
                verb: "list",
                resource: "deployments",
                apiGroup: " apps ",
                allowed: false
            ),
            namespace: "synthetic"
        )

        XCTAssertEqual(check.status, .warning)
        XCTAssertEqual(check.message, "RBAC denied list apps/deployments in synthetic.")
    }

    func testSummarizesPartialPodAccessWhenListWorksButSubresourcesAreDenied() throws {
        let summary = try XCTUnwrap(AuthDoctorRBACProjector.accessSummary(
            namespace: "synthetic",
            capabilities: [
                AuthDoctorRBACCapability(
                    id: "rbac-pods-list",
                    title: "RBAC pod list",
                    verb: "list",
                    resource: "pods",
                    allowed: true
                ),
                AuthDoctorRBACCapability(
                    id: "rbac-pod-logs",
                    title: "RBAC pod logs",
                    verb: "get",
                    resource: "pods",
                    subresource: "log",
                    allowed: false
                ),
                AuthDoctorRBACCapability(
                    id: "rbac-pod-exec",
                    title: "RBAC pod exec",
                    verb: "create",
                    resource: "pods",
                    subresource: "exec",
                    allowed: false
                ),
                AuthDoctorRBACCapability(
                    id: "rbac-port-forward",
                    title: "RBAC port-forward",
                    verb: "create",
                    resource: "pods",
                    subresource: "portforward",
                    allowed: false
                )
            ]
        ))

        XCTAssertEqual(summary.id, "rbac-access-summary")
        XCTAssertEqual(summary.status, .warning)
        XCTAssertEqual(
            summary.message,
            "Partial pod access in namespace synthetic: pods can be listed, but logs, exec, and port-forward are denied."
        )
    }

    func testSummarizesDeniedPodListAsEmptyWorkloadsCause() throws {
        let summary = try XCTUnwrap(AuthDoctorRBACProjector.accessSummary(
            namespace: "synthetic",
            capabilities: [
                AuthDoctorRBACCapability(
                    id: "rbac-pods-list",
                    title: "RBAC pod list",
                    verb: "list",
                    resource: "pods",
                    allowed: false
                )
            ]
        ))

        XCTAssertEqual(summary.status, .failed)
        XCTAssertTrue(summary.message.contains("Workloads can appear empty even when pods exist."))
    }

    func testDoesNotClaimFullAccessWhenSubresourceChecksAreUnknown() {
        let summary = AuthDoctorRBACProjector.accessSummary(
            namespace: "synthetic",
            capabilities: [
                AuthDoctorRBACCapability(
                    id: "rbac-pods-list",
                    title: "RBAC pod list",
                    verb: "list",
                    resource: "pods",
                    allowed: true
                )
            ]
        )

        XCTAssertNil(summary)
    }
}
