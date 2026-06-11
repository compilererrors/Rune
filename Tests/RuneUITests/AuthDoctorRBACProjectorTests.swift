import XCTest
@testable import RuneCore
@testable import RuneUI

final class AuthDoctorRBACProjectorTests: XCTestCase {
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
