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
            AuthDoctorEntryActionResolver.resolve(check: check("pod-logs"), hasPodTarget: false)?.destination,
            .resource(section: .workloads, kind: .pod)
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-pod-logs"), hasPodTarget: true)?.destination,
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
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-endpoints-list"), hasPodTarget: false)?.destination,
            .resource(section: .networking, kind: .endpoint)
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
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-serviceaccounts-list"), hasPodTarget: false)?.destination,
            .resource(section: .rbac, kind: .serviceAccount)
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

    func testDeniedPodSubresourceChecksPrefillCanISimulatorInsteadOfOpeningBlockedPanels() {
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-pod-logs", status: .warning), hasPodTarget: true)?.destination,
            .rbacCanIPreset(
                verb: "get",
                resource: "pods",
                apiGroup: nil,
                subresource: "log",
                scope: .namespace
            )
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-pod-exec", status: .warning), hasPodTarget: true)?.destination,
            .rbacCanIPreset(
                verb: "create",
                resource: "pods",
                apiGroup: nil,
                subresource: "exec",
                scope: .namespace
            )
        )
        XCTAssertEqual(
            AuthDoctorEntryActionResolver.resolve(check: check("rbac-port-forward", status: .failed), hasPodTarget: false)?.destination,
            .rbacCanIPreset(
                verb: "create",
                resource: "pods",
                apiGroup: nil,
                subresource: "portforward",
                scope: .namespace
            )
        )
    }

    func testResolvesKubeconfigEntriesToImportReview() {
        let action = AuthDoctorEntryActionResolver.resolve(check: check("kubeconfig-files"), hasPodTarget: false)

        XCTAssertEqual(action?.title, "Review Config")
        XCTAssertEqual(action?.destination, .kubeconfigReview)
    }

    func testFallsBackToOfficialDocsWhenNoResourceTargetExists() {
        let execAuth = AuthDoctorEntryActionResolver.resolve(check: check("exec-auth-tools"), hasPodTarget: false)
        XCTAssertEqual(execAuth?.title, "Exec Auth Docs")
        XCTAssertEqual(execAuth?.systemImage, "key.viewfinder")
        guard case let .documentation(execAuthURL) = execAuth?.destination else {
            return XCTFail("Expected exec auth docs fallback")
        }
        XCTAssertEqual(execAuthURL.host, "kubernetes.io")
        XCTAssertTrue(execAuthURL.absoluteString.contains("client-go-credential-plugins"))

        let execAuthCache = AuthDoctorEntryActionResolver.resolve(check: check("exec-auth-cache"), hasPodTarget: false)
        XCTAssertEqual(execAuthCache?.title, "Exec Auth Docs")
        guard case let .documentation(execAuthCacheURL) = execAuthCache?.destination else {
            return XCTFail("Expected exec auth cache docs fallback")
        }
        XCTAssertEqual(execAuthCacheURL.host, "kubernetes.io")
        XCTAssertTrue(execAuthCacheURL.absoluteString.contains("client-go-credential-plugins"))

        let clientCertificateAuth = AuthDoctorEntryActionResolver.resolve(check: check("client-certificate-auth"), hasPodTarget: false)
        XCTAssertEqual(clientCertificateAuth?.title, "Auth Docs")
        guard case let .documentation(clientCertificateAuthURL) = clientCertificateAuth?.destination else {
            return XCTFail("Expected client certificate auth docs fallback")
        }
        XCTAssertEqual(clientCertificateAuthURL.host, "kubernetes.io")
        XCTAssertTrue(clientCertificateAuthURL.absoluteString.contains("authentication"))
    }

    func testRBACAccessSummaryLinksToOfficialRBACDocs() {
        let action = AuthDoctorEntryActionResolver.resolve(check: check("rbac-access-summary"), hasPodTarget: true)
        XCTAssertEqual(action?.title, "RBAC Docs")
        XCTAssertEqual(action?.systemImage, "person.2.badge.gearshape")

        guard case let .documentation(url) = action?.destination else {
            return XCTFail("Expected RBAC docs fallback")
        }
        XCTAssertEqual(url.host, "kubernetes.io")
        XCTAssertTrue(url.absoluteString.contains("rbac"))
    }

    func testDeniedRBACPreflightEntryPrefillsCanISimulator() {
        let action = AuthDoctorEntryActionResolver.resolve(
            check: check("rbac-deployments-list", status: .warning),
            hasPodTarget: false
        )

        XCTAssertEqual(action?.title, "Check RBAC")
        XCTAssertEqual(action?.systemImage, "person.badge.key")
        XCTAssertEqual(
            action?.destination,
            .rbacCanIPreset(
                verb: "list",
                resource: "deployments",
                apiGroup: "apps",
                subresource: nil,
                scope: .namespace
            )
        )
    }

    func testAPIAuthAndAuthorizationDiagnosticsLinkToOfficialDocs() {
        let auth = AuthDoctorEntryActionResolver.resolve(check: check("api-auth"), hasPodTarget: false)
        let authorization = AuthDoctorEntryActionResolver.resolve(check: check("api-authorization"), hasPodTarget: false)

        XCTAssertEqual(auth?.title, "Auth Docs")
        XCTAssertEqual(authorization?.title, "RBAC Docs")
        guard case let .documentation(authURL) = auth?.destination,
              case let .documentation(authorizationURL) = authorization?.destination else {
            return XCTFail("Expected documentation destinations")
        }
        XCTAssertTrue(authURL.absoluteString.contains("authentication"))
        XCTAssertTrue(authorizationURL.absoluteString.contains("rbac"))
    }

    func testTransportAndCloudLoginDiagnosticsUseSpecificDocumentationActions() {
        let transport = AuthDoctorEntryActionResolver.resolve(check: check("transport"), hasPodTarget: false)
        let cloudLogin = AuthDoctorEntryActionResolver.resolve(check: check("cloud-login-tools"), hasPodTarget: false)

        XCTAssertEqual(transport?.title, "API Access Docs")
        XCTAssertEqual(transport?.systemImage, "network.badge.shield.half.filled")
        XCTAssertEqual(cloudLogin?.title, "Exec Auth Docs")
        XCTAssertEqual(cloudLogin?.systemImage, "key.viewfinder")
    }

    func testCloudLoginDiagnosticsUseProviderSpecificDocumentationActions() {
        let eks = AuthDoctorEntryActionResolver.resolve(
            check: check("cloud-login-tools", message: "Missing aws on PATH. Install or sign in with the provider CLI before running cloud login."),
            hasPodTarget: false
        )
        let gke = AuthDoctorEntryActionResolver.resolve(
            check: check("cloud-login-tools", message: "Missing gcloud, gke-gcloud-auth-plugin on PATH."),
            hasPodTarget: false
        )
        let aks = AuthDoctorEntryActionResolver.resolve(
            check: check("cloud-login-tools", message: "Missing az, kubelogin on PATH."),
            hasPodTarget: false
        )

        XCTAssertEqual(eks?.title, "EKS Login Docs")
        XCTAssertEqual(gke?.title, "GKE Login Docs")
        XCTAssertEqual(aks?.title, "AKS Login Docs")

        guard case let .documentation(eksURL) = eks?.destination,
              case let .documentation(gkeURL) = gke?.destination,
              case let .documentation(aksURL) = aks?.destination else {
            return XCTFail("Expected provider documentation destinations")
        }
        XCTAssertEqual(eksURL.host, "docs.aws.amazon.com")
        XCTAssertEqual(gkeURL.host, "cloud.google.com")
        XCTAssertEqual(aksURL.host, "learn.microsoft.com")
    }

    func testProviderProfileDiagnosticsUseSpecificDocumentationActions() {
        let eks = AuthDoctorEntryActionResolver.resolve(
            check: check("eks-role-profile", message: "EKS role assumption was detected without exporting the role ARN."),
            hasPodTarget: false
        )
        let gke = AuthDoctorEntryActionResolver.resolve(
            check: check("gke-auth-plugin-profile", message: "GKE auth plugin was found on PATH."),
            hasPodTarget: false
        )
        let aks = AuthDoctorEntryActionResolver.resolve(
            check: check("aks-kubelogin-profile", message: "AKS kubelogin was found on PATH."),
            hasPodTarget: false
        )
        let oidc = AuthDoctorEntryActionResolver.resolve(
            check: check("oidc-token-profile", message: "OIDC-style auth appears expired."),
            hasPodTarget: false
        )

        XCTAssertEqual(eks?.title, "EKS Login Docs")
        XCTAssertEqual(gke?.title, "GKE Login Docs")
        XCTAssertEqual(aks?.title, "AKS Login Docs")
        XCTAssertEqual(oidc?.title, "OIDC Auth Docs")
    }

    func testAdditionalProviderDiagnosticsUseSpecificDocumentationActions() {
        let doks = AuthDoctorEntryActionResolver.resolve(
            check: check("cloud-login-tools", message: "Missing doctl on PATH for DOKS auth."),
            hasPodTarget: false
        )
        let rancher = AuthDoctorEntryActionResolver.resolve(
            check: check("cloud-login-tools", message: "Missing rancher on PATH for Rancher auth."),
            hasPodTarget: false
        )
        let openshift = AuthDoctorEntryActionResolver.resolve(
            check: check("cloud-login-tools", message: "Missing oc on PATH for OpenShift auth."),
            hasPodTarget: false
        )
        let doksProfile = AuthDoctorEntryActionResolver.resolve(
            check: check("doks-doctl-profile", message: "DOKS doctl was found on PATH."),
            hasPodTarget: false
        )
        let rancherProfile = AuthDoctorEntryActionResolver.resolve(
            check: check("rancher-cli-profile", message: "Rancher CLI was found on PATH."),
            hasPodTarget: false
        )
        let openshiftProfile = AuthDoctorEntryActionResolver.resolve(
            check: check("openshift-cli-profile", message: "OpenShift CLI was found on PATH."),
            hasPodTarget: false
        )

        XCTAssertEqual(doks?.title, "DOKS Login Docs")
        XCTAssertEqual(rancher?.title, "Rancher Kubeconfig Docs")
        XCTAssertEqual(openshift?.title, "OpenShift CLI Docs")
        XCTAssertEqual(doksProfile?.title, "DOKS Login Docs")
        XCTAssertEqual(rancherProfile?.title, "Rancher Kubeconfig Docs")
        XCTAssertEqual(openshiftProfile?.title, "OpenShift CLI Docs")

        guard case let .documentation(doksURL) = doks?.destination,
              case let .documentation(rancherURL) = rancher?.destination,
              case let .documentation(openshiftURL) = openshift?.destination,
              case let .documentation(doksProfileURL) = doksProfile?.destination,
              case let .documentation(rancherProfileURL) = rancherProfile?.destination,
              case let .documentation(openshiftProfileURL) = openshiftProfile?.destination else {
            return XCTFail("Expected provider documentation destinations")
        }
        XCTAssertEqual(doksURL.host, "docs.digitalocean.com")
        XCTAssertEqual(rancherURL.host, "ranchermanager.docs.rancher.com")
        XCTAssertEqual(openshiftURL.host, "docs.redhat.com")
        XCTAssertEqual(doksProfileURL.host, "docs.digitalocean.com")
        XCTAssertEqual(rancherProfileURL.host, "ranchermanager.docs.rancher.com")
        XCTAssertEqual(openshiftProfileURL.host, "docs.redhat.com")
    }

    func testUnknownEntryHasNoAction() {
        XCTAssertNil(AuthDoctorEntryActionResolver.resolve(check: check("synthetic-unknown"), hasPodTarget: true))
    }

    private func check(
        _ id: String,
        status: RuneHealthCheckStatus = .passed,
        message: String = "Synthetic"
    ) -> RuneHealthCheck {
        RuneHealthCheck(id: id, title: "Synthetic", status: status, message: message)
    }
}
