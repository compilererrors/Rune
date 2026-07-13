import XCTest
@testable import RuneCore
@testable import RuneUI

final class AuthDoctorFailureProjectorTests: XCTestCase {
    func testClassifiesExecCredentialFailures() throws {
        let check = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "ExecCredential plugin exited with status 1 and returned invalid JSON"
        ).first)

        XCTAssertEqual(check.id, "exec-auth")
        XCTAssertEqual(check.status, .failed)
        XCTAssertTrue(check.message.contains("invalid ExecCredential JSON"))
    }

    func testClassifiesSpecificExecCredentialFailureModes() throws {
        let timeout = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "Command failed: kubeconfig exec auth provider: Timed out after 25 seconds"
        ).first)
        let apiVersion = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "Kubeconfig exec auth returned apiVersion client.authentication.k8s.io/v1beta1, expected client.authentication.k8s.io/v1"
        ).first)
        let missingStatus = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "Kubeconfig exec auth response is missing status"
        ).first)
        let incompleteCertificate = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "Kubeconfig exec auth returned incomplete client certificate credentials"
        ).first)
        let missingCredential = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "Kubeconfig exec auth returned missing token or client certificate credentials"
        ).first)
        let missingCommand = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "Command failed: kubeconfig exec auth provider: executable file not found"
        ).first)
        let nonZeroExit = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "Command failed: kubeconfig exec auth provider: not logged in"
        ).first)

        XCTAssertTrue(timeout.message.contains("timed out"))
        XCTAssertTrue(apiVersion.message.contains("API version"))
        XCTAssertTrue(missingStatus.message.contains("without status credentials"))
        XCTAssertTrue(incompleteCertificate.message.contains("incomplete credentials"))
        XCTAssertTrue(missingCredential.message.contains("incomplete credentials"))
        XCTAssertTrue(missingCommand.message.contains("could not be started"))
        XCTAssertTrue(nonZeroExit.message.contains("returned an error"))
    }

    func testClassifiesDisabledCLIBackedAuthAsExecAuthFailure() throws {
        let check = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: RuneExternalCommandPolicy.disabledMessage
        ).first)

        XCTAssertEqual(check.id, "exec-auth")
        XCTAssertEqual(check.status, .failed)
        XCTAssertEqual(check.message, RuneExternalCommandPolicy.disabledMessage)
    }

    func testClassifiesMissingNativeProviderProfileWithoutLeakingProviderDetails() throws {
        let check = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "Connect Amazon EKS credentials in Rune before using this context."
        ).first)

        XCTAssertEqual(check.id, "native-auth-profile")
        XCTAssertEqual(check.status, .failed)
        XCTAssertTrue(check.message.contains("connected native provider profile"))
        XCTAssertFalse(check.message.contains("Amazon EKS"))
    }

    func testClassifiesAPIAuthenticationFailures() throws {
        let check = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "HTTP status 401 Unauthorized: invalid bearer token"
        ).first)

        XCTAssertEqual(check.id, "api-auth")
        XCTAssertEqual(check.status, .failed)
        XCTAssertTrue(check.message.contains("rejected the configured credentials"))
    }

    func testClassifiesClientCertificateAuthenticationFailures() throws {
        let pairing = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "Client certificate and key in kubeconfig could not be paired into a TLS identity: OSStatus -25300"
        ).first)
        let challenge = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "client-certificate challenge default-handling host=api.internal reason=no-client-identity"
        ).first)

        XCTAssertEqual(pairing.id, "client-certificate-auth")
        XCTAssertEqual(pairing.status, .failed)
        XCTAssertTrue(pairing.message.contains("client certificate"))
        XCTAssertEqual(challenge.id, "client-certificate-auth")
        XCTAssertFalse(pairing.message.contains("api.internal"))
    }

    func testClassifiesAPIAuthorizationFailures() throws {
        let check = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "pods is forbidden: User cannot list resource pods in namespace default"
        ).first)

        XCTAssertEqual(check.id, "api-authorization")
        XCTAssertEqual(check.status, .warning)
        XCTAssertTrue(check.message.contains("missing permission"))
    }

    func testProjectsFailedAPIEndpointWithoutLeakingPathValuesOrQueryTokens() throws {
        let checks = AuthDoctorFailureProjector.checks(
            for: "HTTP status 403 GET https://cluster.example.invalid/api/v1/namespaces/team-a/pods/api-0/log?container=app&token=secret-token"
        )
        let endpoint = try XCTUnwrap(checks.first { $0.id == "api-request-endpoint" })

        XCTAssertEqual(checks.first?.id, "api-authorization")
        XCTAssertEqual(endpoint.status, .warning)
        XCTAssertTrue(endpoint.message.contains("/api/v1/namespaces/<namespace>/pods/<name>/log?container=<redacted>&token=<redacted>"))
        XCTAssertFalse(endpoint.message.contains("team-a"))
        XCTAssertFalse(endpoint.message.contains("api-0"))
        XCTAssertFalse(endpoint.message.contains("secret-token"))
        XCTAssertFalse(endpoint.message.contains("cluster.example.invalid"))
    }

    func testBoundsOversizedFailureMessagesBeforeEndpointScanning() throws {
        let secretSuffix = String(repeating: "x", count: 8_000)
            + " https://cluster.example.invalid/api/v1/namespaces/team-a/pods/api-0/log?token=secret-token"
        let checks = AuthDoctorFailureProjector.checks(
            for: "TLS handshake failed: x509 certificate signed by unknown authority \(secretSuffix)"
        )

        let check = try XCTUnwrap(checks.first)
        XCTAssertEqual(check.id, "transport")
        XCTAssertFalse(checks.contains { $0.id == "api-request-endpoint" })
        XCTAssertFalse(checks.map(\.message).joined(separator: " ").contains("secret-token"))
        XCTAssertFalse(checks.map(\.message).joined(separator: " ").contains("team-a"))
    }

    func testClassifiesTLSProxyAndConnectivityFailures() throws {
        let tls = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "TLS handshake failed: x509 certificate signed by unknown authority"
        ).first)
        let proxy = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "Proxy CONNECT tunnel failed with HTTP 407"
        ).first)
        let connectivity = try XCTUnwrap(AuthDoctorFailureProjector.checks(
            for: "DNS resolution timed out while connecting to the Kubernetes API"
        ).first)

        XCTAssertEqual(tls.id, "transport")
        XCTAssertTrue(tls.message.contains("TLS or custom CA"))
        XCTAssertEqual(proxy.id, "transport")
        XCTAssertTrue(proxy.message.contains("Proxy routing failed"))
        XCTAssertEqual(connectivity.id, "transport")
        XCTAssertTrue(connectivity.message.contains("DNS"))
    }

    func testReturnsNoChecksForUnknownFailures() {
        XCTAssertTrue(AuthDoctorFailureProjector.checks(for: "synthetic parse failure").isEmpty)
    }
}
