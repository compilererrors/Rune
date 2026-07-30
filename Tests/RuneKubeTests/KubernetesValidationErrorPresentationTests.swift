import XCTest
@testable import RuneCore
@testable import RuneKube

final class KubernetesValidationErrorPresentationTests: XCTestCase {
    func testKubernetesStatusHTTPErrorUsesMessageInsteadOfRawEnvelope() {
        let body = """
        {
          "kind": "Status",
          "apiVersion": "v1",
          "status": "Failure",
          "message": "Synthetic Kubernetes validation failed.",
          "reason": "Invalid",
          "code": 422
        }
        """

        let message = KubernetesRESTErrorMessageFormatter.httpErrorMessage(
            statusCode: 422,
            responseBody: body
        )

        XCTAssertEqual(message, "HTTP 422: Synthetic Kubernetes validation failed.")
        XCTAssertFalse(message.contains(#""kind""#))
        XCTAssertFalse(message.contains(#""reason""#))
    }

    func testHTTPErrorFormatterKeepsFallbackForNonStatusResponses() {
        XCTAssertEqual(
            KubernetesRESTErrorMessageFormatter.httpErrorMessage(
                statusCode: 500,
                responseBody: "synthetic plain-text failure"
            ),
            "HTTP 500: synthetic plain-text failure"
        )
        XCTAssertEqual(
            KubernetesRESTErrorMessageFormatter.httpErrorMessage(
                statusCode: 503,
                responseBody: "  "
            ),
            "HTTP 503"
        )
    }

    func testRetryAdviceIsOnlyAddedToSafeReadMethods() {
        let decision = KubernetesRequestRetryPolicy.classifyHTTPStatus(500)
        let message = "HTTP 500: Synthetic temporary failure."

        XCTAssertEqual(
            KubernetesRESTErrorMessageFormatter.appendingRetryAdvice(
                to: message,
                method: "PATCH",
                decision: decision
            ),
            message
        )

        let readMessage = KubernetesRESTErrorMessageFormatter.appendingRetryAdvice(
            to: message,
            method: "GET",
            decision: decision
        )
        XCTAssertTrue(readMessage.contains("Temporary Kubernetes API error."))
        XCTAssertTrue(readMessage.contains("safely retry this read"))
        XCTAssertFalse(readMessage.contains("serverUnavailable"))
    }

    func testGenericServerFailureIsPresentedAsTransportInsteadOfManifestError() throws {
        let issues = KubernetesClient.parseValidationIssues(
            from: "HTTP 503: Synthetic temporary API failure.",
            yaml: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: synthetic"
        )
        let issue = try XCTUnwrap(issues.first)

        XCTAssertEqual(issues.count, 1)
        XCTAssertEqual(issue.source, .transport)
        XCTAssertEqual(issue.severity, .warning)
        XCTAssertEqual(issue.message, "HTTP 503: Synthetic temporary API failure.")
    }

    func testTypedPatchNumericStringBecomesConciseKubernetesValidationIssue() throws {
        let yaml = """
        apiVersion: v1
        kind: Pod
        metadata:
          name: synthetic-workload
          namespace: synthetic-zone
          generation: not-a-number
        spec:
          containers: []
        """
        let statusBody = """
        {
          "kind": "Status",
          "apiVersion": "v1",
          "metadata": {},
          "status": "Failure",
          "message": "failed to create typed patch object (synthetic-zone/synthetic-workload; /v1, Kind=Pod): .metadata.generation: expected numeric (int or float), got string",
          "reason": "InternalError",
          "code": 500
        }
        """
        let formatted = KubernetesRESTErrorMessageFormatter.httpErrorMessage(
            statusCode: 500,
            responseBody: statusBody
        )
        let output = KubernetesRESTErrorMessageFormatter.appendingRetryAdvice(
            to: formatted,
            method: "PATCH",
            decision: KubernetesRequestRetryPolicy.classifyHTTPStatus(500)
        )

        let issues = KubernetesClient.parseValidationIssues(from: output, yaml: yaml)
        let issue = try XCTUnwrap(issues.first)

        XCTAssertEqual(issues.count, 1)
        XCTAssertEqual(issue.source, .kubernetes)
        XCTAssertEqual(issue.severity, .error)
        XCTAssertEqual(
            issue.message,
            "`metadata.generation` must be a number. Fix or remove the field before applying."
        )
        XCTAssertEqual(issue.line, 6)
        XCTAssertEqual(issue.column, 3)
        XCTAssertNotNil(issue.range)
        XCTAssertFalse(issue.message.contains("HTTP 500"))
        XCTAssertFalse(issue.message.contains("synthetic-workload"))
        XCTAssertFalse(issue.message.contains("safe"))
    }
}
