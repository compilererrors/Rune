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

    /// What a K3s 1.36 API server answers when a label or annotation value
    /// is not a string: HTTP 500 with one line per field. The draft is at
    /// fault, so each field is an error at its own line, never a transport
    /// warning that would let the dry-run count as passed.
    func testTypedPatchStringFailuresAreErrorsAtTheirFields() throws {
        let yaml = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: synthetic-settings
          namespace: synthetic-zone
          labels:
            enabled: yes
          annotations:
            example.test/count: 3
        data:
          key: value
        """
        let output = """
        HTTP 500: failed to create typed patch object (synthetic-zone/synthetic-settings; /v1, Kind=ConfigMap): errors:
          .metadata.annotations.example.test/count: expected string, got &value.valueUnstructured{Value:3}
          .metadata.labels.enabled: expected string, got &value.valueUnstructured{Value:true}
        """

        let issues = KubernetesClient.parseValidationIssues(from: output, yaml: yaml)

        XCTAssertEqual(issues.map(\.source), [.kubernetes, .kubernetes])
        XCTAssertEqual(issues.map(\.severity), [.error, .error])
        XCTAssertEqual(issues.map(\.message), [
            "`metadata.annotations.example.test/count` must be a string. Quote the value before applying.",
            "`metadata.labels.enabled` must be a string. Quote the value before applying.",
        ])
        XCTAssertEqual(issues.map(\.line), [9, 7])
    }

    func testAnUnreadTypedPatchFailureIsStillAnError() throws {
        let output = "HTTP 500: failed to create typed patch object (synthetic-zone/synthetic-settings; /v1, Kind=ConfigMap): synthetic unknown reason"

        let issue = try XCTUnwrap(KubernetesClient.parseValidationIssues(from: output, yaml: "kind: ConfigMap").first)

        XCTAssertEqual(issue.source, .kubernetes)
        XCTAssertEqual(issue.severity, .error)
    }
}
