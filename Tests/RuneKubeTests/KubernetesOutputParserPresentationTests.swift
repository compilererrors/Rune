import RuneKube
import XCTest

final class KubernetesOutputParserPresentationTests: XCTestCase {
    func testConfigMapSummariesUseSpecificPluralizedValueCounts() throws {
        let raw = #"""
        {
          "items": [
            {
              "metadata": { "name": "text-only", "namespace": "sample-namespace" },
              "data": { "setting": "enabled" }
            },
            {
              "metadata": { "name": "mixed-values", "namespace": "sample-namespace" },
              "data": { "first": "one", "second": "two" },
              "binaryData": { "payload": "AA==" }
            }
          ]
        }
        """#

        let summaries = try KubernetesOutputParser().parseConfigMaps(
            namespace: "sample-namespace",
            from: raw
        )

        XCTAssertEqual(summaries.map(\.name), ["mixed-values", "text-only"])
        XCTAssertEqual(summaries[0].primaryText, "3 keys")
        XCTAssertEqual(summaries[0].secondaryText, "2 text values · 1 binary value")
        XCTAssertEqual(summaries[1].primaryText, "1 key")
        XCTAssertEqual(summaries[1].secondaryText, "1 text value · 0 binary values")
    }

    func testGenericSummariesExposeSemanticProgressSelectorsAndPluralization() throws {
        let parser = KubernetesOutputParser()
        let jobs = try parser.parseJobs(
            namespace: "sample-namespace",
            from: #"{"items":[{"metadata":{"name":"batch","namespace":"sample-namespace"},"spec":{"completions":3},"status":{"active":1,"succeeded":2}}]}"#
        )
        let policies = try parser.parseNetworkPolicies(
            namespace: "sample-namespace",
            from: #"{"items":[{"metadata":{"name":"allow-web","namespace":"sample-namespace"},"spec":{"policyTypes":["Ingress"],"podSelector":{"matchLabels":{"app":"sample"}}}}]}"#
        )
        let secrets = try parser.parseSecrets(
            namespace: "sample-namespace",
            from: #"{"items":[{"metadata":{"name":"credentials","namespace":"sample-namespace"},"type":"Opaque","data":{"token":"AA=="}}]}"#
        )

        XCTAssertEqual(jobs.first?.primaryText, "Running (1)")
        XCTAssertEqual(jobs.first?.secondaryText, "2/3 complete")
        XCTAssertEqual(policies.first?.primaryText, "Ingress")
        XCTAssertEqual(policies.first?.secondaryText, "app=sample")
        XCTAssertEqual(secrets.first?.primaryText, "Opaque")
        XCTAssertEqual(secrets.first?.secondaryText, "1 value")
    }

    func testExpressionSelectorsArePresentedAsScopedSelections() throws {
        let parser = KubernetesOutputParser()
        let policies = try parser.parseNetworkPolicies(
            namespace: "sample-namespace",
            from: #"{"items":[{"metadata":{"name":"expression-policy","namespace":"sample-namespace"},"spec":{"policyTypes":["Ingress"],"podSelector":{"matchExpressions":[{"key":"tier","operator":"In","values":["api","web"]}]}}}]}"#
        )
        let statefulSets = try parser.parseStatefulSets(
            namespace: "sample-namespace",
            from: #"{"items":[{"metadata":{"name":"expression-workload","namespace":"sample-namespace"},"spec":{"replicas":2,"selector":{"matchExpressions":[{"key":"deprecated","operator":"DoesNotExist"}]}},"status":{"readyReplicas":1}}]}"#
        )

        XCTAssertEqual(policies.first?.secondaryText, "tier in (api, web)")
        XCTAssertNotEqual(policies.first?.secondaryText, "All pods")
        XCTAssertEqual(statefulSets.first?.secondaryText, "!deprecated")
    }
}
