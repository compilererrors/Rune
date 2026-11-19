import RuneCore
import RuneDiagnostics
import XCTest

/// A support bundle carries the manifest of the selected resource. When
/// that is a Secret, its values must not travel with it: the keys of a
/// Secret can have any name, so they are not found by a list of key names.
@MainActor
final class SupportBundleSecretRedactionTests: XCTestCase {
    private func bundle(yaml: String, describe: String = "") throws -> (yaml: String, describe: String, json: String) {
        let state = RuneAppState()
        state.setResourceYAML(yaml)
        state.setResourceDescribe(describe)
        let request = SupportBundleRequest.snapshot(
            state: state,
            generatedAt: "2026-05-06T00:00:00Z",
            resourceCounts: [:],
            selectedResourceKind: "Secret",
            selectedResourceName: "database"
        )
        let data = try JSONSupportBundleBuilder().buildBundle(from: request)
        let decoded = try JSONDecoder().decode(SupportBundleRequest.self, from: data)
        return (decoded.resourceYAML, decoded.resourceDescribe, String(decoding: data, as: UTF8.self))
    }

    func testSecretValuesAreNotInTheBundle() throws {
        let yaml = """
        apiVersion: v1
        kind: Secret
        metadata:
          name: database
          namespace: synthetic
          annotations:
            kubectl.kubernetes.io/last-applied-configuration: |
              {"apiVersion":"v1","data":{"DATABASE_URL":"c3ludGhldGljLWxhc3QtYXBwbGllZA=="},"kind":"Secret"}
            team: synthetic
        type: Opaque
        data:
          DATABASE_URL: c3ludGhldGljLWRhdGFiYXNlLXVybA==
          api-key: c3ludGhldGljLWFwaS1rZXk=
          tls.key: |
            c3ludGhldGljLXRscy1rZXktbGluZS0x
            c3ludGhldGljLXRscy1rZXktbGluZS0y
        stringData:
          plain: synthetic-plain-value
        immutable: false
        """

        let result = try bundle(yaml: yaml)

        for value in [
            "c3ludGhldGljLWRhdGFiYXNlLXVybA==",
            "c3ludGhldGljLWFwaS1rZXk=",
            "c3ludGhldGljLXRscy1rZXktbGluZS0x",
            "c3ludGhldGljLXRscy1rZXktbGluZS0y",
            "synthetic-plain-value",
            "c3ludGhldGljLWxhc3QtYXBwbGllZA=="
        ] {
            XCTAssertFalse(result.json.contains(value), "\(value) is in the bundle")
        }
        XCTAssertEqual(result.yaml, """
        apiVersion: v1
        kind: Secret
        metadata:
          name: database
          namespace: synthetic
          annotations:
            kubectl.kubernetes.io/last-applied-configuration: <redacted>
            team: synthetic
        type: Opaque
        data:
          DATABASE_URL: <redacted>
          api-key: <redacted>
          tls.key: <redacted>
        stringData:
          plain: <redacted>
        immutable: false
        """)
    }

    func testSecretValuesInTheDescribeJSONAreNotInTheBundle() throws {
        let describe = """
        Name: database
        Namespace: synthetic
        Kind: Secret

        Manifest JSON:
        {
          "apiVersion" : "v1",
          "data" : {
            "DATABASE_URL" : "c3ludGhldGljLWRhdGFiYXNlLXVybA==",
            "api-key" : "c3ludGhldGljLWFwaS1rZXk="
          },
          "kind" : "Secret",
          "metadata" : {
            "annotations" : {
              "kubectl.kubernetes.io/last-applied-configuration" : "{\\"data\\":{\\"api-key\\":\\"c3ludGhldGljLWxhc3QtYXBwbGllZA==\\"}}"
            },
            "name" : "database"
          },
          "type" : "Opaque"
        }

        Events:
        <none>
        """

        let result = try bundle(yaml: "", describe: describe)

        XCTAssertFalse(result.json.contains("c3ludGhldGljLWRhdGFiYXNlLXVybA=="))
        XCTAssertFalse(result.json.contains("c3ludGhldGljLWFwaS1rZXk="))
        XCTAssertFalse(result.json.contains("c3ludGhldGljLWxhc3QtYXBwbGllZA=="))
        XCTAssertTrue(result.describe.contains(#""DATABASE_URL" : "<redacted>","#))
        XCTAssertTrue(result.describe.contains(#""api-key" : "<redacted>""#))
        XCTAssertTrue(result.describe.contains(#""kubectl.kubernetes.io/last-applied-configuration" : "<redacted>""#))
        XCTAssertTrue(result.describe.contains(#""name" : "database""#), "the rest of the manifest stays")
        XCTAssertTrue(result.describe.contains(#""type" : "Opaque""#))
        XCTAssertTrue(result.describe.hasSuffix("Events:\n<none>"))
    }

    func testOtherResourcesKeepTheirData() throws {
        let configMap = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: settings
        data:
          LOG_LEVEL: debug
          note: mentions a Secret in passing
        """

        XCTAssertEqual(try bundle(yaml: configMap).yaml, configMap)
    }

    func testOnlyTheSecretOfSeveralDocumentsIsRedacted() throws {
        let yaml = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: settings
        data:
          LOG_LEVEL: debug
        ---
        apiVersion: v1
        kind: "Secret"
        metadata:
          name: database
        data: {inline: c3ludGhldGljLWlubGluZQ==}
        """

        let result = try bundle(yaml: yaml)

        XCTAssertTrue(result.yaml.contains("LOG_LEVEL: debug"))
        XCTAssertFalse(result.yaml.contains("c3ludGhldGljLWlubGluZQ=="))
        XCTAssertTrue(result.yaml.contains("data: <redacted>"))
    }
}
