import XCTest
@testable import RuneCore
@testable import RuneSecurity

final class ManualTokenKubeConfigBuilderTests: XCTestCase {
    func testBuildYAMLQuotesValuesAndNormalizesContextName() throws {
        let yaml = try ManualTokenKubeConfigBuilder.buildYAML(for: ManualTokenKubeConfigRequest(
            name: " Manual Token Cluster ",
            server: " https://example.invalid:6443 ",
            namespace: " team alpha ",
            token: " synthetic-token-value "
        ))

        XCTAssertTrue(yaml.contains(#"current-context: "Manual-Token-Cluster""#))
        XCTAssertTrue(yaml.contains(#"server: "https://example.invalid:6443""#))
        XCTAssertTrue(yaml.contains(#"namespace: "team alpha""#))
        XCTAssertTrue(yaml.contains(#"token: "synthetic-token-value""#))
        XCTAssertFalse(yaml.contains(" Manual Token Cluster "))
    }

    func testBuildYAMLUsesServerHostFallbackAndOmitsBlankNamespace() throws {
        let yaml = try ManualTokenKubeConfigBuilder.buildYAML(for: ManualTokenKubeConfigRequest(
            name: "",
            server: "https://cluster.example.invalid",
            namespace: " ",
            token: "synthetic-token-value"
        ))

        XCTAssertTrue(yaml.contains(#"current-context: "cluster.example.invalid""#))
        XCTAssertFalse(yaml.contains("namespace:"))
    }

    func testBuildYAMLRejectsInvalidServerAndMissingToken() {
        XCTAssertThrowsError(try ManualTokenKubeConfigBuilder.buildYAML(for: ManualTokenKubeConfigRequest(
            name: "demo",
            server: "file:///tmp/config",
            namespace: "demo",
            token: "synthetic-token-value"
        ))) { error in
            XCTAssertTrue(String(describing: error).contains("valid HTTP or HTTPS URL"))
        }

        XCTAssertThrowsError(try ManualTokenKubeConfigBuilder.buildYAML(for: ManualTokenKubeConfigRequest(
            name: "demo",
            server: "https://example.invalid",
            namespace: "demo",
            token: " "
        ))) { error in
            XCTAssertTrue(String(describing: error).contains("token is required"))
        }
    }
}
