import XCTest
@testable import RuneCore
@testable import RuneKube

final class KubectlTests: XCTestCase {
    func testContextArguments() {
        let builder = KubectlCommandBuilder()
        XCTAssertEqual(builder.contextListArguments(), ["config", "get-contexts", "-o", "name"])
    }

    func testNamespaceArguments() {
        let builder = KubectlCommandBuilder()
        XCTAssertEqual(
            builder.namespaceListArguments(context: "prod"),
            ["--context", "prod", "get", "namespaces", "-o", "custom-columns=NAME:.metadata.name", "--no-headers"]
        )
    }

    func testContextNamespaceArguments() {
        let builder = KubectlCommandBuilder()
        XCTAssertEqual(
            builder.contextNamespaceArguments(context: "prod"),
            ["config", "view", "--minify", "--context", "prod", "-o", "jsonpath={..namespace}"]
        )
    }

    func testNamespacedResourceCountArguments() {
        let builder = KubectlCommandBuilder()
        XCTAssertEqual(
            builder.namespacedResourceCountArguments(
                context: "prod",
                namespace: "default",
                resource: "deployments"
            ),
            ["--context", "prod", "get", "deployments", "-n", "default", "-o", "custom-columns=NAME:.metadata.name", "--no-headers"]
        )
    }

    func testClusterResourceCountArguments() {
        let builder = KubectlCommandBuilder()
        XCTAssertEqual(
            builder.clusterResourceCountArguments(context: "prod", resource: "nodes"),
            ["--context", "prod", "get", "nodes", "-o", "custom-columns=NAME:.metadata.name", "--no-headers"]
        )
    }

    func testPodAllNamespacesArguments() {
        let builder = KubectlCommandBuilder()
        let args = builder.podListAllNamespacesArguments(context: "prod")

        XCTAssertEqual(args, ["--context", "prod", "get", "pods", "-A", "-o", "custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,STATUS:.status.phase", "--no-headers"])
    }

    func testLogsArgumentsWithRelativeFilter() {
        let builder = KubectlCommandBuilder()

        let args = builder.podLogsArguments(
            context: "dev",
            namespace: "default",
            podName: "api-1",
            container: nil,
            filter: .lastMinutes(15),
            previous: false,
            follow: true
        )

        XCTAssertTrue(args.contains("--since"))
        XCTAssertFalse(args.contains("--since-time"))
        XCTAssertTrue(args.contains("-f"))
        XCTAssertTrue(args.contains("--timestamps"))
    }

    func testPodParsing() {
        let parser = KubectlOutputParser()
        let raw = "api-6f99fdcc7f-7sm5x Running\nworker-86fdd44558-cv95w Pending\n"

        let pods = parser.parsePods(namespace: "default", from: raw)

        XCTAssertEqual(pods.count, 2)
        XCTAssertEqual(pods.first?.name, "api-6f99fdcc7f-7sm5x")
        XCTAssertEqual(pods.first?.status, "Running")
    }

    func testNamespaceParsing() {
        let parser = KubectlOutputParser()
        let raw = "kube-system\n default \nplatform\n"
        XCTAssertEqual(parser.parseNamespaces(from: raw), ["default", "kube-system", "platform"])
    }

    func testLogsArgumentsWithAbsoluteTimeUsesSinceTime() {
        let builder = KubectlCommandBuilder()

        let args = builder.podLogsArguments(
            context: "dev",
            namespace: "default",
            podName: "api-1",
            container: nil,
            filter: .since(Date(timeIntervalSince1970: 0)),
            previous: true,
            follow: false
        )

        XCTAssertTrue(args.contains("--since-time"))
        XCTAssertFalse(args.contains("--since"))
        XCTAssertTrue(args.contains("--previous"))
    }

    func testPodYAMLArguments() {
        let builder = KubectlCommandBuilder()
        let args = builder.resourceYAMLArguments(context: "dev", namespace: "default", kind: .pod, name: "api-1")

        XCTAssertEqual(args, ["--context", "dev", "get", "pod", "api-1", "-n", "default", "-o", "yaml"])
    }

    func testPodExecArguments() {
        let builder = KubectlCommandBuilder()
        let args = builder.podExecArguments(
            context: "dev",
            namespace: "default",
            podName: "api-1",
            container: nil,
            command: ["printenv", "HOSTNAME"]
        )

        XCTAssertEqual(args, ["--context", "dev", "exec", "api-1", "-n", "default", "--", "printenv", "HOSTNAME"])
    }

    func testParseDeployments() throws {
        let parser = KubectlOutputParser()
        let raw = """
        {"items":[
          {"metadata":{"name":"api"},"spec":{"replicas":3},"status":{"readyReplicas":2}},
          {"metadata":{"name":"worker"},"spec":{"replicas":1},"status":{"readyReplicas":1}}
        ]}
        """

        let deployments = try parser.parseDeployments(namespace: "default", from: raw)

        XCTAssertEqual(deployments.count, 2)
        XCTAssertEqual(deployments.first?.name, "api")
        XCTAssertEqual(deployments.first?.replicaText, "2/3")
    }

    func testParseServiceSelector() throws {
        let parser = KubectlOutputParser()
        let raw = """
        {
          "metadata":{"name":"svc"},
          "spec":{"selector":{"app":"api","tier":"backend"}}
        }
        """

        let selector = try parser.parseServiceSelector(from: raw)

        XCTAssertEqual(selector["app"], "api")
        XCTAssertEqual(selector["tier"], "backend")
    }

    func testDeploymentJSONArguments() {
        let builder = KubectlCommandBuilder()
        let args = builder.deploymentJSONArguments(context: "prod", namespace: "default", deploymentName: "api")

        XCTAssertEqual(args, ["--context", "prod", "get", "deployment", "api", "-n", "default", "-o", "json"])
    }

    func testRolloutRestartArguments() {
        let builder = KubectlCommandBuilder()
        let args = builder.rolloutRestartArguments(context: "prod", namespace: "default", deploymentName: "api")

        XCTAssertEqual(args, ["--context", "prod", "rollout", "restart", "deployment", "api", "-n", "default"])
    }

    func testPortForwardArguments() {
        let builder = KubectlCommandBuilder()
        let args = builder.portForwardArguments(
            context: "prod",
            namespace: "default",
            targetKind: .service,
            targetName: "api-svc",
            localPort: 8080,
            remotePort: 80,
            address: "127.0.0.1"
        )

        XCTAssertEqual(args, ["--context", "prod", "port-forward", "service/api-svc", "8080:80", "-n", "default", "--address", "127.0.0.1"])
    }

    func testRolloutHistoryArguments() {
        let builder = KubectlCommandBuilder()
        let args = builder.rolloutHistoryArguments(context: "prod", namespace: "default", deploymentName: "api")

        XCTAssertEqual(args, ["--context", "prod", "rollout", "history", "deployment", "api", "-n", "default"])
    }

    func testRolloutUndoArgumentsWithRevision() {
        let builder = KubectlCommandBuilder()
        let args = builder.rolloutUndoArguments(context: "prod", namespace: "default", deploymentName: "api", revision: 3)

        XCTAssertEqual(args, ["--context", "prod", "rollout", "undo", "deployment", "api", "-n", "default", "--to-revision", "3"])
    }

    func testParseDeploymentSelector() throws {
        let parser = KubectlOutputParser()
        let raw = """
        {
          "spec":{"selector":{"matchLabels":{"app":"api","component":"web"}}}
        }
        """

        let selector = try parser.parseDeploymentSelector(from: raw)

        XCTAssertEqual(selector["app"], "api")
        XCTAssertEqual(selector["component"], "web")
    }
}
