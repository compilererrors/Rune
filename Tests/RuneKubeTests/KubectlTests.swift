import XCTest
@testable import RuneCore
@testable import RuneKube

final class KubectlTests: XCTestCase {
    func testContextArguments() {
        let builder = KubectlCommandBuilder()
        XCTAssertEqual(builder.contextListArguments(), ["config", "get-contexts", "-o", "name"])
    }

    func testConfigViewJSONArguments() {
        let builder = KubectlCommandBuilder()
        XCTAssertEqual(
            builder.configViewJSONArguments(context: "prod"),
            ["config", "view", "--context", "prod", "-o", "json"]
        )
    }

    func testKubeConfigViewJSONDecodesForDirectREST() async throws {
        let json = """
        {
          "clusters": [
            {
              "name": "c1",
              "cluster": {
                "server": "https://127.0.0.1:6443",
                "certificate-authority-data": "QUJD"
              }
            }
          ],
          "users": [
            {
              "name": "u1",
              "user": { "token": "secret" }
            }
          ],
          "contexts": [
            {
              "name": "ctx1",
              "context": { "cluster": "c1", "user": "u1" }
            }
          ]
        }
        """
        let view = try JSONDecoder().decode(KubeConfigViewJSON.self, from: Data(json.utf8))
        let base = URL(fileURLWithPath: "/tmp")
        let creds = await KubeRESTCredentialResolver.resolve(
            view: view,
            contextName: "ctx1",
            kubeconfigDirectoryForRelativePaths: base,
            baseEnvironment: [:],
            runner: ProcessCommandRunner(),
            execTimeout: 30
        )
        XCTAssertNotNil(creds)
        XCTAssertEqual(creds?.serverURL.absoluteString, "https://127.0.0.1:6443")
        XCTAssertEqual(creds?.bearerToken, "secret")
        XCTAssertEqual(creds?.anchorCertificateDER, Data(base64Encoded: "QUJD"))
    }

    func testExecCredentialJSONDecodes() throws {
        let json = """
        {
          "apiVersion": "client.authentication.k8s.io/v1",
          "kind": "ExecCredential",
          "status": {
            "token": "tok",
            "expirationTimestamp": "2030-01-01T00:00:00Z"
          }
        }
        """
        let decoded = try JSONDecoder().decode(ExecCredentialResponseJSON.self, from: Data(json.utf8))
        XCTAssertEqual(decoded.status?.token, "tok")
        XCTAssertEqual(decoded.status?.expirationTimestamp, "2030-01-01T00:00:00Z")
    }

    func testNamespaceArguments() {
        let builder = KubectlCommandBuilder()
        XCTAssertEqual(
            builder.namespaceListArguments(context: "prod"),
            ["--context", "prod", "--request-timeout=90s", "get", "namespaces", "-o", "custom-columns=NAME:.metadata.name", "--no-headers"]
        )
    }

    func testRawListMetadataPathForDeployments() {
        let builder = KubectlCommandBuilder()
        XCTAssertEqual(
            builder.namespacedResourceListMetadataAPIPath(namespace: "default", resource: "deployments"),
            "/apis/apps/v1/namespaces/default/deployments?limit=1"
        )
        XCTAssertEqual(
            builder.rawGetArguments(context: "prod", apiPath: "/apis/apps/v1/namespaces/default/deployments?limit=1"),
            ["--context", "prod", "get", "--raw", "/apis/apps/v1/namespaces/default/deployments?limit=1"]
        )
        let req = KubernetesRESTRequest(apiPath: "/apis/apps/v1/namespaces/default/deployments?limit=1")
        XCTAssertEqual(
            builder.rawGetArguments(context: "prod", request: req),
            ["--context", "prod", "get", "--raw", "/apis/apps/v1/namespaces/default/deployments?limit=1"]
        )
    }

    func testKubernetesRESTPathJobsAndStatefulSets() {
        XCTAssertEqual(
            KubernetesRESTPath.namespacedCollectionMetadataProbe(namespace: "ns-a", resource: "jobs"),
            "/apis/batch/v1/namespaces/ns-a/jobs?limit=1"
        )
        XCTAssertEqual(
            KubernetesRESTPath.namespacedCollectionMetadataProbe(namespace: "ns-a", resource: "statefulsets"),
            "/apis/apps/v1/namespaces/ns-a/statefulsets?limit=1"
        )
    }

    func testKubernetesRESTPathMetadataProbeForPVCNetworkPolicyHPA() {
        XCTAssertEqual(
            KubernetesRESTPath.namespacedCollectionMetadataProbe(namespace: "ns-a", resource: "persistentvolumeclaims"),
            "/api/v1/namespaces/ns-a/persistentvolumeclaims?limit=1"
        )
        XCTAssertEqual(
            KubernetesRESTPath.namespacedCollectionMetadataProbe(namespace: "ns-a", resource: "networkpolicies"),
            "/apis/networking.k8s.io/v1/namespaces/ns-a/networkpolicies?limit=1"
        )
        XCTAssertEqual(
            KubernetesRESTPath.namespacedCollectionMetadataProbe(namespace: "ns-a", resource: "horizontalpodautoscalers"),
            "/apis/autoscaling/v2/namespaces/ns-a/horizontalpodautoscalers?limit=1"
        )
    }

    func testCollectionListTotalUsesRemainingItemCount() {
        let json = """
        {"metadata":{"remainingItemCount":99,"resourceVersion":"1"},"items":[{"metadata":{"name":"a"}}]}
        """
        XCTAssertEqual(KubectlListJSON.collectionListTotal(from: json), 100)
    }

    func testCollectionListTotalCompleteList() {
        let json = """
        {"metadata":{"resourceVersion":"1"},"items":[{"metadata":{"name":"a"}},{"metadata":{"name":"b"}}]}
        """
        XCTAssertEqual(KubectlListJSON.collectionListTotal(from: json), 2)
    }

    func testCollectionListTotalReturnsNilWhenContinueWithoutRemaining() {
        let json = """
        {"metadata":{"continue":"opaque","resourceVersion":"1"},"items":[{"metadata":{"name":"a"}}]}
        """
        XCTAssertNil(KubectlListJSON.collectionListTotal(from: json))
    }

    func testCronJobListTextArgumentsUsesTabsFriendlyColumns() {
        let builder = KubectlCommandBuilder()
        let args = builder.cronJobListTextArguments(context: "example-context", namespace: "example-namespace")
        XCTAssertEqual(
            args,
            [
                "--context", "example-context",
                "get", "cronjobs",
                "-n", "example-namespace",
                "--chunk-size=200",
                "--request-timeout=90s",
                "-o", "custom-columns=NAME:.metadata.name,SCHEDULE:.spec.schedule,SUSPEND:.spec.suspend",
                "--no-headers"
            ]
        )
    }

    func testParseCronJobsTableTabSeparatedSchedule() {
        let parser = KubectlOutputParser()
        let stdout = "sample-cronjob\t0 6 * * *\tfalse\n"
        let rows = parser.parseCronJobsTable(namespace: "example-namespace", from: stdout)
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows[0].name, "sample-cronjob")
        XCTAssertEqual(rows[0].primaryText, "0 6 * * *")
        XCTAssertEqual(rows[0].secondaryText, "Active")
    }

    func testParseJobsTable() {
        let parser = KubectlOutputParser()
        let stdout = "my-job\t1\t0\t0\n"
        let rows = parser.parseJobsTable(namespace: "default", from: stdout)
        XCTAssertEqual(rows.count, 1)
        XCTAssertEqual(rows[0].name, "my-job")
        XCTAssertTrue(rows[0].primaryText.contains("Complete"))
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
            ["--context", "prod", "get", "deployments", "-n", "default", "--chunk-size=200", "--request-timeout=15s", "-o", "custom-columns=NAME:.metadata.name", "--no-headers"]
        )
    }

    func testClusterResourceCountArguments() {
        let builder = KubectlCommandBuilder()
        XCTAssertEqual(
            builder.clusterResourceCountArguments(context: "prod", resource: "nodes"),
            ["--context", "prod", "get", "nodes", "--chunk-size=200", "--request-timeout=15s", "-o", "custom-columns=NAME:.metadata.name", "--no-headers"]
        )
    }

    func testPodAllNamespacesArguments() {
        let builder = KubectlCommandBuilder()
        let args = builder.podListAllNamespacesArguments(context: "prod")

        XCTAssertEqual(args, ["--context", "prod", "get", "pods", "-A", "--chunk-size=200", "--request-timeout=20s", "-o", "json"])
    }

    func testPodListArgumentsUsesJSON() {
        let builder = KubectlCommandBuilder()
        let args = builder.podListArguments(context: "prod", namespace: "default")

        XCTAssertEqual(args, ["--context", "prod", "get", "pods", "-n", "default", "--chunk-size=200", "--request-timeout=20s", "-o", "json"])
    }

    func testPodStatusListArgumentsUsesCustomColumns() {
        let builder = KubectlCommandBuilder()
        let args = builder.podStatusListArguments(context: "prod", namespace: "default")

        XCTAssertEqual(
            args,
            ["--context", "prod", "get", "pods", "-n", "default", "--chunk-size=200", "--request-timeout=90s", "-o", "custom-columns=NAME:.metadata.name,STATUS:.status.phase", "--no-headers"]
        )
    }

    func testPodListTextArgumentsIncludeRestartsAndCreated() {
        let builder = KubectlCommandBuilder()
        let args = builder.podListTextArguments(context: "prod", namespace: "default")

        XCTAssertEqual(
            args,
            ["--context", "prod", "get", "pods", "-n", "default", "--chunk-size=200", "--request-timeout=20s", "-o", "custom-columns=NAME:.metadata.name,STATUS:.status.phase,RESTARTS:.status.containerStatuses[*].restartCount,CREATED:.metadata.creationTimestamp", "--no-headers"]
        )
    }

    func testPodTopArguments() {
        let builder = KubectlCommandBuilder()
        let args = builder.podTopArguments(context: "prod", namespace: "qa")

        XCTAssertEqual(args, ["--context", "prod", "top", "pods", "-n", "qa", "--no-headers"])
    }

    func testNodeTopArguments() {
        let builder = KubectlCommandBuilder()
        let args = builder.nodeTopArguments(context: "prod")

        XCTAssertEqual(args, ["--context", "prod", "top", "nodes", "--no-headers"])
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
        XCTAssertTrue(args.contains("--tail=5000"))
        XCTAssertTrue(args.contains("--request-timeout"))
    }

    func testLogsArgumentsRecentLinesUsesTailOnly() {
        let builder = KubectlCommandBuilder()
        let args = builder.podLogsArguments(
            context: "dev",
            namespace: "default",
            podName: "api-1",
            container: nil,
            filter: .tailLines(200),
            previous: false,
            follow: false
        )

        XCTAssertTrue(args.contains("--tail=200"))
        XCTAssertFalse(args.contains("--since"))
    }

    func testPodParsingLegacyText() {
        let parser = KubectlOutputParser()
        let raw = "api-6f99fdcc7f-7sm5x Running\nworker-86fdd44558-cv95w Pending\n"

        let pods = parser.parsePods(namespace: "default", from: raw)

        XCTAssertEqual(pods.count, 2)
        XCTAssertEqual(pods.first?.name, "api-6f99fdcc7f-7sm5x")
        XCTAssertEqual(pods.first?.status, "Running")
    }

    func testPodListJSONParsing() throws {
        let parser = KubectlOutputParser()
        let raw = """
        {"items":[
          {"metadata":{"name":"api-0","namespace":"default","creationTimestamp":"2024-06-01T12:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"restartCount":2},{"restartCount":1}]}},
          {"metadata":{"name":"worker","namespace":"default","creationTimestamp":"2024-06-01T11:00:00Z"},"status":{"phase":"Pending","containerStatuses":[]}}
        ]}
        """

        let pods = try parser.parsePodsListJSON(namespace: "default", from: raw)

        XCTAssertEqual(pods.count, 2)
        XCTAssertEqual(pods[0].name, "api-0")
        XCTAssertEqual(pods[0].status, "Running")
        XCTAssertEqual(pods[0].totalRestarts, 3)
        XCTAssertEqual(pods[1].status, "Pending")
    }

    func testPodListJSONParsingExtendedFields() throws {
        let parser = KubectlOutputParser()
        let raw = """
        {"items":[
          {"metadata":{"name":"web-1","namespace":"prod","creationTimestamp":"2024-06-01T12:00:00Z"},
           "spec":{"nodeName":"node-a","containers":[{"name":"nginx"},{"name":"sidecar"}]},
           "status":{"phase":"Running","podIP":"10.1.2.3","hostIP":"192.168.0.5","qosClass":"Burstable",
            "containerStatuses":[{"name":"nginx","ready":true,"restartCount":0},{"name":"sidecar","ready":true,"restartCount":1}]}}
        ]}
        """

        let pods = try parser.parsePodsListJSON(namespace: "prod", from: raw)

        XCTAssertEqual(pods.count, 1)
        let p = try XCTUnwrap(pods.first)
        XCTAssertEqual(p.podIP, "10.1.2.3")
        XCTAssertEqual(p.hostIP, "192.168.0.5")
        XCTAssertEqual(p.nodeName, "node-a")
        XCTAssertEqual(p.qosClass, "Burstable")
        XCTAssertEqual(p.containersReady, "2/2")
        XCTAssertEqual(p.totalRestarts, 1)
        XCTAssertEqual(p.containerNamesLine, "nginx, sidecar")
    }

    func testParseSinglePodJSON() throws {
        let parser = KubectlOutputParser()
        let raw = """
        {"metadata":{"name":"web-1","namespace":"prod","creationTimestamp":"2024-06-01T12:00:00Z"},
         "spec":{"nodeName":"node-a","containers":[{"name":"nginx"}]},
         "status":{"phase":"Running","podIP":"10.1.2.3","hostIP":"192.168.0.5","qosClass":"Guaranteed",
          "containerStatuses":[{"name":"nginx","ready":true,"restartCount":0}]}}
        """

        let p = try parser.parseSinglePodJSON(namespace: "prod", from: raw)

        XCTAssertEqual(p.name, "web-1")
        XCTAssertEqual(p.namespace, "prod")
        XCTAssertEqual(p.podIP, "10.1.2.3")
        XCTAssertEqual(p.hostIP, "192.168.0.5")
        XCTAssertEqual(p.nodeName, "node-a")
        XCTAssertEqual(p.qosClass, "Guaranteed")
        XCTAssertEqual(p.containersReady, "1/1")
    }

    func testResourceJSONArgumentsPod() {
        let builder = KubectlCommandBuilder()
        let args = builder.resourceJSONArguments(context: "ctx", namespace: "ns", kind: .pod, name: "p1")

        XCTAssertEqual(
            args,
            ["--context", "ctx", "get", "pod", "p1", "-n", "ns", "-o", "json"]
        )
    }

    func testParsePodTopByName() {
        let parser = KubectlOutputParser()
        let raw = """
        api-0   5m   120Mi
        api-1   10m   64Mi
        """

        let map = parser.parsePodTopByName(from: raw)

        XCTAssertEqual(map["api-0"]?.cpu, "5m")
        XCTAssertEqual(map["api-0"]?.memory, "120Mi")
        XCTAssertEqual(map["api-1"]?.cpu, "10m")
    }

    func testParsePodsTableFallback() {
        let parser = KubectlOutputParser()
        let raw = """
        api-0 Running 0,1 2024-06-01T12:00:00Z
        api-1 Pending <none> 2024-06-01T11:00:00Z
        """

        let pods = parser.parsePodsTable(namespace: "default", from: raw)

        XCTAssertEqual(pods.count, 2)
        XCTAssertEqual(pods[0].name, "api-0")
        XCTAssertEqual(pods[0].totalRestarts, 1)
        XCTAssertEqual(pods[1].totalRestarts, 0)
        XCTAssertNotEqual(pods[0].ageDescription, "—")
    }

    func testParseNodeTopUsagePercent() {
        let parser = KubectlOutputParser()
        let raw = """
        aks-node-1   232m   6%   1550Mi   48%
        aks-node-2   164m   4%   1682Mi   52%
        """

        let usage = parser.parseNodeTopUsagePercent(from: raw)

        XCTAssertEqual(usage.cpuPercent, 5)
        XCTAssertEqual(usage.memoryPercent, 50)
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

    func testPodDescribeArguments() {
        let builder = KubectlCommandBuilder()
        let args = builder.describeResourceArguments(context: "dev", namespace: "default", kind: .pod, name: "api-1")

        XCTAssertEqual(args, ["--context", "dev", "describe", "pod", "api-1", "-n", "default"])
    }

    func testClusterRoleDescribeOmitsNamespace() {
        let builder = KubectlCommandBuilder()
        let args = builder.describeResourceArguments(
            context: "dev",
            namespace: "ignored",
            kind: .clusterRole,
            name: "view"
        )

        XCTAssertEqual(args, ["--context", "dev", "describe", "clusterrole", "view"])
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

    func testParseDeploymentsTableFallback() {
        let parser = KubectlOutputParser()
        let raw = """
        api 2 3
        worker <none> 1
        """

        let deployments = parser.parseDeploymentsTable(namespace: "default", from: raw)

        XCTAssertEqual(deployments.count, 2)
        XCTAssertEqual(deployments[0].name, "api")
        XCTAssertEqual(deployments[0].replicaText, "2/3")
        XCTAssertEqual(deployments[1].replicaText, "0/1")
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
