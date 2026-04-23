import XCTest
@testable import RuneCore
@testable import RuneKube

final class KubectlTests: XCTestCase {
    func testListContextsPrefersKubectlBeforeRESTNormalization() async throws {
        let runner = ScriptedCommandRunner(script: [
            CommandKey(
                executable: "/usr/bin/env",
                arguments: ["kubectl", "config", "get-contexts", "-o", "name"]
            ): .success(CommandResult(
                stdout: "orbit-prod\norbit-qa\n",
                stderr: "",
                exitCode: 0
            ))
        ])
        let client = makeClient(runner: runner)
        let sources = try makeKubeConfigSources()

        let contexts = try await client.listContexts(from: sources)
        let didRunContextList = await runner.didRun(arguments: ["kubectl", "config", "get-contexts", "-o", "name"])
        let didRunNormalizedConfig = await runner.didRun(arguments: ["kubectl", "config", "view", "--raw", "--flatten", "-o", "json"])

        XCTAssertEqual(contexts.map(\.name), ["orbit-prod", "orbit-qa"])
        XCTAssertTrue(didRunContextList)
        XCTAssertFalse(didRunNormalizedConfig)
    }

    func testListNamespacesPrefersKubectlBeforeRESTNormalization() async throws {
        let runner = ScriptedCommandRunner(script: [
            CommandKey(
                executable: "/usr/bin/env",
                arguments: ["kubectl", "--context", "orbit-prod", "--request-timeout=90s", "get", "namespaces", "-o", "custom-columns=NAME:.metadata.name", "--no-headers"]
            ): .success(CommandResult(
                stdout: "alpha-zone\nkube-system\n",
                stderr: "",
                exitCode: 0
            ))
        ])
        let client = makeClient(runner: runner)
        let sources = try makeKubeConfigSources()

        let namespaces = try await client.listNamespaces(from: sources, context: KubeContext(name: "orbit-prod"))
        let didRunNamespaceList = await runner.didRun(arguments: ["kubectl", "--context", "orbit-prod", "--request-timeout=90s", "get", "namespaces", "-o", "custom-columns=NAME:.metadata.name", "--no-headers"])
        let didRunNormalizedConfig = await runner.didRun(arguments: ["kubectl", "config", "view", "--raw", "--flatten", "-o", "json"])

        XCTAssertEqual(namespaces, ["alpha-zone", "kube-system"])
        XCTAssertTrue(didRunNamespaceList)
        XCTAssertFalse(didRunNormalizedConfig)
    }

    func testContextNamespacePrefersKubectlBeforeRESTNormalization() async throws {
        let runner = ScriptedCommandRunner(script: [
            CommandKey(
                executable: "/usr/bin/env",
                arguments: ["kubectl", "config", "view", "--minify", "--context", "orbit-prod", "-o", "jsonpath={..namespace}"]
            ): .success(CommandResult(
                stdout: "alpha-zone",
                stderr: "",
                exitCode: 0
            ))
        ])
        let client = makeClient(runner: runner)
        let sources = try makeKubeConfigSources()

        let namespace = try await client.contextNamespace(from: sources, context: KubeContext(name: "orbit-prod"))
        let didRunContextNamespace = await runner.didRun(arguments: ["kubectl", "config", "view", "--minify", "--context", "orbit-prod", "-o", "jsonpath={..namespace}"])
        let didRunNormalizedConfig = await runner.didRun(arguments: ["kubectl", "config", "view", "--raw", "--flatten", "-o", "json"])

        XCTAssertEqual(namespace, "alpha-zone")
        XCTAssertTrue(didRunContextNamespace)
        XCTAssertFalse(didRunNormalizedConfig)
    }

    func testRESTNormalizationFailureTemporarilyDisablesRepeatedAttempts() async throws {
        let runner = ScriptedCommandRunner(script: [
            CommandKey(
                executable: "/usr/bin/env",
                arguments: ["kubectl", "config", "view", "--raw", "--flatten", "-o", "json"]
            ): .failure(RuneError.commandFailed(command: "kubectl config view", message: "Timed out after 2 seconds"))
        ])
        let client = KubernetesRESTClient(runner: runner, kubectlPath: "/usr/bin/env")
        let environment = ["KUBECONFIG": "/tmp/rune-tests-config"]

        do {
            _ = try await client.listContexts(environment: environment)
            XCTFail("Expected first normalization to fail")
        } catch {
            XCTAssertTrue(String(describing: error).contains("Timed out"))
        }

        do {
            _ = try await client.listContexts(environment: environment)
            XCTFail("Expected second normalization to be blocked")
        } catch {
            XCTAssertTrue(String(describing: error).contains("temporarily disabled"))
        }

        let normalizedConfigRunCount = await runner.runCount(arguments: ["kubectl", "config", "view", "--raw", "--flatten", "-o", "json"])
        XCTAssertEqual(normalizedConfigRunCount, 1)
    }

    func testContextArguments() {
        let builder = KubectlCommandBuilder()
        XCTAssertEqual(builder.contextListArguments(), ["config", "get-contexts", "-o", "name"])
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

    func testCollectionPageInfoReadsItemsContinueAndRemaining() {
        let json = """
        {"metadata":{"continue":"next-token","remainingItemCount":42},"items":[{"metadata":{"name":"a"}},{"metadata":{"name":"b"}}]}
        """
        let page = KubectlListJSON.collectionPageInfo(from: json)
        XCTAssertEqual(page?.itemsCount, 2)
        XCTAssertEqual(page?.continueToken, "next-token")
        XCTAssertEqual(page?.remainingItemCount, 42)
    }

    func testCollectionPageInfoNormalizesBlankContinue() {
        let json = """
        {"metadata":{"continue":"   "},"items":[{"metadata":{"name":"a"}}]}
        """
        let page = KubectlListJSON.collectionPageInfo(from: json)
        XCTAssertEqual(page?.itemsCount, 1)
        XCTAssertNil(page?.continueToken)
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
        XCTAssertTrue(args.contains("--tail=2000"))
        XCTAssertTrue(args.contains("--request-timeout"))
    }

    func testUnifiedLogsArgumentsWithRelativeFilterUseSmallerTailCap() {
        let builder = KubectlCommandBuilder()

        let args = builder.podLogsArguments(
            context: "dev",
            namespace: "default",
            podName: "api-1",
            container: nil,
            filter: .lastMinutes(15),
            previous: false,
            follow: false,
            profile: .unifiedPerPod
        )

        XCTAssertTrue(args.contains("--since"))
        XCTAssertTrue(args.contains("--tail=400"))
    }

    func testAgentPodLogsWithRelativeFilterUseTrimmedTailCap() async throws {
        let agentPath = try makeExecutableFile()
        let runner = ScriptedCommandRunner(script: [
            CommandKey(
                executable: agentPath,
                arguments: ["logs", "--context", "dev", "--namespace", "default", "--pod", "api-1", "--since", "15m", "--tail", "2000"]
            ): .success(CommandResult(stdout: "line\n", stderr: "", exitCode: 0))
        ])

        let stdout = try await RuneK8sAgentOperationsClient.podLogs(
            executablePath: agentPath,
            runner: runner,
            environment: ["KUBECONFIG": "/tmp/rune-tests-config"],
            contextName: "dev",
            namespace: "default",
            podName: "api-1",
            filter: .lastMinutes(15),
            previous: false,
            timeout: 5
        )

        XCTAssertEqual(stdout, "line\n")
    }

    func testAgentPodLogsAllLogsOmitsTailAndSince() async throws {
        let agentPath = try makeExecutableFile()
        let runner = ScriptedCommandRunner(script: [
            CommandKey(
                executable: agentPath,
                arguments: ["logs", "--context", "dev", "--namespace", "default", "--pod", "api-1"]
            ): .success(CommandResult(stdout: "all logs\n", stderr: "", exitCode: 0))
        ])

        let stdout = try await RuneK8sAgentOperationsClient.podLogs(
            executablePath: agentPath,
            runner: runner,
            environment: ["KUBECONFIG": "/tmp/rune-tests-config"],
            contextName: "dev",
            namespace: "default",
            podName: "api-1",
            filter: .all,
            previous: false,
            timeout: 5
        )

        XCTAssertEqual(stdout, "all logs\n")
    }

    func testUnifiedServiceLogsPrefersMergedAgentLogFetch() async throws {
        let agentPath = try makeExecutableFile()
        let runner = ScriptedCommandRunner(script: [
            CommandKey(
                executable: agentPath,
                arguments: [
                    "selector", "service",
                    "--context", "prod",
                    "--namespace", "default",
                    "--name", "api"
                ]
            ): .success(CommandResult(stdout: #"{"app":"api"}"#, stderr: "", exitCode: 0)),
            CommandKey(
                executable: agentPath,
                arguments: [
                    "logs", "selector",
                    "--context", "prod",
                    "--namespace", "default",
                    "--label-selector", "app=api",
                    "--tail", "400",
                    "--max-pods", "8",
                    "--concurrency", "3",
                    "--since", "15m"
                ]
            ): .success(CommandResult(stdout: #"{"podNames":["api-0","api-1"],"mergedText":"[api-0] first\n[api-1] second"}"#, stderr: "", exitCode: 0))
        ])
        let client = KubectlClient(
            runner: runner,
            longRunningRunner: NoopLongRunningCommandRunner(),
            kubectlPath: "/usr/bin/env",
            k8sAgentPath: agentPath
        )
        let sources = try makeKubeConfigSources()

        let logs = try await client.unifiedLogsForService(
            from: sources,
            context: KubeContext(name: "prod"),
            namespace: "default",
            service: ServiceSummary(name: "api", namespace: "default", type: "ClusterIP", clusterIP: "10.0.0.10"),
            filter: .lastMinutes(15),
            previous: false
        )

        XCTAssertEqual(logs.podNames, ["api-0", "api-1"])
        XCTAssertEqual(logs.mergedText, "[api-0] first\n[api-1] second")
        let ranSelectorPods = await runner.didRun(arguments: ["selector", "pods", "--context", "prod", "--namespace", "default", "--label-selector", "app=api"])
        XCTAssertFalse(ranSelectorPods)
    }

    func testPodLogsFallsBackToKubectlWhenAgentReturnsEmptyOutput() async throws {
        let agentPath = try makeExecutableFile()
        let builder = KubectlCommandBuilder()
        let runner = ScriptedCommandRunner(script: [
            CommandKey(
                executable: agentPath,
                arguments: ["logs", "--context", "prod", "--namespace", "default", "--pod", "api-0", "--tail", "200"]
            ): .success(CommandResult(stdout: "", stderr: "", exitCode: 0)),
            CommandKey(
                executable: "/usr/bin/env",
                arguments: ["kubectl"] + builder.podLogsArguments(
                    context: "prod",
                    namespace: "default",
                    podName: "api-0",
                    container: nil,
                    filter: .tailLines(200),
                    previous: false,
                    follow: false
                )
            ): .success(CommandResult(stdout: "hello from kubectl\n", stderr: "", exitCode: 0))
        ])
        let client = KubectlClient(
            runner: runner,
            longRunningRunner: NoopLongRunningCommandRunner(),
            kubectlPath: "/usr/bin/env",
            k8sAgentPath: agentPath
        )
        let sources = try makeKubeConfigSources()

        let logs = try await client.podLogs(
            from: sources,
            context: KubeContext(name: "prod"),
            namespace: "default",
            podName: "api-0",
            filter: .tailLines(200),
            previous: false
        )

        XCTAssertEqual(logs, "hello from kubectl\n")
    }

    func testUnifiedServiceLogsFallsBackWhenMergedAgentOutputIsEmpty() async throws {
        let agentPath = try makeExecutableFile()
        let builder = KubectlCommandBuilder()
        let podsJSON = """
        {"items":[
          {"metadata":{"name":"api-0","namespace":"default","creationTimestamp":"2024-06-01T12:00:00Z"},"status":{"phase":"Running","containerStatuses":[]}},
          {"metadata":{"name":"api-1","namespace":"default","creationTimestamp":"2024-06-01T12:01:00Z"},"status":{"phase":"Running","containerStatuses":[]}}
        ]}
        """
        let runner = ScriptedCommandRunner(script: [
            CommandKey(
                executable: agentPath,
                arguments: [
                    "selector", "service",
                    "--context", "prod",
                    "--namespace", "default",
                    "--name", "api"
                ]
            ): .success(CommandResult(stdout: #"{"app":"api"}"#, stderr: "", exitCode: 0)),
            CommandKey(
                executable: agentPath,
                arguments: [
                    "logs", "selector",
                    "--context", "prod",
                    "--namespace", "default",
                    "--label-selector", "app=api",
                    "--tail", "200",
                    "--max-pods", "8",
                    "--concurrency", "3"
                ]
            ): .success(CommandResult(stdout: #"{"podNames":["api-0","api-1"],"mergedText":""}"#, stderr: "", exitCode: 0)),
            CommandKey(
                executable: agentPath,
                arguments: [
                    "selector", "pods",
                    "--context", "prod",
                    "--namespace", "default",
                    "--label-selector", "app=api"
                ]
            ): .failure(RuneError.commandFailed(command: "rune-k8s-agent selector pods", message: "simulated fallback")),
            CommandKey(
                executable: "/usr/bin/env",
                arguments: ["kubectl"] + builder.podsByLabelSelectorArguments(context: "prod", namespace: "default", selector: "app=api")
            ): .success(CommandResult(stdout: podsJSON, stderr: "", exitCode: 0)),
            CommandKey(
                executable: "/usr/bin/env",
                arguments: ["kubectl"] + builder.podLogsArguments(
                    context: "prod",
                    namespace: "default",
                    podName: "api-0",
                    container: nil,
                    filter: .tailLines(200),
                    previous: false,
                    follow: false,
                    profile: .unifiedPerPod
                )
            ): .success(CommandResult(stdout: "2024-06-01T12:00:00Z first\n", stderr: "", exitCode: 0)),
            CommandKey(
                executable: "/usr/bin/env",
                arguments: ["kubectl"] + builder.podLogsArguments(
                    context: "prod",
                    namespace: "default",
                    podName: "api-1",
                    container: nil,
                    filter: .tailLines(200),
                    previous: false,
                    follow: false,
                    profile: .unifiedPerPod
                )
            ): .success(CommandResult(stdout: "2024-06-01T12:00:01Z second\n", stderr: "", exitCode: 0))
        ])
        let client = KubectlClient(
            runner: runner,
            longRunningRunner: NoopLongRunningCommandRunner(),
            kubectlPath: "/usr/bin/env",
            k8sAgentPath: agentPath
        )
        let sources = try makeKubeConfigSources()

        let logs = try await client.unifiedLogsForService(
            from: sources,
            context: KubeContext(name: "prod"),
            namespace: "default",
            service: ServiceSummary(name: "api", namespace: "default", type: "ClusterIP", clusterIP: "10.0.0.10"),
            filter: .tailLines(200),
            previous: false
        )

        XCTAssertTrue(logs.mergedText.contains("[api-0] 2024-06-01T12:00:00Z first"))
        XCTAssertTrue(logs.mergedText.contains("[api-1] 2024-06-01T12:00:01Z second"))
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

    func testLogsArgumentsAllLogsMatchesPlainKubectlLogsShape() {
        let builder = KubectlCommandBuilder()
        let args = builder.podLogsArguments(
            context: "dev",
            namespace: "default",
            podName: "api-1",
            container: nil,
            filter: .all,
            previous: false,
            follow: false
        )

        XCTAssertFalse(args.contains { $0.hasPrefix("--tail") })
        XCTAssertFalse(args.contains("--since"))
        XCTAssertFalse(args.contains("--since-time"))
        XCTAssertTrue(args.contains("--timestamps"))
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

    func testValidateFileArguments() {
        let builder = KubectlCommandBuilder()
        let args = builder.validateFileArguments(
            context: "dev",
            namespace: "default",
            filePath: "/tmp/resource.yaml"
        )

        XCTAssertEqual(
            args,
            ["--context", "dev", "apply", "--dry-run=server", "--validate=true", "-n", "default", "-f", "/tmp/resource.yaml"]
        )
    }

    func testParseValidationIssuesMapsYAMLLineErrorsToSyntaxIssues() {
        let output = """
        error parsing /tmp/rune.yaml: error converting YAML to JSON: yaml: line 3: did not find expected key
        """
        let yaml = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: api
        """

        let issues = KubectlClient.parseValidationIssues(from: output, yaml: yaml)

        XCTAssertEqual(issues.count, 1)
        XCTAssertEqual(issues.first?.source, .syntax)
        XCTAssertEqual(issues.first?.line, 3)
        XCTAssertEqual(issues.first?.message, "did not find expected key")
        XCTAssertEqual(issues.first?.range.map { (yaml as NSString).substring(with: $0.nsRange) }, "metadata:\n")
    }

    func testParseValidationIssuesMapsKubernetesSchemaFailures() {
        let output = """
        error: error validating "/tmp/rune.yaml": error validating data: [ValidationError(Deployment.spec.template.spec.containers[0].image): required value]; if you choose to ignore these errors, turn validation off with --validate=false
        """

        let issues = KubectlClient.parseValidationIssues(from: output, yaml: "kind: Deployment\n")

        XCTAssertEqual(issues.count, 1)
        XCTAssertEqual(issues.first?.source, .kubernetes)
        XCTAssertEqual(
            issues.first?.message,
            "Deployment.spec.template.spec.containers[0].image: required value"
        )
    }

    func testParseValidationIssuesMapsTransportFailuresToWarnings() {
        let output = "Unable to connect to the server: dial tcp 127.0.0.1:6443: connect: connection refused"

        let issues = KubectlClient.parseValidationIssues(from: output, yaml: "kind: Pod\n")

        XCTAssertEqual(issues.count, 1)
        XCTAssertEqual(issues.first?.source, .transport)
        XCTAssertEqual(issues.first?.severity, .warning)
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

    private func makeClient(runner: ScriptedCommandRunner) -> KubectlClient {
        KubectlClient(
            runner: runner,
            longRunningRunner: NoopLongRunningCommandRunner(),
            kubectlPath: "/usr/bin/env"
        )
    }

    private func makeKubeConfigSources(file: StaticString = #filePath, line: UInt = #line) throws -> [KubeConfigSource] {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
            .appendingPathExtension("yaml")
        let created = FileManager.default.createFile(atPath: url.path, contents: Data(), attributes: nil)
        XCTAssertTrue(created, file: file, line: line)
        addTeardownBlock {
            try? FileManager.default.removeItem(at: url)
        }
        return [KubeConfigSource(url: url)]
    }

    private func makeExecutableFile(file: StaticString = #filePath, line: UInt = #line) throws -> String {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let created = FileManager.default.createFile(atPath: url.path, contents: Data("#!/bin/sh\n".utf8), attributes: nil)
        XCTAssertTrue(created, file: file, line: line)
        XCTAssertNoThrow(try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: url.path), file: file, line: line)
        addTeardownBlock {
            try? FileManager.default.removeItem(at: url)
        }
        return url.path
    }
}

private struct CommandKey: Hashable, Sendable {
    let executable: String
    let arguments: [String]
}

private enum ScriptedCommandOutcome {
    case success(CommandResult)
    case failure(Error)
}

private actor ScriptedCommandRunner: CommandRunning {
    private let script: [CommandKey: ScriptedCommandOutcome]
    private var calls: [CommandKey] = []

    init(script: [CommandKey: ScriptedCommandOutcome]) {
        self.script = script
    }

    func run(
        executable: String,
        arguments: [String],
        environment: [String : String],
        timeout: TimeInterval?
    ) async throws -> CommandResult {
        let key = CommandKey(executable: executable, arguments: arguments)
        calls.append(key)

        guard let outcome = script[key] else {
            throw RuneError.commandFailed(command: "missing scripted command", message: arguments.joined(separator: " "))
        }

        switch outcome {
        case let .success(result):
            return result
        case let .failure(error):
            throw error
        }
    }

    func didRun(arguments: [String]) -> Bool {
        calls.contains(where: { $0.arguments == arguments })
    }

    func runCount(arguments: [String]) -> Int {
        calls.filter { $0.arguments == arguments }.count
    }
}

private final class NoopLongRunningCommandRunner: LongRunningCommandRunning, @unchecked Sendable {
    func start(
        executable: String,
        arguments: [String],
        environment: [String : String],
        onStdout: @escaping @Sendable (String) -> Void,
        onStderr: @escaping @Sendable (String) -> Void,
        onTermination: @escaping @Sendable (Int32) -> Void
    ) throws -> any RunningCommandControlling {
        DummyRunningCommandController()
    }
}

private final class DummyRunningCommandController: RunningCommandControlling, @unchecked Sendable {
    let id = UUID()

    func terminate() {}
}
