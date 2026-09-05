import Foundation
import XCTest
@testable import RuneCore
@testable import RuneFakeK8sSupport
@testable import RuneKube

final class RuneFakeK8sRESTServerTests: XCTestCase {
    func testResourceWatchStreamsEventsAndUsesExactNamespaceScope() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }
        let client = KubernetesClient(commandTimeout: 2)

        let stream = try await client.watchResourceChanges(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: "alpha-zone",
            kind: .pod,
            resourceVersion: "synthetic-1",
            timeoutSeconds: 30
        )
        var events: [KubernetesResourceWatchEvent] = []
        for try await event in stream {
            events.append(event)
        }

        XCTAssertEqual(events.map(\.type), [.modified, .bookmark])
        XCTAssertEqual(events.map(\.resourceVersion), ["synthetic-2", "synthetic-3"])
        XCTAssertTrue(server.requestLines().contains { line in
            line.contains("GET /api/v1/namespaces/alpha-zone/pods?")
                && line.contains("watch=1")
                && line.contains("allowWatchBookmarks=true")
                && line.contains("resourceVersion=synthetic-1")
                && line.contains("resourceVersionMatch=NotOlderThan")
        })
    }

    func testRESTResponseDecodingPreservesValidLogsAroundMalformedUTF8() {
        let data = Data([0x61, 0x6C, 0x70, 0x68, 0x61, 0x0A, 0xFF, 0x0A, 0x6F, 0x6D, 0x65, 0x67, 0x61])

        XCTAssertEqual(
            KubernetesRESTClient.decodeResponseBody(data),
            "alpha\n\u{FFFD}\nomega"
        )
    }

    func testSelfSubjectAccessReviewRequestBodyIncludesAPIGroupWhenPresent() throws {
        let body = try KubernetesRESTClient.selfSubjectAccessReviewRequestBody(
            namespace: "synthetic",
            verb: "list",
            resource: "deployments",
            apiGroup: "apps",
            subresource: nil
        )
        let attributes = try resourceAttributes(from: body)

        XCTAssertEqual(attributes["namespace"] as? String, "synthetic")
        XCTAssertEqual(attributes["verb"] as? String, "list")
        XCTAssertEqual(attributes["resource"] as? String, "deployments")
        XCTAssertEqual(attributes["group"] as? String, "apps")
        XCTAssertNil(attributes["subresource"])
    }

    func testSelfSubjectAccessReviewRequestBodyOmitsBlankAPIGroup() throws {
        let body = try KubernetesRESTClient.selfSubjectAccessReviewRequestBody(
            namespace: "synthetic",
            verb: "list",
            resource: "pods",
            apiGroup: "   ",
            subresource: "log"
        )
        let attributes = try resourceAttributes(from: body)

        XCTAssertEqual(attributes["resource"] as? String, "pods")
        XCTAssertEqual(attributes["subresource"] as? String, "log")
        XCTAssertNil(attributes["group"])
    }

    func testSelfSubjectAccessReviewAllowedParserReadsStatusAllowed() throws {
        let allowed = try KubernetesRESTClient.selfSubjectAccessReviewAllowed(
            from: #"{"apiVersion":"authorization.k8s.io/v1","status":{"allowed":true}}"#
        )
        let denied = try KubernetesRESTClient.selfSubjectAccessReviewAllowed(
            from: #"{"apiVersion":"authorization.k8s.io/v1","status":{"allowed":false}}"#
        )

        XCTAssertTrue(allowed)
        XCTAssertFalse(denied)
    }

    func testRESTFakeClusterAppliesSelfSubjectAccessReviewDenials() async throws {
        let fixture = RuneFakeK8sFixture(selfSubjectAccessReviewDenials: [
            RuneFakeK8sRBACRule(namespace: "alpha-zone", verb: "get", resource: "pods", subresource: "log")
        ])
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)

        let canListPods = try await client.canI(
            from: sources,
            context: context,
            namespace: "alpha-zone",
            verb: "list",
            resource: "pods"
        )
        let canReadPodLogs = try await client.canI(
            from: sources,
            context: context,
            namespace: "alpha-zone",
            verb: "get",
            resource: "pods",
            subresource: "log"
        )

        XCTAssertTrue(canListPods)
        XCTAssertFalse(canReadPodLogs)
    }

    private func resourceAttributes(from body: String) throws -> [String: Any] {
        let object = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(body.utf8)) as? [String: Any])
        let spec = try XCTUnwrap(object["spec"] as? [String: Any])
        return try XCTUnwrap(spec["resourceAttributes"] as? [String: Any])
    }

    func testNativeClientReadsScriptlessRESTFakeCluster() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)

        let contexts = try await client.listContexts(from: sources)
        XCTAssertEqual(contexts, [context])
        let defaultNamespace = try await client.contextNamespace(from: sources, context: context)
        XCTAssertEqual(defaultNamespace, "alpha-zone")
        let namespaces = try await client.listNamespaces(from: sources, context: context)
        XCTAssertEqual(namespaces, ["alpha-zone", "bravo-zone"])

        let pods = try await client.listPods(from: sources, context: context, namespace: "alpha-zone")
        XCTAssertEqual(pods.map(\.name), ["ember-gate-75c9f746b8-kq2wm", "orbit-lens-6f58d7d89b-hx9q2"])
        XCTAssertEqual(pods.first(where: { $0.name.hasPrefix("orbit-lens") })?.cpuUsage, "42m")
        XCTAssertEqual(pods.first(where: { $0.name.hasPrefix("orbit-lens") })?.memoryUsage, "96Mi")
        XCTAssertTrue(pods.contains { $0.containerNamesLine != nil || !$0.labels.isEmpty || $0.nodeName != nil })

        let deployments = try await client.listDeployments(from: sources, context: context, namespace: "alpha-zone")
        XCTAssertEqual(deployments.map(\.name), ["ember-gate", "orbit-lens"])
        XCTAssertEqual(deployments.first(where: { $0.name == "orbit-lens" })?.readyReplicas, 2)

        let services = try await client.listServices(from: sources, context: context, namespace: "alpha-zone")
        XCTAssertEqual(services.map(\.name), ["ember-gate", "orbit-lens"])
        XCTAssertEqual(services.first(where: { $0.name == "orbit-lens" })?.selector, ["app": "orbit-lens"])

        let endpoints = try await client.listEndpoints(from: sources, context: context, namespace: "alpha-zone")
        XCTAssertEqual(endpoints.map(\.name), ["ember-gate", "orbit-lens"])
        XCTAssertEqual(endpoints.first(where: { $0.name == "orbit-lens" })?.primaryText, "1/1 ready")

        let serviceAccounts = try await client.listServiceAccounts(from: sources, context: context, namespace: "alpha-zone")
        XCTAssertTrue(serviceAccounts.map(\.name).contains("default"))
        XCTAssertTrue(serviceAccounts.map(\.name).contains("orbit-lens-runner"))

        let count = try await client.countNamespacedResources(
            from: sources,
            context: context,
            namespace: "alpha-zone",
            resource: "pods"
        )
        XCTAssertEqual(count, 2)

        let logs = try await client.podLogs(
            from: sources,
            context: context,
            namespace: "alpha-zone",
            podName: "orbit-lens-6f58d7d89b-hx9q2",
            filter: .tailLines(50),
            previous: false
        )
        XCTAssertTrue(logs.contains("synthetic REST fake log"))
    }

    func testPodLogWindowSettingsReachRESTAPIWithoutImplicitRecentTail() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        let absoluteDate = Date(timeIntervalSince1970: 0)
        let scenarios: [(name: String, filter: LogTimeFilter, expectedWindowQuery: [String: String])] = [
            ("all logs", .all, [:]),
            (
                "custom lines",
                RuneCustomLogPresetConfig(
                    mode: .lines,
                    lines: 731,
                    timeValue: 1,
                    timeUnit: .minutes
                ).filter,
                ["tailLines": "731"]
            ),
            (
                "custom minutes",
                RuneCustomLogPresetConfig(
                    mode: .time,
                    lines: 1,
                    timeValue: 9,
                    timeUnit: .minutes
                ).filter,
                ["sinceSeconds": "540"]
            ),
            (
                "custom hours",
                RuneCustomLogPresetConfig(
                    mode: .time,
                    lines: 1,
                    timeValue: 6,
                    timeUnit: .hours
                ).filter,
                ["sinceSeconds": "21600"]
            ),
            (
                "custom days",
                RuneCustomLogPresetConfig(
                    mode: .time,
                    lines: 1,
                    timeValue: 2,
                    timeUnit: .days
                ).filter,
                ["sinceSeconds": "172800"]
            ),
            (
                "absolute time",
                .since(absoluteDate),
                ["sinceTime": ISO8601DateFormatter().string(from: absoluteDate)]
            ),
            (
                "overflow-safe time",
                .lastDays(Int.max),
                ["sinceSeconds": String(Int.max)]
            )
        ]

        for scenario in scenarios {
            server.resetRequestLines()
            _ = try await client.podLogs(
                from: sources,
                context: context,
                namespace: "alpha-zone",
                podName: "orbit-lens-6f58d7d89b-hx9q2",
                container: "lens",
                filter: scenario.filter,
                previous: false
            )

            let requestLine = try XCTUnwrap(
                server.requestLines().last { $0.contains("/pods/orbit-lens-6f58d7d89b-hx9q2/log") },
                scenario.name
            )
            let query = try requestQueryItems(requestLine)
            XCTAssertEqual(query["container"], "lens", scenario.name)
            XCTAssertEqual(
                query.filter { ["tailLines", "sinceSeconds", "sinceTime"].contains($0.key) },
                scenario.expectedWindowQuery,
                scenario.name
            )
        }
    }

    func testResourceYAMLRequestMatrixRoutesScopesAndPreservesLargeUnicodePayloads() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let longValue = "synthetic-start-" + String(repeating: "λ", count: 8_192) + "-終"
        let configMapYAML = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: orbit-lens-settings
          namespace: alpha-zone
        data:
          multiline: |-
            synthetic first line
            synthetic café λ
          long-value: "\(longValue)"
        """
        let scenarios = [
            ResourceYAMLRequestScenario(
                name: "namespaced ConfigMap validation with multiline, Unicode, and a long line",
                operation: .validate,
                yaml: configMapYAML,
                expectedTarget: "/api/v1/namespaces/alpha-zone/configmaps/orbit-lens-settings?fieldManager=rune&force=true&dryRun=All"
            ),
            ResourceYAMLRequestScenario(
                name: "namespaced ConfigMap apply with multiline, Unicode, and a long line",
                operation: .apply,
                yaml: configMapYAML,
                expectedTarget: "/api/v1/namespaces/alpha-zone/configmaps/orbit-lens-settings?fieldManager=rune&force=true"
            ),
            ResourceYAMLRequestScenario(
                name: "namespaced Deployment apply",
                operation: .apply,
                yaml: """
                apiVersion: apps/v1
                kind: Deployment
                metadata:
                  name: orbit-lens
                  namespace: alpha-zone
                spec:
                  replicas: 3
                """,
                expectedTarget: "/apis/apps/v1/namespaces/alpha-zone/deployments/orbit-lens?fieldManager=rune&force=true"
            ),
            ResourceYAMLRequestScenario(
                name: "namespaced Pod owner metadata never overrides the apply target",
                operation: .apply,
                yaml: """
                apiVersion: v1
                kind: Pod
                metadata:
                  name: synthetic-pod
                  namespace: alpha-zone
                  labels:
                    name: nested-label-value
                  ownerReferences:
                    - apiVersion: apps/v1
                      kind: ReplicaSet
                      name: synthetic-owner
                spec:
                  containers:
                    - name: synthetic-container
                      image: example.invalid/synthetic:1
                """,
                expectedTarget: "/api/v1/namespaces/alpha-zone/pods/synthetic-pod?fieldManager=rune&force=true"
            ),
            ResourceYAMLRequestScenario(
                name: "cluster-scoped Node apply ignores manifest namespace",
                operation: .apply,
                yaml: """
                apiVersion: v1
                kind: Node
                metadata:
                  name: orbit-node-a
                  namespace: synthetic-ignored-zone
                spec:
                  unschedulable: true
                """,
                expectedTarget: "/api/v1/nodes/orbit-node-a?fieldManager=rune&force=true"
            ),
            ResourceYAMLRequestScenario(
                name: "namespaced Secret validation uses the default namespace and remains dry-run",
                operation: .validate,
                yaml: """
                apiVersion: v1
                kind: Secret
                metadata:
                  name: synthetic-settings
                type: Opaque
                stringData:
                  greeting: synthetic-only
                """,
                expectedTarget: "/api/v1/namespaces/alpha-zone/secrets/synthetic-settings?fieldManager=rune&force=true&dryRun=All"
            ),
            ResourceYAMLRequestScenario(
                name: "namespaced batch Job apply",
                operation: .apply,
                yaml: """
                apiVersion: batch/v1
                kind: Job
                metadata:
                  name: synthetic-batch-run
                  namespace: alpha-zone
                spec:
                  template:
                    spec:
                      restartPolicy: Never
                      containers:
                        - name: task
                          image: example.invalid/synthetic:1
                """,
                expectedTarget: "/apis/batch/v1/namespaces/alpha-zone/jobs/synthetic-batch-run?fieldManager=rune&force=true"
            ),
            ResourceYAMLRequestScenario(
                name: "namespaced networking Ingress apply",
                operation: .apply,
                yaml: """
                apiVersion: networking.k8s.io/v1
                kind: Ingress
                metadata:
                  name: synthetic-edge
                  namespace: alpha-zone
                spec:
                  rules: []
                """,
                expectedTarget: "/apis/networking.k8s.io/v1/namespaces/alpha-zone/ingresses/synthetic-edge?fieldManager=rune&force=true"
            ),
            ResourceYAMLRequestScenario(
                name: "namespaced autoscaling HPA apply",
                operation: .apply,
                yaml: """
                apiVersion: autoscaling/v2
                kind: HorizontalPodAutoscaler
                metadata:
                  name: synthetic-scaler
                  namespace: alpha-zone
                spec:
                  minReplicas: 1
                  maxReplicas: 3
                  scaleTargetRef:
                    apiVersion: apps/v1
                    kind: Deployment
                    name: orbit-lens
                """,
                expectedTarget: "/apis/autoscaling/v2/namespaces/alpha-zone/horizontalpodautoscalers/synthetic-scaler?fieldManager=rune&force=true"
            ),
            ResourceYAMLRequestScenario(
                name: "namespaced RBAC Role apply",
                operation: .apply,
                yaml: """
                apiVersion: rbac.authorization.k8s.io/v1
                kind: Role
                metadata:
                  name: synthetic-reader
                  namespace: alpha-zone
                rules:
                  - apiGroups: [""]
                    resources: ["configmaps"]
                    verbs: ["get"]
                """,
                expectedTarget: "/apis/rbac.authorization.k8s.io/v1/namespaces/alpha-zone/roles/synthetic-reader?fieldManager=rune&force=true"
            ),
            ResourceYAMLRequestScenario(
                name: "cluster-scoped RBAC ClusterRole apply",
                operation: .apply,
                yaml: """
                apiVersion: rbac.authorization.k8s.io/v1
                kind: ClusterRole
                metadata:
                  name: synthetic-reader-global
                rules:
                  - apiGroups: [""]
                    resources: ["namespaces"]
                    verbs: ["get"]
                """,
                expectedTarget: "/apis/rbac.authorization.k8s.io/v1/clusterroles/synthetic-reader-global?fieldManager=rune&force=true"
            ),
            ResourceYAMLRequestScenario(
                name: "namespaced storage PVC apply",
                operation: .apply,
                yaml: """
                apiVersion: v1
                kind: PersistentVolumeClaim
                metadata:
                  name: synthetic-cache
                  namespace: alpha-zone
                spec:
                  accessModes: ["ReadWriteOnce"]
                  resources:
                    requests:
                      storage: 1Mi
                """,
                expectedTarget: "/api/v1/namespaces/alpha-zone/persistentvolumeclaims/synthetic-cache?fieldManager=rune&force=true"
            ),
            ResourceYAMLRequestScenario(
                name: "cluster-scoped storage StorageClass apply",
                operation: .apply,
                yaml: """
                apiVersion: storage.k8s.io/v1
                kind: StorageClass
                metadata:
                  name: synthetic-storage
                provisioner: example.invalid/synthetic
                volumeBindingMode: WaitForFirstConsumer
                """,
                expectedTarget: "/apis/storage.k8s.io/v1/storageclasses/synthetic-storage?fieldManager=rune&force=true"
            )
        ]

        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)

        for scenario in scenarios {
            server.resetRequestLines()

            switch scenario.operation {
            case .apply:
                try await client.applyYAML(
                    from: sources,
                    context: context,
                    namespace: "alpha-zone",
                    yaml: scenario.yaml
                )
            case .validate:
                let issues = try await client.validateResourceYAML(
                    from: sources,
                    context: context,
                    namespace: "alpha-zone",
                    yaml: scenario.yaml
                )
                XCTAssertTrue(issues.isEmpty, "\(scenario.name): \(issues)")
            }

            let patches = server.requests().filter { $0.requestLine.hasPrefix("PATCH ") }
            let patch = try XCTUnwrap(
                patches.first,
                "\(scenario.name): expected one server-side apply PATCH"
            )
            XCTAssertEqual(patches.count, 1, scenario.name)
            XCTAssertTrue(
                patch.requestLine.hasPrefix("PATCH \(scenario.expectedTarget) "),
                "\(scenario.name): \(patch.requestLine)"
            )
            XCTAssertEqual(patch.body, scenario.yaml, scenario.name)
            XCTAssertEqual(patch.body?.utf8.count, scenario.yaml.utf8.count, scenario.name)
        }
    }

    func testResourceYAMLRejectionMatrixStopsBeforeAnyMutationRequest() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let scenarios = [
            RejectedResourceYAMLScenario(
                name: "generic CRD kind",
                operation: .apply,
                yaml: """
                apiVersion: synthetic.example.invalid/v1
                kind: SyntheticWidget
                metadata:
                  name: matrix-widget
                  namespace: alpha-zone
                spec:
                  message: synthetic
                """,
                expectedErrorFragment: "kind is not supported"
            ),
            RejectedResourceYAMLScenario(
                name: "missing metadata.name",
                operation: .validate,
                yaml: """
                apiVersion: v1
                kind: ConfigMap
                metadata:
                  namespace: alpha-zone
                data:
                  message: synthetic
                """,
                expectedErrorFragment: "missing metadata.name"
            )
        ]

        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)

        for scenario in scenarios {
            server.resetRequestLines()

            switch scenario.operation {
            case .apply:
                do {
                    try await client.applyYAML(
                        from: sources,
                        context: context,
                        namespace: "alpha-zone",
                        yaml: scenario.yaml
                    )
                    XCTFail("\(scenario.name): expected client-side rejection")
                } catch {
                    XCTAssertTrue(
                        String(describing: error).contains(scenario.expectedErrorFragment),
                        "\(scenario.name): \(error)"
                    )
                }
            case .validate:
                let issues = try await client.validateResourceYAML(
                    from: sources,
                    context: context,
                    namespace: "alpha-zone",
                    yaml: scenario.yaml
                )
                XCTAssertTrue(
                    issues.contains { $0.message.contains(scenario.expectedErrorFragment) },
                    "\(scenario.name): \(issues)"
                )
            }

            XCTAssertFalse(
                server.requests().contains { $0.requestLine.hasPrefix("PATCH ") },
                "\(scenario.name): rejected YAML must not reach a mutation endpoint"
            )
        }
    }

    func testConfigMapApplyThenUndoSendsReversiblePayloadSequence() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let baselineYAML = """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: orbit-lens-settings
          namespace: alpha-zone
        data:
          mode: baseline
        """
        let editedYAML = baselineYAML.replacingOccurrences(of: "mode: baseline", with: "mode: edited")
        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)

        try await client.applyYAML(
            from: sources,
            context: context,
            namespace: "alpha-zone",
            yaml: editedYAML
        )
        try await client.applyYAML(
            from: sources,
            context: context,
            namespace: "alpha-zone",
            yaml: baselineYAML
        )

        let patches = server.requests().filter { request in
            request.requestLine.hasPrefix(
                "PATCH /api/v1/namespaces/alpha-zone/configmaps/orbit-lens-settings?"
            )
        }
        XCTAssertEqual(patches.map(\.body), [editedYAML, baselineYAML])
    }

    func testAllContainerLogsPreserveSuccessfulOutputAndNamePartialFailures() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        let podName = "ember-gate-75c9f746b8-kq2wm"

        let logs = try await client.podLogs(
            from: sources,
            context: context,
            namespace: "alpha-zone",
            podName: podName,
            containers: ["gate", "synthetic-missing"],
            filter: .tailLines(50),
            previous: false
        )

        XCTAssertTrue(logs.contains("[gate]"))
        XCTAssertTrue(logs.contains("synthetic REST fake log"))
        XCTAssertTrue(logs.contains("[synthetic-missing] ⚠ Logs unavailable for container synthetic-missing:"))

        do {
            _ = try await client.podLogs(
                from: sources,
                context: context,
                namespace: "alpha-zone",
                podName: podName,
                containers: ["synthetic-missing-a", "synthetic-missing-b"],
                filter: .tailLines(50),
                previous: false
            )
            XCTFail("All-container logs must fail when every requested container fails.")
        } catch {
            XCTAssertFalse(error is CancellationError)
        }
    }

    func testUnifiedLogsFetchEveryMatchingPodAcrossPhasesWithoutAnEightPodCap() async throws {
        let podCount = 10
        let pods = (0..<podCount).map { index in
            RuneFakeK8sPod(
                name: "synthetic-worker-\(String(format: "%02d", index))",
                deploymentName: "synthetic-worker",
                phase: index == 8 ? "Succeeded" : (index == 9 ? "Failed" : "Running"),
                restarts: 0,
                cpu: "1m",
                memory: "1Mi",
                podIP: nil,
                nodeName: "synthetic-node",
                labels: ["app": "synthetic-worker"],
                containers: ["worker"]
            )
        }
        let fixture = RuneFakeK8sFixture(contexts: [
            RuneFakeK8sCluster(
                contextName: RuneFakeK8sFixture.defaultContextName,
                defaultNamespace: "synthetic-zone",
                namespaces: [
                    RuneFakeK8sNamespace(
                        name: "synthetic-zone",
                        pods: pods,
                        deployments: [],
                        services: [
                            RuneFakeK8sService(
                                name: "synthetic-worker",
                                selector: ["app": "synthetic-worker"],
                                clusterIP: "10.96.0.1"
                            )
                        ]
                    )
                ],
                nodes: []
            )
        ])
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let logs = try await KubernetesClient(commandTimeout: 2).unifiedLogsForService(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: "synthetic-zone",
            service: ServiceSummary(
                name: "synthetic-worker",
                namespace: "synthetic-zone",
                type: "ClusterIP",
                clusterIP: "10.96.0.1",
                selector: ["app": "synthetic-worker"]
            ),
            filter: .all,
            previous: false
        )

        XCTAssertEqual(Set(logs.podNames), Set(pods.map(\.name)))
        XCTAssertEqual(logs.podNames.count, podCount)
        for pod in pods {
            XCTAssertTrue(logs.mergedText.contains("[\(pod.name)]"))
        }
        XCTAssertEqual(
            server.requestLines().filter { $0.contains("/pods/") && $0.contains("/log") }.count,
            podCount
        )
    }

    func testUnifiedLogsKeepSuccessfulPodsAndNameEachFailedPod() async throws {
        let successfulPodName = "synthetic-worker-ok"
        let failingPodName = "synthetic-worker-failed"
        let pods = [successfulPodName, failingPodName].map { name in
            RuneFakeK8sPod(
                name: name,
                deploymentName: "synthetic-worker",
                phase: "Running",
                restarts: 0,
                cpu: "1m",
                memory: "1Mi",
                podIP: nil,
                nodeName: "synthetic-node",
                labels: ["app": "synthetic-worker"],
                containers: ["worker"]
            )
        }
        let fixture = RuneFakeK8sFixture(contexts: [
            RuneFakeK8sCluster(
                contextName: RuneFakeK8sFixture.defaultContextName,
                defaultNamespace: "synthetic-zone",
                namespaces: [
                    RuneFakeK8sNamespace(
                        name: "synthetic-zone",
                        pods: pods,
                        deployments: [],
                        services: [
                            RuneFakeK8sService(
                                name: "synthetic-worker",
                                selector: ["app": "synthetic-worker"],
                                clusterIP: "10.96.0.1"
                            )
                        ],
                        failingLogPodNames: [failingPodName]
                    )
                ],
                nodes: []
            )
        ])
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let logs = try await KubernetesClient(commandTimeout: 2).unifiedLogsForService(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: "synthetic-zone",
            service: ServiceSummary(
                name: "synthetic-worker",
                namespace: "synthetic-zone",
                type: "ClusterIP",
                clusterIP: "10.96.0.1",
                selector: ["app": "synthetic-worker"]
            ),
            filter: .all,
            previous: false
        )

        XCTAssertEqual(Set(logs.podNames), Set([successfulPodName, failingPodName]))
        XCTAssertTrue(logs.mergedText.contains("[\(successfulPodName)]"))
        XCTAssertTrue(
            logs.mergedText.contains("[\(failingPodName)] ⚠ Logs unavailable for pod \(failingPodName):")
        )
    }

    func testRESTFakeCanForcePodLogEndpointFailure() async throws {
        let failingPodName = "ember-gate-75c9f746b8-kq2wm"
        let base = RuneFakeK8sFixture.defaultContexts[0]
        let fixture = RuneFakeK8sFixture(contexts: [
            RuneFakeK8sCluster(
                contextName: base.contextName,
                defaultNamespace: base.defaultNamespace,
                namespaces: base.namespaces.map { namespace in
                    RuneFakeK8sNamespace(
                        name: namespace.name,
                        pods: namespace.pods,
                        deployments: namespace.deployments,
                        services: namespace.services,
                        failingLogPodNames: namespace.name == "alpha-zone" ? [failingPodName] : []
                    )
                },
                nodes: base.nodes,
                operatorResources: base.operatorResources
            )
        ])
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        defer { server.stop() }

        let url = URL(string: "http://127.0.0.1:\(server.port)/api/v1/namespaces/alpha-zone/pods/\(failingPodName)/log")!
        let (data, response) = try await URLSession.shared.data(from: url)

        XCTAssertEqual((response as? HTTPURLResponse)?.statusCode, 500)
        XCTAssertTrue(String(decoding: data, as: UTF8.self).contains("Synthetic forced pod log failure"))
    }

    func testNativeClientRetriesTransientRESTReadFailure() async throws {
        let target = "/api/v1/namespaces/alpha-zone/pods"
        let fixture = RuneFakeK8sFixture(transientFailureTargets: [target])
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let pods = try await client.listPods(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: "alpha-zone"
        )
        let podListRequests = server.requestLines().filter { line in
            line.contains(" \(target) ")
        }

        XCTAssertEqual(pods.map(\.name), ["ember-gate-75c9f746b8-kq2wm", "orbit-lens-6f58d7d89b-hx9q2"])
        XCTAssertEqual(podListRequests.count, 2)
    }

    func testNativeClientRecordsPrivacySafeRESTRequestMetrics() async throws {
        let recorder = KubernetesRESTRequestMetricsRecorder()
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let restClient = KubernetesRESTClient(requestMetricsRecorder: recorder)
        let client = KubernetesClient(
            commandTimeout: 2,
            restClient: restClient,
            requestMetricsRecorder: recorder
        )
        let pods = try await client.listPodStatuses(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: "alpha-zone"
        )

        let report = await recorder.report()
        let metrics = report.metrics
        let summary = report.summary
        let scopedReport = await recorder.report(contextName: RuneFakeK8sFixture.defaultContextName)
        let unrelatedReport = await recorder.report(contextName: "synthetic-unrelated-context")
        let podListMetric = try XCTUnwrap(metrics.first { $0.apiPath.contains("/pods") })

        XCTAssertEqual(pods.count, 2)
        XCTAssertEqual(podListMetric.sourcePath, "swift-rest")
        XCTAssertEqual(podListMetric.method, "GET")
        XCTAssertEqual(podListMetric.statusCode, 200)
        XCTAssertEqual(podListMetric.outcome, .success)
        XCTAssertGreaterThan(podListMetric.responseBytes, 0)
        XCTAssertGreaterThanOrEqual(podListMetric.durationSeconds, 0)
        XCTAssertEqual(podListMetric.apiPath, "/api/v1/namespaces/<namespace>/pods")
        XCTAssertFalse(podListMetric.apiPath.contains("alpha-zone"))
        XCTAssertEqual(summary.requestCount, metrics.count)
        XCTAssertGreaterThanOrEqual(summary.successCount, 1)
        XCTAssertEqual(scopedReport.metrics, metrics)
        XCTAssertEqual(scopedReport.summary, summary)
        XCTAssertTrue(unrelatedReport.metrics.isEmpty)
        XCTAssertEqual(unrelatedReport.summary.requestCount, 0)
    }

    func testNativeClientDoesNotMergeMetricsWhenContextNameIsReusedByAnotherKubeconfig() async throws {
        let recorder = KubernetesRESTRequestMetricsRecorder()
        let firstServer = try await RuneFakeK8sRESTServer.start()
        let secondServer = try await RuneFakeK8sRESTServer.start()
        defer {
            firstServer.stop()
            secondServer.stop()
        }
        let firstKubeconfig = try writeKubeconfig(firstServer.kubeconfigYAML())
        let secondKubeconfig = try writeKubeconfig(secondServer.kubeconfigYAML())
        defer {
            try? FileManager.default.removeItem(at: firstKubeconfig)
            try? FileManager.default.removeItem(at: secondKubeconfig)
        }

        let restClient = KubernetesRESTClient(requestMetricsRecorder: recorder)
        let client = KubernetesClient(
            commandTimeout: 2,
            restClient: restClient,
            requestMetricsRecorder: recorder
        )
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)

        _ = try await client.listPodStatuses(
            from: [KubeConfigSource(url: firstKubeconfig)],
            context: context,
            namespace: "alpha-zone"
        )
        let firstReport = await recorder.report(contextName: context.name)

        _ = try await client.listPodStatuses(
            from: [KubeConfigSource(url: secondKubeconfig)],
            context: context,
            namespace: "alpha-zone"
        )
        let secondReport = await recorder.report(contextName: context.name)
        let globalSummary = await recorder.summary()

        XCTAssertEqual(firstReport.summary.requestCount, 1)
        XCTAssertEqual(secondReport.summary.requestCount, 1)
        XCTAssertEqual(secondReport.metrics.count, 1)
        XCTAssertEqual(secondReport.endpointGroups.reduce(0) { $0 + $1.requestCount }, 1)
        XCTAssertEqual(globalSummary.requestCount, 2)
    }

    func testScopedMetricsReportStaysEmptyAfterSamePathKubeconfigReplacementUntilNewRequest() async throws {
        let recorder = KubernetesRESTRequestMetricsRecorder()
        let firstServer = try await RuneFakeK8sRESTServer.start()
        let secondServer = try await RuneFakeK8sRESTServer.start()
        defer {
            firstServer.stop()
            secondServer.stop()
        }
        let kubeconfig = try writeKubeconfig(firstServer.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(
            commandTimeout: 2,
            restClient: KubernetesRESTClient(requestMetricsRecorder: recorder),
            requestMetricsRecorder: recorder
        )
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)

        _ = try await client.listPodStatuses(
            from: sources,
            context: context,
            namespace: "alpha-zone"
        )
        let initialReport = await client.restRequestMetricsReport(
            from: sources,
            context: context
        )
        XCTAssertEqual(initialReport.summary.requestCount, 1)

        try secondServer.kubeconfigYAML().write(
            to: kubeconfig,
            atomically: true,
            encoding: .utf8
        )
        try FileManager.default.setAttributes(
            [.modificationDate: Date(timeIntervalSinceNow: 2)],
            ofItemAtPath: kubeconfig.path
        )

        let replacementBeforeRequest = await client.restRequestMetricsReport(
            from: sources,
            context: context
        )
        XCTAssertEqual(replacementBeforeRequest, .empty)

        _ = try await client.listPodStatuses(
            from: sources,
            context: context,
            namespace: "alpha-zone"
        )
        let replacementAfterRequest = await client.restRequestMetricsReport(
            from: sources,
            context: context
        )
        XCTAssertEqual(replacementAfterRequest.summary.requestCount, 1)
        XCTAssertEqual(replacementAfterRequest.metrics.count, 1)
    }

    func testFailedReplacementScopeDoesNotExposePreviousSameNameMetrics() async throws {
        let recorder = KubernetesRESTRequestMetricsRecorder()
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let firstKubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        let unresolvedKubeconfig = try writeKubeconfig(
            """
            apiVersion: v1
            kind: Config
            current-context: \(RuneFakeK8sFixture.defaultContextName)
            contexts:
            - name: \(RuneFakeK8sFixture.defaultContextName)
              context:
                cluster: missing-cluster
                user: missing-user
                namespace: alpha-zone
            clusters: []
            users: []
            """
        )
        defer {
            try? FileManager.default.removeItem(at: firstKubeconfig)
            try? FileManager.default.removeItem(at: unresolvedKubeconfig)
        }

        let client = KubernetesClient(
            commandTimeout: 2,
            restClient: KubernetesRESTClient(requestMetricsRecorder: recorder),
            requestMetricsRecorder: recorder
        )
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)

        _ = try await client.listPodStatuses(
            from: [KubeConfigSource(url: firstKubeconfig)],
            context: context,
            namespace: "alpha-zone"
        )
        let firstReport = await recorder.report(contextName: context.name)
        XCTAssertEqual(firstReport.summary.requestCount, 1)

        do {
            _ = try await client.listPodStatuses(
                from: [KubeConfigSource(url: unresolvedKubeconfig)],
                context: context,
                namespace: "alpha-zone"
            )
            XCTFail("Expected the replacement kubeconfig with a missing cluster to fail.")
        } catch {
            // Expected: activation still advances to the replacement scope before
            // the request can reach the network.
        }

        let replacementReport = await recorder.report(contextName: context.name)
        let globalSummary = await recorder.summary()
        XCTAssertTrue(replacementReport.metrics.isEmpty)
        XCTAssertEqual(replacementReport.summary.requestCount, 0)
        XCTAssertEqual(globalSummary.requestCount, 1)
    }

    func testNativeClientRecordsRESTRequestMetricsForRetriedReads() async throws {
        let recorder = KubernetesRESTRequestMetricsRecorder()
        let target = "/api/v1/namespaces/alpha-zone/pods"
        let fixture = RuneFakeK8sFixture(transientFailureTargets: [target])
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let restClient = KubernetesRESTClient(requestMetricsRecorder: recorder)
        let client = KubernetesClient(
            commandTimeout: 2,
            restClient: restClient,
            requestMetricsRecorder: recorder
        )
        let pods = try await client.listPodStatuses(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: "alpha-zone"
        )

        let scopedReport = await recorder.report(contextName: RuneFakeK8sFixture.defaultContextName)
        let unrelatedReport = await recorder.report(contextName: "synthetic-unrelated-context")
        let podMetrics = scopedReport.metrics
            .filter { $0.apiPath == "/api/v1/namespaces/<namespace>/pods" }

        XCTAssertEqual(pods.count, 2)
        XCTAssertEqual(podMetrics.map(\.attempt), [1, 2])
        XCTAssertEqual(podMetrics.map(\.outcome), [.httpError, .success])
        XCTAssertEqual(podMetrics.map(\.statusCode), [503, 200])
        XCTAssertEqual(scopedReport.summary.requestCount, 2)
        XCTAssertEqual(scopedReport.summary.failureCount, 1)
        XCTAssertEqual(unrelatedReport.summary.requestCount, 0)
        XCTAssertTrue(podMetrics.allSatisfy { !$0.apiPath.contains("alpha-zone") })
    }

    func testNativeClientPreservesCancellationAndRecordsCancelledRESTMetric() async throws {
        let recorder = KubernetesRESTRequestMetricsRecorder()
        let target = "/api/v1/namespaces/alpha-zone/pods"
        let fixture = RuneFakeK8sFixture(delayedResponseTargets: [target: 500_000_000])
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let restClient = KubernetesRESTClient(requestMetricsRecorder: recorder)
        let client = KubernetesClient(
            commandTimeout: 2,
            restClient: restClient,
            requestMetricsRecorder: recorder
        )
        let task = Task {
            try await client.listPodStatuses(
                from: [KubeConfigSource(url: kubeconfig)],
                context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
                namespace: "alpha-zone"
            )
        }

        for _ in 0..<100 where server.requestLines().isEmpty {
            try await Task.sleep(nanoseconds: 5_000_000)
        }
        XCTAssertFalse(server.requestLines().isEmpty)

        task.cancel()
        do {
            _ = try await task.value
            XCTFail("Expected cancelled pod list read to throw CancellationError.")
        } catch is CancellationError {
        } catch {
            XCTFail("Expected CancellationError, got \(error).")
        }

        let scopedReport = await recorder.report(contextName: RuneFakeK8sFixture.defaultContextName)
        let unrelatedReport = await recorder.report(contextName: "synthetic-unrelated-context")
        let podMetrics = scopedReport.metrics
            .filter { $0.apiPath == "/api/v1/namespaces/<namespace>/pods" }
        let cancelledMetric = try XCTUnwrap(podMetrics.first)

        XCTAssertEqual(cancelledMetric.outcome, .cancelled)
        XCTAssertEqual(cancelledMetric.cancellationReason, "task-cancelled")
        XCTAssertNil(cancelledMetric.statusCode)
        XCTAssertEqual(cancelledMetric.responseBytes, 0)
        XCTAssertEqual(scopedReport.summary.cancelledCount, 1)
        XCTAssertEqual(scopedReport.summary.requestCount, 1)
        XCTAssertEqual(unrelatedReport.summary.requestCount, 0)
        XCTAssertTrue(podMetrics.allSatisfy { !$0.apiPath.contains("alpha-zone") })
    }

    func testNativeClientSendsDryRunAllForDeploymentRollbackPreview() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        try await client.dryRunRollbackDeploymentRollout(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: "alpha-zone",
            deploymentName: "orbit-lens",
            revision: 1
        )
        let requests = server.requestLines().joined(separator: "\n")

        XCTAssertTrue(requests.contains("GET /apis/apps/v1/namespaces/alpha-zone/deployments/orbit-lens "))
        XCTAssertTrue(requests.contains("GET /apis/apps/v1/namespaces/alpha-zone/replicasets?labelSelector=app%3Dorbit-lens "))
        XCTAssertTrue(requests.contains("PATCH /apis/apps/v1/namespaces/alpha-zone/deployments/orbit-lens?dryRun=All "))
    }

    func testRESTFakeSupportsKubernetesPaginationMetadata() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }

        let url = URL(string: "http://127.0.0.1:\(server.port)/api/v1/namespaces/alpha-zone/pods?limit=1")!
        let (data, response) = try await URLSession.shared.data(from: url)
        XCTAssertEqual((response as? HTTPURLResponse)?.statusCode, 200)

        let object = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        let metadata = object?["metadata"] as? [String: Any]
        let items = object?["items"] as? [[String: Any]]

        XCTAssertEqual(metadata?["remainingItemCount"] as? Int, 1)
        XCTAssertEqual(metadata?["continue"] as? String, "1")
        XCTAssertEqual(items?.count, 1)
    }

    func testNativeCountUsesPagedRESTFallbackWhenRemainingItemCountIsMissing() async throws {
        let base = RuneFakeK8sFixture.defaultContexts[0]
        let pods = (0..<251).map { index in
            RuneFakeK8sPod(
                name: String(format: "paged-pod-%03d", index),
                deploymentName: "paged",
                phase: "Running",
                restarts: 0,
                cpu: "1m",
                memory: "8Mi",
                podIP: "10.42.9.\(index % 250)",
                nodeName: "orbit-node-a",
                labels: ["app": "paged"],
                containers: ["app"]
            )
        }
        let fixture = RuneFakeK8sFixture(
            contexts: [
                RuneFakeK8sCluster(
                    contextName: base.contextName,
                    defaultNamespace: "alpha-zone",
                    namespaces: [
                        RuneFakeK8sNamespace(
                            name: "alpha-zone",
                            pods: pods,
                            deployments: [],
                            services: []
                        )
                    ],
                    nodes: base.nodes,
                    operatorResources: base.operatorResources
                )
            ],
            listKindsOmittingRemainingItemCount: ["PodList"]
        )
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let count = try await client.countNamespacedResources(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: "alpha-zone",
            resource: "pods"
        )
        let requests = server.requestLines().joined(separator: "\n")

        XCTAssertEqual(count, 251)
        XCTAssertTrue(requests.contains("/api/v1/namespaces/alpha-zone/pods?limit=1"))
        XCTAssertTrue(requests.contains("/api/v1/namespaces/alpha-zone/pods?limit=250"))
        XCTAssertTrue(requests.contains("continue=250"))
    }

    func testOperatorResourceProbeReturnsEmptyWhenCRDsAreAbsent() async throws {
        let server = try await RuneFakeK8sRESTServer.start()
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let resources = try await client.listOperatorResources(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: "alpha-zone"
        )

        XCTAssertTrue(resources.isEmpty)
    }

    func testOperatorResourceProbeReadsCertManagerFluxAndArgoCDResources() async throws {
        let base = RuneFakeK8sFixture.defaultContexts[0]
        let fixture = RuneFakeK8sFixture(contexts: [
            RuneFakeK8sCluster(
                contextName: base.contextName,
                defaultNamespace: base.defaultNamespace,
                namespaces: base.namespaces,
                nodes: base.nodes,
                operatorResources: [
                    RuneFakeK8sOperatorResource(
                        apiGroup: "cert-manager.io",
                        apiVersion: "v1",
                        plural: "certificates",
                        kind: "Certificate",
                        name: "web-tls",
                        namespace: "alpha-zone",
                        conditionType: "Ready",
                        conditionStatus: "True",
                        reason: "Issued",
                        message: "Certificate is up to date",
                        printerColumnDefinitions: [
                            "Message": ".status.conditions[0].message",
                            "Namespace": ".metadata.namespace",
                            "Ready": ".status.conditions[0].status",
                            "UID": ".metadata.uid"
                        ]
                    ),
                    RuneFakeK8sOperatorResource(
                        apiGroup: "source.toolkit.fluxcd.io",
                        apiVersion: "v1",
                        plural: "gitrepositories",
                        kind: "GitRepository",
                        name: "platform",
                        namespace: "alpha-zone",
                        conditionType: "Ready",
                        conditionStatus: "False",
                        reason: "FetchFailed",
                        message: "Waiting for repository access"
                    ),
                    RuneFakeK8sOperatorResource(
                        apiGroup: "argoproj.io",
                        apiVersion: "v1alpha1",
                        plural: "applications",
                        kind: "Application",
                        name: "control-plane",
                        namespace: "alpha-zone",
                        conditionType: "Synced",
                        conditionStatus: "True",
                        reason: "Healthy",
                        message: "Application is synced"
                    ),
                    RuneFakeK8sOperatorResource(
                        apiGroup: "external-secrets.io",
                        apiVersion: "v1beta1",
                        plural: "externalsecrets",
                        kind: "ExternalSecret",
                        name: "payments-api",
                        namespace: "alpha-zone",
                        conditionType: "Ready",
                        conditionStatus: "True",
                        reason: "SecretSynced",
                        message: "Secret is synced"
                    ),
                    RuneFakeK8sOperatorResource(
                        apiGroup: "apiextensions.crossplane.io",
                        apiVersion: "v1",
                        plural: "compositions",
                        kind: "Composition",
                        name: "postgresql",
                        namespace: nil,
                        conditionType: "Established",
                        conditionStatus: "True",
                        reason: "Active",
                        message: "Composition is available"
                    ),
                    RuneFakeK8sOperatorResource(
                        apiGroup: "gateway.networking.k8s.io",
                        apiVersion: "v1",
                        plural: "gateways",
                        kind: "Gateway",
                        name: "edge",
                        namespace: "alpha-zone",
                        conditionType: "Programmed",
                        conditionStatus: "True",
                        reason: "ListenersValid",
                        message: "Gateway listeners are programmed"
                    ),
                    RuneFakeK8sOperatorResource(
                        apiGroup: "monitoring.coreos.com",
                        apiVersion: "v1",
                        plural: "servicemonitors",
                        kind: "ServiceMonitor",
                        name: "api-metrics",
                        namespace: "alpha-zone",
                        conditionType: "Available",
                        conditionStatus: "False",
                        reason: "EndpointMissing",
                        message: "No matching service endpoints"
                    )
                ]
            )
        ])
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let resources = try await client.listOperatorResources(
            from: [KubeConfigSource(url: kubeconfig)],
            context: KubeContext(name: RuneFakeK8sFixture.defaultContextName),
            namespace: "alpha-zone"
        )

        XCTAssertEqual(Set(resources.map(\.family)), Set(["ArgoCD", "Crossplane", "External Secrets", "Flux", "Gateway API", "Prometheus Operator", "cert-manager"]))
        XCTAssertEqual(resources.first(where: { $0.name == "web-tls" })?.kind, "Certificates")
        XCTAssertEqual(resources.first(where: { $0.name == "web-tls" })?.status, "Ready True")
        let printerColumns = try XCTUnwrap(resources.first(where: { $0.name == "web-tls" })?.printerColumns)
        XCTAssertEqual(printerColumns.count, 3)
        XCTAssertTrue(printerColumns.contains(OperatorResourceSummary.PrinterColumn(title: "Message", value: "Certificate is up to date")))
        XCTAssertTrue(printerColumns.contains(OperatorResourceSummary.PrinterColumn(title: "Namespace", value: "alpha-zone")))
        XCTAssertTrue(printerColumns.contains(OperatorResourceSummary.PrinterColumn(title: "Ready", value: "True")))
        XCTAssertEqual(resources.first(where: { $0.name == "platform" })?.message, "Waiting for repository access")
        XCTAssertEqual(resources.first(where: { $0.name == "control-plane" })?.apiPath, "/apis/argoproj.io/v1alpha1/namespaces/alpha-zone/applications")
        XCTAssertEqual(resources.first(where: { $0.name == "payments-api" })?.family, "External Secrets")
        XCTAssertEqual(resources.first(where: { $0.name == "postgresql" })?.namespace, nil)
        XCTAssertEqual(resources.first(where: { $0.name == "edge" })?.status, "Programmed True")
        XCTAssertEqual(resources.first(where: { $0.name == "api-metrics" })?.kind, "ServiceMonitors")
        XCTAssertEqual(resources.first(where: { $0.name == "api-metrics" })?.status, "Available False")
        XCTAssertEqual(resources.first(where: { $0.name == "api-metrics" })?.message, "No matching service endpoints")
    }

    func testOperatorResourceYAMLAndDescribeDrilldownReadsNamespacedAndClusterScopedResources() async throws {
        let base = RuneFakeK8sFixture.defaultContexts[0]
        let fixture = RuneFakeK8sFixture(contexts: [
            RuneFakeK8sCluster(
                contextName: base.contextName,
                defaultNamespace: base.defaultNamespace,
                namespaces: base.namespaces,
                nodes: base.nodes,
                operatorResources: [
                    RuneFakeK8sOperatorResource(
                        apiGroup: "cert-manager.io",
                        apiVersion: "v1",
                        plural: "certificates",
                        kind: "Certificate",
                        name: "web-tls",
                        namespace: "alpha-zone",
                        conditionType: "Ready",
                        conditionStatus: "True",
                        reason: "Issued",
                        message: "Certificate is up to date"
                    ),
                    RuneFakeK8sOperatorResource(
                        apiGroup: "apiextensions.crossplane.io",
                        apiVersion: "v1",
                        plural: "compositions",
                        kind: "Composition",
                        name: "postgresql",
                        namespace: nil,
                        conditionType: "Established",
                        conditionStatus: "True",
                        reason: "Active",
                        message: "Composition is available"
                    )
                ]
            )
        ])
        let server = try await RuneFakeK8sRESTServer.start(fixture: fixture)
        defer { server.stop() }
        let kubeconfig = try writeKubeconfig(server.kubeconfigYAML())
        defer { try? FileManager.default.removeItem(at: kubeconfig) }

        let client = KubernetesClient(commandTimeout: 2)
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: RuneFakeK8sFixture.defaultContextName)
        let resources = try await client.listOperatorResources(from: sources, context: context, namespace: "alpha-zone")

        let certificate = try XCTUnwrap(resources.first { $0.name == "web-tls" })
        let certificateYAML = try await client.operatorResourceYAML(from: sources, context: context, resource: certificate)
        let certificateDescribe = try await client.operatorResourceDescribe(from: sources, context: context, resource: certificate)

        XCTAssertTrue(certificateYAML.contains(#""kind" : "Certificate""#))
        XCTAssertTrue(certificateYAML.contains(#""name" : "web-tls""#))
        XCTAssertTrue(certificateDescribe.contains("Name: web-tls"))
        XCTAssertTrue(certificateDescribe.contains("Namespace: alpha-zone"))
        XCTAssertTrue(certificateDescribe.contains("Kind: Certificates"))
        XCTAssertTrue(certificateDescribe.contains("Certificate is up to date"))

        let composition = try XCTUnwrap(resources.first { $0.name == "postgresql" })
        let compositionYAML = try await client.operatorResourceYAML(from: sources, context: context, resource: composition)
        let compositionDescribe = try await client.operatorResourceDescribe(from: sources, context: context, resource: composition)

        XCTAssertTrue(compositionYAML.contains(#""kind" : "Composition""#))
        XCTAssertTrue(compositionDescribe.contains("Namespace: <cluster>"))
        XCTAssertTrue(compositionDescribe.contains("Composition is available"))
    }

    private func writeKubeconfig(_ contents: String) throws -> URL {
        let url = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-rest-fake-kubeconfig-\(UUID().uuidString).yaml")
        try contents.write(to: url, atomically: true, encoding: .utf8)
        return url
    }

    private func requestQueryItems(_ requestLine: String) throws -> [String: String] {
        let fields = requestLine.split(separator: " ")
        guard fields.count >= 2,
              let components = URLComponents(string: "http://127.0.0.1\(fields[1])")
        else {
            throw RuneError.parseError(message: "Synthetic request line did not contain a valid request target")
        }
        return Dictionary(
            components.queryItems?.compactMap { item in
                item.value.map { (item.name, $0) }
            } ?? [],
            uniquingKeysWith: { _, latest in latest }
        )
    }
}

private enum ResourceYAMLRequestOperation {
    case apply
    case validate
}

private struct ResourceYAMLRequestScenario {
    let name: String
    let operation: ResourceYAMLRequestOperation
    let yaml: String
    let expectedTarget: String
}

private struct RejectedResourceYAMLScenario {
    let name: String
    let operation: ResourceYAMLRequestOperation
    let yaml: String
    let expectedErrorFragment: String
}
