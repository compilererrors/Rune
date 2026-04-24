import Foundation
import XCTest
@testable import RuneCore
@testable import RuneKube

final class LocalKubernetesIntegrationTests: XCTestCase {
    private let integrationFlag = "RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS"
    private let repoRoot = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent()
        .deletingLastPathComponent()
        .deletingLastPathComponent()

    func testRuneFakeK8sEventsPointAtExistingPods() async throws {
        try requireIntegrationEnabled()

        let stateDir = URL(fileURLWithPath: NSTemporaryDirectory())
            .appendingPathComponent("rune-fake-k8s-integration", isDirectory: true)
        try? FileManager.default.removeItem(at: stateDir)
        try FileManager.default.createDirectory(
            at: stateDir.appendingPathComponent("home", isDirectory: true),
            withIntermediateDirectories: true
        )
        let setup = repoRoot.appendingPathComponent("scripts/rune-fake-k8s.sh")
        let fakeK8sBinary = try locateRuneFakeK8sBinary()
        _ = try await ProcessCommandRunner().run(
            executable: "/bin/bash",
            arguments: [setup.path, "setup"],
            environment: [
                "HOME": stateDir.appendingPathComponent("home", isDirectory: true).path,
                "RUNE_FAKE_K8S_BINARY": fakeK8sBinary.path,
                "RUNE_FAKE_K8S_STATE": stateDir.path
            ],
            timeout: 30
        )

        let kubeconfig = stateDir.appendingPathComponent("kubeconfig.yaml")
        try assertRuneFakeKubeconfigIsSafe(kubeconfig)

        let runner = EnvironmentOverlayRunner(overlay: [
            "PATH": stateDir.appendingPathComponent("bin", isDirectory: true).path + ":" + (ProcessInfo.processInfo.environment["PATH"] ?? ""),
            "RUNE_K8S_AGENT": "",
            "RUNE_DISABLE_DEFAULT_KUBECONFIG_DISCOVERY": "1"
        ])
        try await assertPodEventTargetsResolve(
            runner: runner,
            kubeconfig: kubeconfig,
            context: "fake-orbit-mesh",
            namespace: "alpha-zone"
        )
        try await assertPodEventTargetsResolve(
            runner: runner,
            kubeconfig: kubeconfig,
            context: "fake-lattice-spark",
            namespace: "delta-zone"
        )
    }

    func testDockerComposeFakeK8sResourceGraphAndEventsAreLocalAndResolvable() async throws {
        try requireIntegrationEnabled()

        let kubeconfig = repoRoot.appendingPathComponent("docker-compose/generated/rune-fake-kubeconfig.yaml")
        guard FileManager.default.fileExists(atPath: kubeconfig.path) else {
            throw XCTSkip("Start docker compose fake-k8s and run docker-compose/merge-kubeconfig.sh first.")
        }

        try assertDockerComposeKubeconfigIsSafe(kubeconfig)

        let runner = EnvironmentOverlayRunner(overlay: [
            "RUNE_K8S_AGENT": "",
            "RUNE_DISABLE_DEFAULT_KUBECONFIG_DISCOVERY": "1"
        ])

        try await assertClusterReady(
            runner: runner,
            kubeconfig: kubeconfig,
            context: "fake-orbit-mesh"
        )
        try await assertClusterReady(
            runner: runner,
            kubeconfig: kubeconfig,
            context: "fake-lattice-spark"
        )

        try await assertServiceSelectorsResolvePods(
            runner: runner,
            kubeconfig: kubeconfig,
            context: "fake-orbit-mesh",
            namespace: "alpha-zone"
        )
        try await assertServiceSelectorsResolvePods(
            runner: runner,
            kubeconfig: kubeconfig,
            context: "fake-lattice-spark",
            namespace: "delta-zone"
        )

        try await assertEventInvolvedObjectsResolve(
            runner: runner,
            kubeconfig: kubeconfig,
            context: "fake-orbit-mesh",
            namespace: "alpha-zone"
        )
        try await assertEventInvolvedObjectsResolve(
            runner: runner,
            kubeconfig: kubeconfig,
            context: "fake-lattice-spark",
            namespace: "delta-zone"
        )
    }

    func testDockerComposeFakeK8sReadWriteOperationsAreReversible() async throws {
        try requireIntegrationEnabled()

        let kubeconfig = repoRoot.appendingPathComponent("docker-compose/generated/rune-fake-kubeconfig.yaml")
        guard FileManager.default.fileExists(atPath: kubeconfig.path) else {
            throw XCTSkip("Start docker compose fake-k8s and run docker-compose/merge-kubeconfig.sh first.")
        }

        try assertDockerComposeKubeconfigIsSafe(kubeconfig)

        let runner = EnvironmentOverlayRunner(overlay: [
            "RUNE_K8S_AGENT": "",
            "RUNE_DISABLE_DEFAULT_KUBECONFIG_DISCOVERY": "1"
        ])
        let client = KubectlClient(
            runner: runner,
            k8sAgentPath: "/nonexistent/rune-k8s-agent",
            commandTimeout: 45
        )
        let sources = [KubeConfigSource(url: kubeconfig)]
        let context = KubeContext(name: "fake-orbit-mesh")
        let namespace = "alpha-zone"
        let deploymentName = "ember-gate"
        let cronJobName = "orbit-sweep-cycle"
        let runID = "rune-it-\(Int(Date().timeIntervalSince1970))"
        var cleanup: [() async -> Void] = []

        do {
            try await assertClusterReady(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name
            )

            let deployments = try await client.listDeployments(from: sources, context: context, namespace: namespace)
            XCTAssertTrue(deployments.contains { $0.name == deploymentName }, "Expected deployment \(deploymentName)")
            let pods = try await client.listPods(from: sources, context: context, namespace: namespace)
            XCTAssertFalse(pods.isEmpty, "Expected pods in \(context.name)/\(namespace)")
            let services = try await client.listServices(from: sources, context: context, namespace: namespace)
            XCTAssertFalse(services.isEmpty, "Expected services in \(context.name)/\(namespace)")
            let cronJobs = try await client.listCronJobs(from: sources, context: context, namespace: namespace)
            XCTAssertTrue(cronJobs.contains { $0.name == cronJobName }, "Expected CronJob \(cronJobName)")

            let deploymentYAML = try await client.resourceYAML(
                from: sources,
                context: context,
                namespace: namespace,
                kind: .deployment,
                name: deploymentName
            )
            XCTAssertTrue(deploymentYAML.contains("kind: Deployment"))

            let deploymentDescribe = try await client.resourceDescribe(
                from: sources,
                context: context,
                namespace: namespace,
                kind: .deployment,
                name: deploymentName
            )
            XCTAssertTrue(deploymentDescribe.contains("Name:") || deploymentDescribe.contains(deploymentName))

            let selectedPodName = try await waitForReadyPodName(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                selector: "app=\(deploymentName)"
            )
            let execResult = try await client.execInPod(
                from: sources,
                context: context,
                namespace: namespace,
                podName: selectedPodName,
                container: nil,
                command: ["sh", "-c", "printf rune-exec-ok"]
            )
            XCTAssertEqual(execResult.exitCode, 0)
            XCTAssertEqual(execResult.stdout, "rune-exec-ok")

            let logs = try await client.podLogs(
                from: sources,
                context: context,
                namespace: namespace,
                podName: selectedPodName,
                filter: .tailLines(20),
                previous: false
            )
            XCTAssertFalse(logs.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty, "Expected pod logs from \(selectedPodName)")

            let configMapName = "\(runID)-config"
            let configMapYAML = configMapManifest(name: configMapName, namespace: namespace, value: "one")
            let validationIssues = try await client.validateResourceYAML(
                from: sources,
                context: context,
                namespace: namespace,
                yaml: configMapYAML
            )
            XCTAssertTrue(validationIssues.isEmpty, "Valid ConfigMap YAML should pass server validation: \(validationIssues)")

            try await client.applyYAML(from: sources, context: context, namespace: namespace, yaml: configMapYAML)
            cleanup.append {
                try? await client.deleteResource(from: sources, context: context, namespace: namespace, kind: .configMap, name: configMapName)
            }
            var configMap = try await waitForNamedResource(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                resource: "configmaps",
                name: configMapName
            )
            XCTAssertEqual(dataValue(in: configMap, key: "value"), "one")

            try await client.applyYAML(
                from: sources,
                context: context,
                namespace: namespace,
                yaml: configMapManifest(name: configMapName, namespace: namespace, value: "two")
            )
            configMap = try await waitForNamedResource(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                resource: "configmaps",
                name: configMapName
            )
            XCTAssertEqual(dataValue(in: configMap, key: "value"), "two")

            let cronJob = try await namedResourceJSON(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                resource: "cronjobs",
                name: cronJobName
            )
            let originalSuspend = specBool(in: cronJob, key: "suspend") ?? false
            cleanup.append {
                try? await client.patchCronJobSuspend(
                    from: sources,
                    context: context,
                    namespace: namespace,
                    name: cronJobName,
                    suspend: originalSuspend
                )
            }
            try await client.patchCronJobSuspend(from: sources, context: context, namespace: namespace, name: cronJobName, suspend: true)
            try await waitForSpecBool(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                resource: "cronjobs",
                name: cronJobName,
                key: "suspend",
                expected: true
            )
            try await client.patchCronJobSuspend(from: sources, context: context, namespace: namespace, name: cronJobName, suspend: originalSuspend)
            try await waitForSpecBool(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                resource: "cronjobs",
                name: cronJobName,
                key: "suspend",
                expected: originalSuspend
            )

            let manualJobName = "\(runID)-job"
            try await client.createJobFromCronJob(
                from: sources,
                context: context,
                namespace: namespace,
                cronJobName: cronJobName,
                jobName: manualJobName
            )
            cleanup.append {
                try? await client.deleteResource(from: sources, context: context, namespace: namespace, kind: .job, name: manualJobName)
            }
            _ = try await waitForNamedResource(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                resource: "jobs",
                name: manualJobName
            )

            let deployment = try await namedResourceJSON(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                resource: "deployments",
                name: deploymentName
            )
            let originalReplicas = specInt(in: deployment, key: "replicas") ?? 1
            let originalRestartAnnotation = podTemplateAnnotation(
                in: deployment,
                key: "kubectl.kubernetes.io/restartedAt"
            )
            cleanup.append {
                try? await self.restoreDeploymentRestartAnnotation(
                    runner: runner,
                    kubeconfig: kubeconfig,
                    context: context.name,
                    namespace: namespace,
                    deploymentName: deploymentName,
                    value: originalRestartAnnotation
                )
            }
            cleanup.append {
                try? await client.scaleDeployment(
                    from: sources,
                    context: context,
                    namespace: namespace,
                    deploymentName: deploymentName,
                    replicas: originalReplicas
                )
            }
            let scaledReplicas = max(1, originalReplicas + 1)
            try await client.scaleDeployment(
                from: sources,
                context: context,
                namespace: namespace,
                deploymentName: deploymentName,
                replicas: scaledReplicas
            )
            try await waitForSpecInt(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                resource: "deployments",
                name: deploymentName,
                key: "replicas",
                expected: scaledReplicas
            )
            try await client.scaleDeployment(
                from: sources,
                context: context,
                namespace: namespace,
                deploymentName: deploymentName,
                replicas: originalReplicas
            )
            try await waitForDeploymentReadyReplicas(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                deploymentName: deploymentName,
                expectedReady: originalReplicas
            )

            let history = try await client.deploymentRolloutHistory(
                from: sources,
                context: context,
                namespace: namespace,
                deploymentName: deploymentName
            )
            XCTAssertTrue(history.contains(deploymentName) || history.contains("REVISION"))

            let portForwardExpectation = expectation(description: "pod port-forward becomes active")
            let localPort = 20_000 + Int.random(in: 0..<20_000)
            let session = try await client.startPortForward(
                from: sources,
                context: context,
                namespace: namespace,
                targetKind: .pod,
                targetName: selectedPodName,
                localPort: localPort,
                remotePort: 8080,
                address: "127.0.0.1"
            ) { session in
                if session.status == .active {
                    portForwardExpectation.fulfill()
                }
            }
            await fulfillment(of: [portForwardExpectation], timeout: 10)
            await client.stopPortForward(sessionID: session.id)

            try await client.restartDeploymentRollout(
                from: sources,
                context: context,
                namespace: namespace,
                deploymentName: deploymentName
            )
            try await waitForPodTemplateAnnotationChange(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                deploymentName: deploymentName,
                key: "kubectl.kubernetes.io/restartedAt",
                previousValue: originalRestartAnnotation
            )
            try await waitForDeploymentReadyReplicas(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                deploymentName: deploymentName,
                expectedReady: originalReplicas
            )
            try await restoreDeploymentRestartAnnotation(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                deploymentName: deploymentName,
                value: originalRestartAnnotation
            )
            try await waitForDeploymentReadyReplicas(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                deploymentName: deploymentName,
                expectedReady: originalReplicas
            )

            let podToDelete = try await waitForReadyPodName(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                selector: "app=\(deploymentName)"
            )
            try await client.deleteResource(from: sources, context: context, namespace: namespace, kind: .pod, name: podToDelete)
            try await waitForPodReplacement(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context.name,
                namespace: namespace,
                selector: "app=\(deploymentName)",
                deletedPodName: podToDelete
            )

            await runCleanup(cleanup)
        } catch {
            await runCleanup(cleanup)
            throw error
        }
    }

    private func requireIntegrationEnabled() throws {
        guard ProcessInfo.processInfo.environment[integrationFlag] == "1" else {
            throw XCTSkip("Set \(integrationFlag)=1 to run local-only Kubernetes integration tests.")
        }
    }

    private func locateRuneFakeK8sBinary() throws -> URL {
        if let explicit = ProcessInfo.processInfo.environment["RUNE_FAKE_K8S_BINARY"], !explicit.isEmpty {
            let url = URL(fileURLWithPath: explicit)
            guard FileManager.default.isExecutableFile(atPath: url.path) else {
                throw LocalKubernetesIntegrationSafetyError.unsafeKubeconfig("RUNE_FAKE_K8S_BINARY is not executable: \(url.path)")
            }
            return url
        }

        let candidates = [
            repoRoot.appendingPathComponent(".build/debug/RuneFakeK8s"),
            repoRoot.appendingPathComponent(".build/arm64-apple-macosx/debug/RuneFakeK8s")
        ]
        for candidate in candidates where FileManager.default.isExecutableFile(atPath: candidate.path) {
            return candidate
        }

        throw XCTSkip("Build the local fake cluster first: swift build --product RuneFakeK8s")
    }

    private func assertRuneFakeKubeconfigIsSafe(_ kubeconfig: URL) throws {
        let raw = try String(contentsOf: kubeconfig, encoding: .utf8)
        guard kubeconfig.path.contains("rune-fake-k8s-integration") || kubeconfig.path.contains(".rune-fake-k8s") else {
            throw LocalKubernetesIntegrationSafetyError.unsafeKubeconfig("refusing unexpected fake-k8s kubeconfig path: \(kubeconfig.path)")
        }
        guard raw.contains("name: fake-orbit-mesh"), raw.contains("name: fake-lattice-spark") else {
            throw LocalKubernetesIntegrationSafetyError.unsafeKubeconfig("refusing fake-k8s kubeconfig without both fake contexts")
        }
        try assertAllKubeconfigServers(in: raw) { url in
            url.host?.hasSuffix(".fake.rune.local") == true
        }
    }

    private func assertDockerComposeKubeconfigIsSafe(_ kubeconfig: URL) throws {
        let raw = try String(contentsOf: kubeconfig, encoding: .utf8)
        guard kubeconfig.path.hasSuffix("docker-compose/generated/rune-fake-kubeconfig.yaml") else {
            throw LocalKubernetesIntegrationSafetyError.unsafeKubeconfig("refusing unexpected docker compose kubeconfig path: \(kubeconfig.path)")
        }
        guard raw.contains("name: fake-orbit-mesh"), raw.contains("name: fake-lattice-spark") else {
            throw LocalKubernetesIntegrationSafetyError.unsafeKubeconfig("refusing docker compose kubeconfig without both fake contexts")
        }
        try assertAllKubeconfigServers(in: raw) { url in
            guard url.scheme == "https", url.host == "127.0.0.1" else { return false }
            return url.port == 16443 || url.port == 17443
        }
    }

    private func configMapManifest(name: String, namespace: String, value: String) -> String {
        """
        apiVersion: v1
        kind: ConfigMap
        metadata:
          name: \(name)
          namespace: \(namespace)
          labels:
            rune.dev/integration: "true"
        data:
          value: "\(value)"
        """
    }

    private func assertAllKubeconfigServers(
        in raw: String,
        isAllowed: (URL) -> Bool
    ) throws {
        let serverLines = raw
            .split(whereSeparator: \.isNewline)
            .map(String.init)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { $0.hasPrefix("server:") }

        guard !serverLines.isEmpty else {
            throw LocalKubernetesIntegrationSafetyError.unsafeKubeconfig("refusing kubeconfig without explicit server entries")
        }

        for line in serverLines {
            let value = line.replacingOccurrences(of: "server:", with: "")
                .trimmingCharacters(in: .whitespacesAndNewlines)
            let url = try XCTUnwrap(URL(string: value), "Invalid kubeconfig server URL: \(value)")
            guard isAllowed(url) else {
                throw LocalKubernetesIntegrationSafetyError.unsafeKubeconfig("refusing non-local/non-fake kubeconfig server: \(value)")
            }
        }
    }

    private func assertClusterReady(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String
    ) async throws {
        do {
            let result = try await runKubectl(
                runner: runner,
                kubeconfig: kubeconfig,
                arguments: ["--context", context, "get", "--raw", "/readyz"],
                timeout: 10
            )
            XCTAssertTrue(result.stdout.contains("ok") || result.stdout.isEmpty)
        } catch {
            throw XCTSkip("Docker compose fake cluster \(context) is not ready: \(error)")
        }
    }

    private func assertPodEventTargetsResolve(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String
    ) async throws {
        let podNames = try await resourceNames(
            runner: runner,
            kubeconfig: kubeconfig,
            context: context,
            namespace: namespace,
            resource: "pods"
        )
        XCTAssertFalse(podNames.isEmpty, "\(context)/\(namespace) should have pods")

        let events = try await events(
            runner: runner,
            kubeconfig: kubeconfig,
            context: context,
            namespace: namespace
        )
        XCTAssertFalse(events.isEmpty, "\(context)/\(namespace) should have events")

        for event in events where event.involvedKind?.lowercased() == "pod" {
            XCTAssertTrue(
                podNames.contains(event.objectName),
                "Event \(event.reason) points at missing pod \(event.objectName) in \(context)/\(namespace)"
            )
        }
    }

    private func assertServiceSelectorsResolvePods(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String
    ) async throws {
        let deadline = Date().addingTimeInterval(90)
        var lastMissing: [String] = []

        repeat {
            let servicesJSON = try await kubectlJSON(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context,
                namespace: namespace,
                resource: "services"
            )
            let services = items(in: servicesJSON)
            lastMissing = services.isEmpty ? ["\(context)/\(namespace) should have services"] : []

            for service in services {
                guard let metadata = service["metadata"] as? [String: Any],
                      let name = metadata["name"] as? String,
                      let spec = service["spec"] as? [String: Any],
                      let selector = spec["selector"] as? [String: Any],
                      !selector.isEmpty
                else { continue }

                let selectorText = selector
                    .compactMap { key, value -> String? in
                        guard let value = value as? String else { return nil }
                        return "\(key)=\(value)"
                    }
                    .sorted()
                    .joined(separator: ",")
                let pods = try await kubectlJSON(
                    runner: runner,
                    kubeconfig: kubeconfig,
                    context: context,
                    namespace: namespace,
                    resource: "pods",
                    extraArguments: ["-l", selectorText]
                )
                if items(in: pods).isEmpty {
                    lastMissing.append("Service \(context)/\(namespace)/\(name) selector \(selectorText) should match at least one pod")
                }
            }

            if lastMissing.isEmpty {
                return
            }

            try await Task.sleep(nanoseconds: 2_000_000_000)
        } while Date() < deadline

        XCTFail("Timed out waiting for service selectors to resolve:\n" + lastMissing.joined(separator: "\n"))
    }

    private func assertEventInvolvedObjectsResolve(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String
    ) async throws {
        let listedEvents = try await waitForEvents(
            runner: runner,
            kubeconfig: kubeconfig,
            context: context,
            namespace: namespace
        )

        let cronJobNames = try await resourceNames(
            runner: runner,
            kubeconfig: kubeconfig,
            context: context,
            namespace: namespace,
            resource: "cronjobs"
        )
        var missingTargets: [String] = []
        for event in listedEvents.prefix(50) {
            guard let kind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines),
                  !kind.isEmpty,
                  event.objectName != "-"
            else { continue }

            guard let resource = kubectlResourceName(forInvolvedKind: kind) else {
                XCTFail("Event kind \(kind) is not mapped for Go to Resource validation")
                continue
            }

            let targetNamespace = event.involvedNamespace?.trimmingCharacters(in: .whitespacesAndNewlines)
            let exists = try await kubectlObjectExists(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context,
                namespace: targetNamespace,
                resource: resource,
                name: event.objectName
            )
            if !exists {
                if isAllowedStaleCronJobEvent(kind: kind, name: event.objectName, cronJobNames: cronJobNames) {
                    continue
                }
                missingTargets.append("\(context)/\(targetNamespace ?? "-") \(kind) \(event.objectName) from event \(event.reason)")
            }
        }

        XCTAssertTrue(
            missingTargets.isEmpty,
            "Events point at missing resources, so Go to Resource would fail:\n" + missingTargets.joined(separator: "\n")
        )
    }

    private func waitForEvents(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String
    ) async throws -> [EventSummary] {
        let deadline = Date().addingTimeInterval(90)

        repeat {
            let listedEvents = try await events(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context,
                namespace: namespace
            )
            if !listedEvents.isEmpty {
                return listedEvents
            }
            try await Task.sleep(nanoseconds: 2_000_000_000)
        } while Date() < deadline

        throw XCTSkip("\(context)/\(namespace) has no events to validate after waiting for the fake cluster to settle.")
    }

    private func isAllowedStaleCronJobEvent(kind: String, name: String, cronJobNames: Set<String>) -> Bool {
        guard kind.caseInsensitiveCompare("job") == .orderedSame || kind.caseInsensitiveCompare("pod") == .orderedSame else {
            return false
        }
        return cronJobNames.contains { cronJobName in
            name.hasPrefix(cronJobName + "-")
        }
    }

    private func kubectlResourceName(forInvolvedKind kind: String) -> String? {
        switch kind.lowercased() {
        case "pod": return "pods"
        case "deployment": return "deployments"
        case "replicaset": return "replicasets"
        case "statefulset": return "statefulsets"
        case "daemonset": return "daemonsets"
        case "job": return "jobs"
        case "cronjob": return "cronjobs"
        case "service": return "services"
        case "ingress": return "ingresses"
        case "configmap": return "configmaps"
        case "secret": return "secrets"
        case "node": return "nodes"
        case "persistentvolumeclaim": return "persistentvolumeclaims"
        case "persistentvolume": return "persistentvolumes"
        case "storageclass": return "storageclasses"
        case "horizontalpodautoscaler": return "horizontalpodautoscalers"
        case "networkpolicy": return "networkpolicies"
        default: return nil
        }
    }

    private func kubectlGetNamedArguments(
        context: String,
        namespace: String?,
        resource: String,
        name: String
    ) -> [String] {
        var args = ["--context", context, "get", resource, name]
        if let namespace, !namespace.isEmpty, !isClusterScoped(resource) {
            args.append(contentsOf: ["-n", namespace])
        }
        args.append(contentsOf: ["-o", "json"])
        return args
    }

    private func isClusterScoped(_ resource: String) -> Bool {
        ["nodes", "persistentvolumes", "storageclasses"].contains(resource)
    }

    private func resourceNames(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String,
        resource: String
    ) async throws -> Set<String> {
        let json = try await kubectlJSON(
            runner: runner,
            kubeconfig: kubeconfig,
            context: context,
            namespace: namespace,
            resource: resource
        )
        return Set(items(in: json).compactMap { item in
            (item["metadata"] as? [String: Any])?["name"] as? String
        })
    }

    private func events(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String
    ) async throws -> [EventSummary] {
        let result = try await runKubectl(
            runner: runner,
            kubeconfig: kubeconfig,
            arguments: ["--context", context, "get", "events", "-n", namespace, "-o", "json"],
            timeout: 20
        )
        return try KubectlOutputParser().parseEvents(from: result.stdout)
    }

    private func kubectlJSON(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String,
        resource: String,
        extraArguments: [String] = []
    ) async throws -> [String: Any] {
        let result = try await runKubectl(
            runner: runner,
            kubeconfig: kubeconfig,
            arguments: ["--context", context, "get", resource, "-n", namespace] + extraArguments + ["-o", "json"],
            timeout: 20
        )
        let object = try JSONSerialization.jsonObject(with: Data(result.stdout.utf8))
        return try XCTUnwrap(object as? [String: Any])
    }

    private func namedResourceJSON(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String?,
        resource: String,
        name: String
    ) async throws -> [String: Any] {
        let result = try await runKubectl(
            runner: runner,
            kubeconfig: kubeconfig,
            arguments: kubectlGetNamedArguments(
                context: context,
                namespace: namespace,
                resource: resource,
                name: name
            ),
            timeout: 20
        )
        let object = try JSONSerialization.jsonObject(with: Data(result.stdout.utf8))
        return try XCTUnwrap(object as? [String: Any])
    }

    private func waitForNamedResource(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String?,
        resource: String,
        name: String,
        timeout: TimeInterval = 60
    ) async throws -> [String: Any] {
        let deadline = Date().addingTimeInterval(timeout)
        var lastError: Error?

        repeat {
            do {
                return try await namedResourceJSON(
                    runner: runner,
                    kubeconfig: kubeconfig,
                    context: context,
                    namespace: namespace,
                    resource: resource,
                    name: name
                )
            } catch {
                lastError = error
                try await Task.sleep(nanoseconds: 1_000_000_000)
            }
        } while Date() < deadline

        throw lastError ?? LocalKubernetesIntegrationSafetyError.unsafeKubeconfig("Timed out waiting for \(resource)/\(name)")
    }

    private func waitForReadyPodName(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String,
        selector: String,
        timeout: TimeInterval = 90
    ) async throws -> String {
        let deadline = Date().addingTimeInterval(timeout)

        repeat {
            let podsJSON = try await kubectlJSON(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context,
                namespace: namespace,
                resource: "pods",
                extraArguments: ["-l", selector]
            )
            for pod in items(in: podsJSON) {
                guard let metadata = pod["metadata"] as? [String: Any],
                      let name = metadata["name"] as? String,
                      let status = pod["status"] as? [String: Any],
                      (status["phase"] as? String) == "Running"
                else { continue }

                let statuses = status["containerStatuses"] as? [[String: Any]] ?? []
                if statuses.isEmpty || statuses.allSatisfy({ ($0["ready"] as? Bool) == true }) {
                    return name
                }
            }
            try await Task.sleep(nanoseconds: 1_000_000_000)
        } while Date() < deadline

        throw LocalKubernetesIntegrationSafetyError.unsafeKubeconfig("Timed out waiting for ready pod matching \(selector) in \(context)/\(namespace)")
    }

    private func waitForPodReplacement(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String,
        selector: String,
        deletedPodName: String,
        timeout: TimeInterval = 90
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)

        repeat {
            let podsJSON = try await kubectlJSON(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context,
                namespace: namespace,
                resource: "pods",
                extraArguments: ["-l", selector]
            )
            let readyPods = items(in: podsJSON).filter { pod in
                guard let metadata = pod["metadata"] as? [String: Any],
                      let name = metadata["name"] as? String,
                      name != deletedPodName,
                      let status = pod["status"] as? [String: Any],
                      (status["phase"] as? String) == "Running"
                else { return false }

                let statuses = status["containerStatuses"] as? [[String: Any]] ?? []
                return statuses.isEmpty || statuses.allSatisfy { ($0["ready"] as? Bool) == true }
            }
            if !readyPods.isEmpty {
                let deletedStillExists = items(in: podsJSON).contains { pod in
                    (pod["metadata"] as? [String: Any])?["name"] as? String == deletedPodName
                }
                if !deletedStillExists {
                    return
                }
            }
            try await Task.sleep(nanoseconds: 1_000_000_000)
        } while Date() < deadline

        throw LocalKubernetesIntegrationSafetyError.unsafeKubeconfig("Timed out waiting for pod replacement after deleting \(deletedPodName)")
    }

    private func waitForSpecBool(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String,
        resource: String,
        name: String,
        key: String,
        expected: Bool,
        timeout: TimeInterval = 60
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)

        repeat {
            let object = try await namedResourceJSON(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context,
                namespace: namespace,
                resource: resource,
                name: name
            )
            if specBool(in: object, key: key) == expected {
                return
            }
            try await Task.sleep(nanoseconds: 1_000_000_000)
        } while Date() < deadline

        XCTFail("Timed out waiting for \(resource)/\(name) spec.\(key) == \(expected)")
    }

    private func waitForSpecInt(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String,
        resource: String,
        name: String,
        key: String,
        expected: Int,
        timeout: TimeInterval = 60
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)

        repeat {
            let object = try await namedResourceJSON(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context,
                namespace: namespace,
                resource: resource,
                name: name
            )
            if specInt(in: object, key: key) == expected {
                return
            }
            try await Task.sleep(nanoseconds: 1_000_000_000)
        } while Date() < deadline

        XCTFail("Timed out waiting for \(resource)/\(name) spec.\(key) == \(expected)")
    }

    private func waitForDeploymentReadyReplicas(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String,
        deploymentName: String,
        expectedReady: Int,
        timeout: TimeInterval = 120
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)

        repeat {
            let object = try await namedResourceJSON(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context,
                namespace: namespace,
                resource: "deployments",
                name: deploymentName
            )
            let desired = specInt(in: object, key: "replicas") ?? 0
            let status = object["status"] as? [String: Any]
            let ready = status?["readyReplicas"] as? Int ?? 0
            if desired == expectedReady, ready == expectedReady {
                return
            }
            try await Task.sleep(nanoseconds: 2_000_000_000)
        } while Date() < deadline

        XCTFail("Timed out waiting for deployment/\(deploymentName) to be ready with \(expectedReady) replicas")
    }

    private func waitForPodTemplateAnnotationChange(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String,
        deploymentName: String,
        key: String,
        previousValue: String?,
        timeout: TimeInterval = 60
    ) async throws {
        let deadline = Date().addingTimeInterval(timeout)

        repeat {
            let deployment = try await namedResourceJSON(
                runner: runner,
                kubeconfig: kubeconfig,
                context: context,
                namespace: namespace,
                resource: "deployments",
                name: deploymentName
            )
            let nextValue = podTemplateAnnotation(in: deployment, key: key)
            if let nextValue, !nextValue.isEmpty, nextValue != previousValue {
                return
            }
            try await Task.sleep(nanoseconds: 1_000_000_000)
        } while Date() < deadline

        XCTFail("Timed out waiting for deployment/\(deploymentName) pod template annotation \(key) to change")
    }

    private func restoreDeploymentRestartAnnotation(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String,
        deploymentName: String,
        value: String?
    ) async throws {
        let deployment = try await namedResourceJSON(
            runner: runner,
            kubeconfig: kubeconfig,
            context: context,
            namespace: namespace,
            resource: "deployments",
            name: deploymentName
        )
        if podTemplateAnnotation(in: deployment, key: "kubectl.kubernetes.io/restartedAt") == value {
            return
        }

        let patch: String
        let type: String
        if let value {
            type = "merge"
            patch = #"{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":""# + value + #""}}}}}"#
        } else {
            type = "json"
            patch = #"[{"op":"remove","path":"/spec/template/metadata/annotations/kubectl.kubernetes.io~1restartedAt"}]"#
        }
        _ = try await runKubectl(
            runner: runner,
            kubeconfig: kubeconfig,
            arguments: [
                "--context", context,
                "patch", "deployment", deploymentName,
                "-n", namespace,
                "--type=\(type)",
                "-p", patch
            ],
            timeout: 20
        )
    }

    private func dataValue(in object: [String: Any], key: String) -> String? {
        (object["data"] as? [String: Any])?[key] as? String
    }

    private func specBool(in object: [String: Any], key: String) -> Bool? {
        (object["spec"] as? [String: Any])?[key] as? Bool
    }

    private func specInt(in object: [String: Any], key: String) -> Int? {
        (object["spec"] as? [String: Any])?[key] as? Int
    }

    private func podTemplateAnnotation(in object: [String: Any], key: String) -> String? {
        guard let spec = object["spec"] as? [String: Any],
              let template = spec["template"] as? [String: Any],
              let metadata = template["metadata"] as? [String: Any],
              let annotations = metadata["annotations"] as? [String: Any]
        else { return nil }
        return annotations[key] as? String
    }

    private func items(in list: [String: Any]) -> [[String: Any]] {
        list["items"] as? [[String: Any]] ?? []
    }

    private func runKubectl(
        runner: CommandRunning,
        kubeconfig: URL,
        arguments: [String],
        timeout: TimeInterval
    ) async throws -> CommandResult {
        let result = try await runner.run(
            executable: "/usr/bin/env",
            arguments: ["kubectl"] + arguments,
            environment: ["KUBECONFIG": kubeconfig.path],
            timeout: timeout
        )
        guard result.exitCode == 0 else {
            throw LocalKubernetesIntegrationCommandError.nonZeroExit(
                command: "kubectl " + arguments.joined(separator: " "),
                exitCode: result.exitCode,
                stderr: result.stderr
            )
        }
        return result
    }

    private func kubectlObjectExists(
        runner: CommandRunning,
        kubeconfig: URL,
        context: String,
        namespace: String?,
        resource: String,
        name: String
    ) async throws -> Bool {
        let result = try await runner.run(
            executable: "/usr/bin/env",
            arguments: ["kubectl"] + kubectlGetNamedArguments(
                context: context,
                namespace: namespace,
                resource: resource,
                name: name
            ),
            environment: ["KUBECONFIG": kubeconfig.path],
            timeout: 10
        )
        return result.exitCode == 0
    }

    private func runCleanup(_ cleanup: [() async -> Void]) async {
        for action in cleanup.reversed() {
            await action()
        }
    }
}

private final class EnvironmentOverlayRunner: CommandRunning, @unchecked Sendable {
    private let base = ProcessCommandRunner()
    private let overlay: [String: String]

    init(overlay: [String: String]) {
        self.overlay = overlay
    }

    func run(
        executable: String,
        arguments: [String],
        environment: [String: String],
        timeout: TimeInterval?
    ) async throws -> CommandResult {
        var merged = environment
        overlay.forEach { key, value in
            merged[key] = value
        }
        return try await base.run(
            executable: executable,
            arguments: arguments,
            environment: merged,
            timeout: timeout
        )
    }
}

private enum LocalKubernetesIntegrationSafetyError: Error, CustomStringConvertible {
    case unsafeKubeconfig(String)

    var description: String {
        switch self {
        case let .unsafeKubeconfig(message):
            return message
        }
    }
}

private enum LocalKubernetesIntegrationCommandError: Error, CustomStringConvertible {
    case nonZeroExit(command: String, exitCode: Int32, stderr: String)

    var description: String {
        switch self {
        case let .nonZeroExit(command, exitCode, stderr):
            let detail = stderr.trimmingCharacters(in: .whitespacesAndNewlines)
            return "\(command) failed with exit \(exitCode)" + (detail.isEmpty ? "" : ": \(detail)")
        }
    }
}
