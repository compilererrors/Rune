import XCTest
@testable import RuneSecurity
@testable import RuneUI

final class AddClusterNativeContextResolverTests: XCTestCase {
    @MainActor
    func testLatestRequestGateRejectsOlderSameSheetCompletion() throws {
        let gate = RuneScopedLatestRequestGate()
        let scope = gate.advanceScope()
        let requests = try (0..<64).map { _ in
            try XCTUnwrap(gate.begin(expectedScopeGeneration: scope))
        }

        XCTAssertTrue(requests.dropLast().allSatisfy { !gate.isCurrent($0) })
        XCTAssertTrue(gate.isCurrent(try XCTUnwrap(requests.last)))
    }

    @MainActor
    func testLatestRequestGateRejectsDelayedWorkAfterSameProviderReopens() throws {
        let gate = RuneScopedLatestRequestGate()
        let firstSheetScope = gate.advanceScope()
        let firstSheetRequest = try XCTUnwrap(
            gate.begin(expectedScopeGeneration: firstSheetScope)
        )

        let reopenedSheetScope = gate.advanceScope()

        XCTAssertNil(gate.begin(expectedScopeGeneration: firstSheetScope))
        XCTAssertFalse(gate.isCurrent(firstSheetRequest))
        XCTAssertNotNil(gate.begin(expectedScopeGeneration: reopenedSheetScope))
    }

    func testCurrentCompatibleContextWinsWhenProviderHasMultipleOptions() throws {
        let first = descriptor(context: "synthetic-eks-a", provider: .awsEKS)
        let current = descriptor(context: "synthetic-eks-b", provider: .awsEKS)
        let analysis = makeAnalysis(contexts: [first, current])

        let result = AddClusterNativeContextResolver.resolve(
            provider: .awsEKS,
            analysis: analysis,
            currentContextName: current.contextName
        )

        guard case let .selected(option) = result else {
            return XCTFail("Expected the compatible current context to be selected")
        }
        XCTAssertEqual(option.request, current.credentialRequest)
    }

    func testKubeconfigCurrentContextIsUsedWhenNoOverrideIsProvided() {
        let first = descriptor(context: "synthetic-aks-a", provider: .azureKubelogin)
        let current = descriptor(context: "synthetic-aks-b", provider: .azureKubelogin)
        let analysis = makeAnalysis(currentContext: current.contextName, contexts: [first, current])

        let result = AddClusterNativeContextResolver.resolve(
            provider: .azureKubelogin,
            analysis: analysis
        )

        guard case let .selected(option) = result else {
            return XCTFail("Expected kubeconfig's compatible current context to be selected")
        }
        XCTAssertEqual(option.contextName, current.contextName)
    }

    func testSingleCompatibleContextIsSelectedAutomatically() {
        let compatible = descriptor(context: "synthetic-gke", provider: .googleGKE)
        let analysis = makeAnalysis(contexts: [
            descriptor(context: "synthetic-eks", provider: .awsEKS),
            compatible
        ])

        let result = AddClusterNativeContextResolver.resolve(
            provider: .googleGKE,
            analysis: analysis,
            currentContextName: "synthetic-eks"
        )

        XCTAssertEqual(
            result,
            .selected(AddClusterNativeContextOption(request: compatible.credentialRequest!))
        )
    }

    func testMultipleCompatibleContextsRequireExplicitChoice() {
        let first = descriptor(context: "synthetic-eks-a", provider: .awsEKS)
        let second = descriptor(context: "synthetic-eks-b", provider: .awsEKS)
        let analysis = makeAnalysis(contexts: [first, second])

        let result = AddClusterNativeContextResolver.resolve(
            provider: .awsEKS,
            analysis: analysis,
            currentContextName: "synthetic-unrelated"
        )

        guard case let .requiresChoice(options) = result else {
            return XCTFail("Expected ambiguous compatible contexts to require a choice")
        }
        XCTAssertEqual(options.map(\.contextName), [first.contextName, second.contextName])
    }

    func testResolutionExcludesOtherProvidersOIDCAndContextsWithoutNativeRequests() {
        let compatible = descriptor(context: "synthetic-eks", provider: .awsEKS)
        let analysis = makeAnalysis(contexts: [
            descriptor(context: "synthetic-aks", provider: .azureKubelogin),
            descriptor(context: "synthetic-gke", provider: .googleGKE),
            descriptor(context: "synthetic-oidc", provider: .oidc),
            descriptor(context: "synthetic-static", provider: nil),
            compatible
        ])

        let result = AddClusterNativeContextResolver.resolve(
            provider: .awsEKS,
            analysis: analysis
        )

        XCTAssertEqual(
            result,
            .selected(AddClusterNativeContextOption(request: compatible.credentialRequest!))
        )
        XCTAssertEqual(
            AddClusterNativeContextResolver.resolve(provider: .oidc, analysis: analysis),
            .unavailable
        )
    }

    func testNoCompatibleContextIsUnavailable() {
        let analysis = makeAnalysis(contexts: [
            descriptor(context: "synthetic-static", provider: nil),
            descriptor(context: "synthetic-oidc", provider: .oidc)
        ])

        XCTAssertEqual(
            AddClusterNativeContextResolver.resolve(provider: .googleGKE, analysis: analysis),
            .unavailable
        )
    }

    private func makeAnalysis(
        currentContext: String? = nil,
        contexts: [KubernetesNativeAuthContextDescriptor]
    ) -> KubeConfigNativeAuthAnalysis {
        KubeConfigNativeAuthAnalysis(
            currentContext: currentContext,
            contexts: contexts,
            issues: []
        )
    }

    private func descriptor(
        context: String,
        provider: KubernetesNativeAuthProviderKind?
    ) -> KubernetesNativeAuthContextDescriptor {
        KubernetesNativeAuthContextDescriptor(
            contextName: context,
            clusterName: "synthetic-cluster-\(context)",
            userName: "synthetic-user-\(context)",
            namespace: nil,
            cluster: KubernetesNativeAuthClusterDescriptor(
                name: "synthetic-cluster-\(context)",
                server: "https://cluster.example.invalid"
            ),
            exec: provider == nil ? nil : KubernetesNativeAuthExecDescriptor(command: "synthetic-auth-plugin"),
            authProvider: nil,
            provider: provider,
            bindingID: provider == nil ? nil : "synthetic-binding-\(context)"
        )
    }
}
