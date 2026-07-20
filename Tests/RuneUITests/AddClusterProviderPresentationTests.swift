import XCTest
@testable import RuneUI

final class AddClusterProviderPresentationTests: XCTestCase {
    func testExecutionModeUsesCapabilityInsteadOfDistributionLabel() {
        XCTAssertEqual(
            AddClusterProviderExecutionMode(externalCommandsAllowed: true),
            .externalCLI
        )
        XCTAssertEqual(
            AddClusterProviderExecutionMode(externalCommandsAllowed: false),
            .nativeOnly
        )
    }

    func testExternalCLIPresentationsExposeProviderSpecificFieldsAndRunAction() {
        let aks = presentation(.aks, mode: .externalCLI)
        let eks = presentation(.eks, mode: .externalCLI)
        let gke = presentation(.gke, mode: .externalCLI)

        XCTAssertEqual(aks.title, "Microsoft AKS")
        XCTAssertEqual(aks.subtitle, "Azure CLI")
        XCTAssertEqual(aks.fields.map(\.id), [.clusterName, .resourceGroup, .subscription])
        XCTAssertEqual(aks.fields.map(\.isRequired), [true, true, false])

        XCTAssertEqual(eks.title, "Amazon EKS")
        XCTAssertEqual(eks.subtitle, "AWS CLI")
        XCTAssertEqual(eks.fields.map(\.id), [.clusterName, .region, .profile, .roleARN])
        XCTAssertEqual(eks.fields.map(\.isRequired), [true, true, false, false])

        XCTAssertEqual(gke.title, "Google GKE")
        XCTAssertEqual(gke.subtitle, "Google Cloud CLI")
        XCTAssertEqual(gke.fields.map(\.id), [.clusterName, .location, .projectID])
        XCTAssertTrue(gke.fields.allSatisfy(\.isRequired))

        for value in [aks, eks, gke] {
            XCTAssertEqual(value.primaryAction.id, .runExternalCLI)
            XCTAssertTrue(value.allowsExternalCommandExecution)
            XCTAssertTrue(value.exposesCLIOnlyActions)
            XCTAssertTrue(value.showsCommandDetails)
            XCTAssertFalse(value.requiresCompatibleImportedContext)
            XCTAssertEqual(
                value.utilityActions.map(\.id),
                [.copyExternalCommand, .refreshContexts, .runAuthDoctor]
            )
        }
    }

    func testNativeOnlyPresentationsExposeProviderSpecificCredentialActions() {
        let aks = presentation(.aks, mode: .nativeOnly)
        let eks = presentation(.eks, mode: .nativeOnly)
        let gke = presentation(.gke, mode: .nativeOnly)

        XCTAssertEqual(aks.subtitle, "Native service-principal credentials")
        XCTAssertEqual(aks.fields.map(\.id), [.azureClientSecret])
        XCTAssertEqual(aks.primaryAction.id, .connectNativeCredentials)

        XCTAssertEqual(eks.subtitle, "Native AWS credentials")
        XCTAssertEqual(
            eks.fields.map(\.id),
            [.awsAccessKeyID, .awsSecretAccessKey, .awsSessionToken]
        )
        XCTAssertEqual(eks.primaryAction.id, .connectNativeCredentials)

        XCTAssertEqual(gke.subtitle, "Native service-account credentials")
        XCTAssertEqual(gke.fields.map(\.id), [.googleServiceAccountJSON])
        XCTAssertEqual(gke.primaryAction.id, .chooseServiceAccountJSON)
        XCTAssertEqual(gke.primaryAction.title, "Choose JSON…")

        for value in [aks, eks, gke] {
            XCTAssertFalse(value.allowsExternalCommandExecution)
            XCTAssertFalse(value.exposesCLIOnlyActions)
            XCTAssertFalse(value.showsCommandDetails)
            XCTAssertTrue(value.requiresCompatibleImportedContext)
            XCTAssertEqual(
                value.utilityActions.map(\.id),
                [.importKubeconfig, .refreshContexts, .runAuthDoctor]
            )
        }
    }

    func testNativeDisconnectAppearsOnlyForConnectedProviderProfile() {
        let disconnected = AddClusterProviderPresentation.resolve(
            provider: .eks,
            mode: .nativeOnly,
            isNativeProfileConnected: false
        )
        let connected = AddClusterProviderPresentation.resolve(
            provider: .eks,
            mode: .nativeOnly,
            isNativeProfileConnected: true
        )

        XCTAssertFalse(disconnected.utilityActions.contains { $0.id == .disconnectNativeCredentials })
        XCTAssertEqual(connected.utilityActions.last?.id, .disconnectNativeCredentials)
        XCTAssertTrue(connected.utilityActions.last?.isDestructive == true)
    }

    func testNativeOnlyPresentationNeverExposesCloudCLIOnlyActions() {
        for provider in [
            AddClusterProviderIdentifier.aks,
            .eks,
            .gke
        ] {
            let value = presentation(provider, mode: .nativeOnly)
            XCTAssertFalse(value.primaryAction.id.isCLIOnly)
            XCTAssertFalse(value.utilityActions.contains { $0.id.isCLIOnly })
            XCTAssertFalse(value.showsCommandDetails)
        }
    }

    func testLocalPresentationIsCapabilityIndependent() {
        let external = presentation(.local, mode: .externalCLI)
        let nativeOnly = presentation(.local, mode: .nativeOnly)

        XCTAssertEqual(external.title, nativeOnly.title)
        XCTAssertEqual(external.subtitle, nativeOnly.subtitle)
        XCTAssertEqual(external.note, nativeOnly.note)
        XCTAssertEqual(external.symbolName, nativeOnly.symbolName)
        XCTAssertEqual(external.fields, nativeOnly.fields)
        XCTAssertEqual(external.primaryAction, nativeOnly.primaryAction)
        XCTAssertEqual(external.utilityActions, nativeOnly.utilityActions)
        XCTAssertEqual(external.primaryAction.id, .importKubeconfig)
        XCTAssertEqual(
            external.utilityActions.map(\.id),
            [.copyLocalSetupCommand, .refreshContexts, .runAuthDoctor]
        )
        XCTAssertFalse(external.allowsExternalCommandExecution)
        XCTAssertFalse(nativeOnly.allowsExternalCommandExecution)
        XCTAssertFalse(external.exposesCLIOnlyActions)
        XCTAssertFalse(nativeOnly.exposesCLIOnlyActions)
        XCTAssertTrue(external.showsCommandDetails)
        XCTAssertTrue(nativeOnly.showsCommandDetails)
        XCTAssertFalse(external.requiresCompatibleImportedContext)
        XCTAssertFalse(nativeOnly.requiresCompatibleImportedContext)
    }

    func testNativeCredentialFieldsCarryOnlyInputSemantics() throws {
        let eks = presentation(.eks, mode: .nativeOnly)
        let accessKey = try XCTUnwrap(eks.fields.first { $0.id == .awsAccessKeyID })
        let secretKey = try XCTUnwrap(eks.fields.first { $0.id == .awsSecretAccessKey })
        let sessionToken = try XCTUnwrap(eks.fields.first { $0.id == .awsSessionToken })
        let azureSecret = try XCTUnwrap(
            presentation(.aks, mode: .nativeOnly).fields.first
        )
        let serviceAccount = try XCTUnwrap(
            presentation(.gke, mode: .nativeOnly).fields.first
        )

        XCTAssertEqual(accessKey.input, .text)
        XCTAssertFalse(accessKey.input.isSensitive)
        XCTAssertTrue(accessKey.isRequired)

        XCTAssertEqual(secretKey.input, .secureText)
        XCTAssertTrue(secretKey.input.isSensitive)
        XCTAssertTrue(secretKey.isRequired)

        XCTAssertEqual(sessionToken.input, .secureText)
        XCTAssertTrue(sessionToken.input.isSensitive)
        XCTAssertFalse(sessionToken.isRequired)

        XCTAssertEqual(azureSecret.input, .secureText)
        XCTAssertTrue(azureSecret.input.isSensitive)
        XCTAssertEqual(serviceAccount.input, .sensitiveJSONFile)
        XCTAssertTrue(serviceAccount.input.isSensitive)
    }

    func testCapabilityConvenienceResolverMatchesExplicitModeResolver() {
        for provider in AddClusterProviderIdentifier.allCases {
            XCTAssertEqual(
                AddClusterProviderPresentation.resolve(
                    provider: provider,
                    externalCommandsAllowed: true
                ),
                presentation(provider, mode: .externalCLI)
            )
            XCTAssertEqual(
                AddClusterProviderPresentation.resolve(
                    provider: provider,
                    externalCommandsAllowed: false
                ),
                presentation(provider, mode: .nativeOnly)
            )
        }
    }

    private func presentation(
        _ provider: AddClusterProviderIdentifier,
        mode: AddClusterProviderExecutionMode
    ) -> AddClusterProviderPresentation {
        AddClusterProviderPresentation.resolve(provider: provider, mode: mode)
    }
}
