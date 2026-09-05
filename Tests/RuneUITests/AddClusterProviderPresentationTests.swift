import XCTest
@testable import RuneUI

final class AddClusterProviderPresentationTests: XCTestCase {
    func testOptionalCredentialsReuseClusterFieldsWithoutDuplicatingInputs() {
        for provider in [AddClusterProviderIdentifier.aks, .eks, .gke] {
            let primary = presentation(provider, mode: .externalCLI)
            let optional = presentation(provider, mode: .nativeOnly)
            let credentials = optional.credentialFields(excluding: primary.fields)
            let visibleIdentifiers = (primary.fields + credentials).map(\.id)

            XCTAssertEqual(visibleIdentifiers.count, Set(visibleIdentifiers).count)
            XCTAssertTrue(optional.fields.allSatisfy { visibleIdentifiers.contains($0.id) })
            XCTAssertFalse(credentials.contains { $0.id == .clusterName })
            XCTAssertEqual(credentials.filter { $0.input.isSensitive }.count, optional.fields.filter { $0.input.isSensitive }.count)
        }
    }

    func testSharedAzureSubscriptionExplainsBothImportRequirements() throws {
        let primary = presentation(.aks, mode: .externalCLI)
        let field = try XCTUnwrap(primary.fields.first { $0.id == .subscription })
        XCTAssertFalse(field.isRequired, "Azure CLI can use its active subscription.")
        XCTAssertEqual(field.title, "Subscription ID or name")
        XCTAssertEqual(field.helpText, "Optional for Azure CLI. Service-principal import requires a subscription ID.")
    }

    func testLocalCLISetupUsesInteractiveSignInAndClearlyMarksSSOProfileTemplate() {
        XCTAssertEqual(presentation(.aks, mode: .externalCLI).localCLISetupCommands, ["az login"])
        XCTAssertEqual(presentation(.eks, mode: .externalCLI).localCLISetupCommands, ["aws configure sso", "aws sso login --profile <profile>"])
        XCTAssertEqual(presentation(.gke, mode: .externalCLI).localCLISetupCommands, ["gcloud auth login"])
        for provider in [AddClusterProviderIdentifier.aks, .eks, .gke] {
            let value = presentation(provider, mode: .externalCLI)
            XCTAssertEqual(value.localCLISetupDocumentationURL?.scheme, "https")
            XCTAssertFalse(value.localCLISetupCommands.contains("aws configure"))
            XCTAssertTrue(value.localCLISetupDescription.localizedCaseInsensitiveContains("no Rune account or server"))
        }
        XCTAssertTrue(presentation(.local, mode: .externalCLI).localCLISetupCommands.isEmpty)
    }

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
        XCTAssertEqual(aks.subtitle, "Azure CLI · automatic kubeconfig")
        XCTAssertEqual(aks.fields.map(\.id), [.clusterName, .resourceGroup, .subscription])
        XCTAssertEqual(aks.fields.map(\.isRequired), [true, true, false])

        XCTAssertEqual(eks.title, "Amazon EKS")
        XCTAssertEqual(eks.subtitle, "AWS CLI · automatic kubeconfig")
        XCTAssertEqual(eks.fields.map(\.id), [.clusterName, .region, .profile, .roleARN])
        XCTAssertEqual(eks.fields.map(\.isRequired), [true, true, false, false])

        XCTAssertEqual(gke.title, "Google GKE")
        XCTAssertEqual(gke.subtitle, "Google Cloud CLI · automatic kubeconfig")
        XCTAssertEqual(gke.fields.map(\.id), [.clusterName, .location, .projectID])
        XCTAssertTrue(gke.fields.allSatisfy(\.isRequired))

        for value in [aks, eks, gke] {
            XCTAssertEqual(value.primaryAction.id, .runExternalCLI)
            XCTAssertTrue(value.allowsExternalCommandExecution)
            XCTAssertTrue(value.exposesCLIOnlyActions)
            XCTAssertTrue(value.showsCommandDetails)
            XCTAssertFalse(value.requiresCompatibleImportedContext)
            XCTAssertEqual(value.compactSubtitle, "Automatic CLI setup")
            XCTAssertEqual(value.primaryAction.title, "Add cluster")
            XCTAssertEqual(
                value.utilityActions.map(\.id),
                [.importKubeconfig, .copyExternalCommand, .refreshContexts, .runAuthDoctor]
            )
            XCTAssertEqual(value.utilityActions.first?.title, "Import file…")
        }
    }

    func testNativeOnlyPresentationsExposeProviderSpecificCredentialActions() {
        let aks = presentation(.aks, mode: .nativeOnly)
        let eks = presentation(.eks, mode: .nativeOnly)
        let gke = presentation(.gke, mode: .nativeOnly)

        XCTAssertEqual(aks.subtitle, "Kubeconfig and optional cloud access")
        XCTAssertEqual(
            aks.fields.map(\.id),
            [.clusterName, .resourceGroup, .subscription, .azureTenantID, .azureClientID, .azureClientSecret]
        )
        XCTAssertTrue(aks.fields.allSatisfy { $0.requirement == .requiredForOptionalMethod })
        XCTAssertTrue(aks.fields.allSatisfy { !$0.isRequired })
        XCTAssertEqual(aks.primaryAction.id, .importKubeconfig)
        XCTAssertEqual(aks.primaryAction.title, "Import kubeconfig…")
        XCTAssertEqual(
            aks.utilityActions.map(\.id),
            [.runNativeImport, .refreshContexts, .runAuthDoctor]
        )
        XCTAssertEqual(aks.utilityActions.first?.title, "Import with service principal")

        XCTAssertEqual(eks.subtitle, "Kubeconfig and optional cloud access")
        XCTAssertEqual(
            eks.fields.map(\.id),
            [.clusterName, .region, .awsAccessKeyID, .awsSecretAccessKey, .awsSessionToken]
        )
        XCTAssertEqual(
            eks.fields.map(\.requirement),
            [.requiredForOptionalMethod, .requiredForOptionalMethod, .requiredForOptionalMethod, .requiredForOptionalMethod, .optional]
        )
        XCTAssertTrue(eks.fields.allSatisfy { !$0.isRequired })
        XCTAssertEqual(eks.primaryAction.id, .importKubeconfig)
        XCTAssertEqual(eks.primaryAction.title, "Import kubeconfig…")
        XCTAssertEqual(eks.utilityActions.map(\.id), [.runNativeImport, .refreshContexts, .runAuthDoctor])
        XCTAssertEqual(eks.utilityActions.first?.title, "Import with access keys")

        XCTAssertEqual(gke.subtitle, "Kubeconfig and optional cloud access")
        XCTAssertEqual(gke.fields.map(\.id), [.clusterName, .location, .projectID, .googleServiceAccountJSON])
        XCTAssertTrue(gke.fields.allSatisfy { $0.requirement == .requiredForOptionalMethod })
        XCTAssertTrue(gke.fields.allSatisfy { !$0.isRequired })
        XCTAssertEqual(gke.primaryAction.id, .importKubeconfig)
        XCTAssertEqual(gke.primaryAction.title, "Import kubeconfig…")
        XCTAssertEqual(gke.utilityActions.map(\.id), [.chooseServiceAccountJSON, .refreshContexts, .runAuthDoctor])
        XCTAssertEqual(gke.utilityActions.first?.title, "Import with service account…")

        for value in [aks, eks, gke] {
            XCTAssertFalse(value.allowsExternalCommandExecution)
            XCTAssertFalse(value.exposesCLIOnlyActions)
            XCTAssertFalse(value.showsCommandDetails)
            XCTAssertFalse(value.requiresCompatibleImportedContext)
            XCTAssertEqual(value.compactSubtitle, "Kubeconfig + auth")
        }
        XCTAssertEqual(aks.credentialSectionTitle, "Service principal")
        XCTAssertEqual(eks.credentialSectionTitle, "Access keys")
        XCTAssertEqual(gke.credentialSectionTitle, "Service account")
        XCTAssertTrue(aks.credentialSectionDescription.contains("optional method"))
        XCTAssertTrue(eks.credentialSectionDescription.contains("session token"))
        XCTAssertTrue(gke.credentialSectionDescription.contains("service-account JSON"))
    }

    func testNativeAddFlowDoesNotMixExistingProfileDisconnectionIntoImport() {
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
        XCTAssertFalse(connected.utilityActions.contains { $0.id == .disconnectNativeCredentials })
    }

    func testNativeOnlyKeepsEachProvidersUsableDefaultImportPrimary() {
        for provider in [
            AddClusterProviderIdentifier.aks,
            .eks,
            .gke
        ] {
            let value = presentation(provider, mode: .nativeOnly)
            let initialPrimary = value.primaryAction(hasCompatibleImportedContext: false)
            let initialUtilities = value.utilityActions(hasCompatibleImportedContext: false)

            XCTAssertEqual(initialPrimary, value.primaryAction)
            let expectedUtilities: [AddClusterProviderActionIdentifier]
            switch provider {
            case .aks, .eks:
                expectedUtilities = [.runNativeImport, .refreshContexts, .runAuthDoctor]
            case .gke:
                expectedUtilities = [.chooseServiceAccountJSON, .refreshContexts, .runAuthDoctor]
            case .local:
                expectedUtilities = []
            }
            XCTAssertEqual(initialUtilities.map(\.id), expectedUtilities)

            let availablePrimary = value.primaryAction(hasCompatibleImportedContext: true)
            let availableUtilities = value.utilityActions(hasCompatibleImportedContext: true)

            XCTAssertEqual(availablePrimary, value.primaryAction)
            XCTAssertEqual(availableUtilities.map(\.id), expectedUtilities)
            XCTAssertEqual(availablePrimary.id, .importKubeconfig)
            XCTAssertNotEqual(availableUtilities.first?.id, .importKubeconfig)
        }
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
        XCTAssertEqual(nativeOnly.compactSubtitle, "Local kubeconfig")
    }

    func testNativeCredentialFieldsCarryOnlyInputSemantics() throws {
        let eks = presentation(.eks, mode: .nativeOnly)
        let accessKey = try XCTUnwrap(eks.fields.first { $0.id == .awsAccessKeyID })
        let secretKey = try XCTUnwrap(eks.fields.first { $0.id == .awsSecretAccessKey })
        let sessionToken = try XCTUnwrap(eks.fields.first { $0.id == .awsSessionToken })
        let azureSecret = try XCTUnwrap(
            presentation(.aks, mode: .nativeOnly).fields.first { $0.id == .azureClientSecret }
        )
        let serviceAccount = try XCTUnwrap(
            presentation(.gke, mode: .nativeOnly).fields.first { $0.id == .googleServiceAccountJSON }
        )

        XCTAssertEqual(accessKey.input, .text)
        XCTAssertFalse(accessKey.input.isSensitive)
        XCTAssertFalse(accessKey.isRequired)
        XCTAssertEqual(accessKey.requirement, .requiredForOptionalMethod)

        XCTAssertEqual(secretKey.input, .secureText)
        XCTAssertTrue(secretKey.input.isSensitive)
        XCTAssertFalse(secretKey.isRequired)

        XCTAssertEqual(sessionToken.input, .secureText)
        XCTAssertTrue(sessionToken.input.isSensitive)
        XCTAssertFalse(sessionToken.isRequired)

        XCTAssertEqual(azureSecret.input, .secureText)
        XCTAssertTrue(azureSecret.input.isSensitive)
        XCTAssertEqual(azureSecret.requirement, .requiredForOptionalMethod)
        XCTAssertEqual(serviceAccount.input, .sensitiveJSONFile)
        XCTAssertTrue(serviceAccount.input.isSensitive)
        XCTAssertEqual(serviceAccount.requirement, .requiredForOptionalMethod)
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
