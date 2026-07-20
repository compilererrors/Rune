import Foundation

enum AddClusterProviderIdentifier: String, CaseIterable, Identifiable, Sendable {
    case aks
    case eks
    case gke
    case local

    var id: String { rawValue }
}

enum AddClusterProviderExecutionMode: Sendable, Equatable {
    case externalCLI
    case nativeOnly

    init(externalCommandsAllowed: Bool) {
        self = externalCommandsAllowed ? .externalCLI : .nativeOnly
    }
}

enum AddClusterProviderFieldIdentifier: String, Sendable, Equatable {
    case clusterName
    case resourceGroup
    case subscription
    case region
    case profile
    case roleARN
    case location
    case projectID
    case awsAccessKeyID
    case awsSecretAccessKey
    case awsSessionToken
    case azureTenantID
    case azureClientID
    case azureClientSecret
    case googleServiceAccountJSON
}

enum AddClusterProviderFieldInput: Sendable, Equatable {
    case text
    case secureText
    case sensitiveJSONFile

    var isSensitive: Bool {
        switch self {
        case .text:
            return false
        case .secureText, .sensitiveJSONFile:
            return true
        }
    }
}

struct AddClusterProviderField: Sendable, Equatable, Identifiable {
    let id: AddClusterProviderFieldIdentifier
    let title: String
    let isRequired: Bool
    let input: AddClusterProviderFieldInput

    init(
        _ id: AddClusterProviderFieldIdentifier,
        title: String,
        isRequired: Bool,
        input: AddClusterProviderFieldInput = .text
    ) {
        self.id = id
        self.title = title
        self.isRequired = isRequired
        self.input = input
    }
}

enum AddClusterProviderActionIdentifier: String, Sendable, Equatable {
    case runExternalCLI
    case runNativeImport
    case connectNativeCredentials
    case chooseServiceAccountJSON
    case importKubeconfig
    case copyExternalCommand
    case copyLocalSetupCommand
    case refreshContexts
    case runAuthDoctor
    case disconnectNativeCredentials

    var isCLIOnly: Bool {
        switch self {
        case .runExternalCLI, .copyExternalCommand:
            return true
        case .runNativeImport,
             .connectNativeCredentials,
             .chooseServiceAccountJSON,
             .importKubeconfig,
             .copyLocalSetupCommand,
             .refreshContexts,
             .runAuthDoctor,
             .disconnectNativeCredentials:
            return false
        }
    }
}

struct AddClusterProviderAction: Sendable, Equatable, Identifiable {
    let id: AddClusterProviderActionIdentifier
    let title: String
    let systemImage: String
    let isDestructive: Bool

    init(
        _ id: AddClusterProviderActionIdentifier,
        title: String,
        systemImage: String,
        isDestructive: Bool = false
    ) {
        self.id = id
        self.title = title
        self.systemImage = systemImage
        self.isDestructive = isDestructive
    }
}

struct AddClusterProviderPresentation: Sendable, Equatable {
    let provider: AddClusterProviderIdentifier
    let executionMode: AddClusterProviderExecutionMode
    let title: String
    let shortTitle: String
    let subtitle: String
    let note: String
    let symbolName: String
    let fields: [AddClusterProviderField]
    let primaryAction: AddClusterProviderAction
    let utilityActions: [AddClusterProviderAction]
    let showsCommandDetails: Bool
    let requiresCompatibleImportedContext: Bool

    var allowsExternalCommandExecution: Bool {
        primaryAction.id == .runExternalCLI
    }

    var exposesCLIOnlyActions: Bool {
        primaryAction.id.isCLIOnly || utilityActions.contains { $0.id.isCLIOnly }
    }

    func primaryAction(hasCompatibleImportedContext: Bool) -> AddClusterProviderAction {
        guard executionMode == .nativeOnly,
              requiresCompatibleImportedContext,
              !hasCompatibleImportedContext else {
            return primaryAction
        }
        return AddClusterProviderAction(
            .importKubeconfig,
            title: "Import kubeconfig…",
            systemImage: "doc.badge.plus"
        )
    }

    func utilityActions(hasCompatibleImportedContext: Bool) -> [AddClusterProviderAction] {
        guard executionMode == .nativeOnly,
              requiresCompatibleImportedContext else {
            return utilityActions
        }
        if !hasCompatibleImportedContext {
            return utilityActions.filter { $0.id != .importKubeconfig }
        }
        return utilityActions.map { action in
            guard action.id == .importKubeconfig else { return action }
            return AddClusterProviderAction(
                .importKubeconfig,
                title: "Import another…",
                systemImage: action.systemImage
            )
        }
    }

    static func resolve(
        provider: AddClusterProviderIdentifier,
        externalCommandsAllowed: Bool,
        isNativeProfileConnected: Bool = false
    ) -> AddClusterProviderPresentation {
        resolve(
            provider: provider,
            mode: AddClusterProviderExecutionMode(externalCommandsAllowed: externalCommandsAllowed),
            isNativeProfileConnected: isNativeProfileConnected
        )
    }

    static func resolve(
        provider: AddClusterProviderIdentifier,
        mode: AddClusterProviderExecutionMode,
        isNativeProfileConnected: Bool = false
    ) -> AddClusterProviderPresentation {
        if provider == .local {
            return localPresentation(mode: mode)
        }

        switch mode {
        case .externalCLI:
            return externalCLIPresentation(provider: provider)
        case .nativeOnly:
            return nativePresentation(
                provider: provider,
                isNativeProfileConnected: isNativeProfileConnected
            )
        }
    }

    private static func externalCLIPresentation(
        provider: AddClusterProviderIdentifier
    ) -> AddClusterProviderPresentation {
        let fields: [AddClusterProviderField]
        let subtitle: String
        let note: String

        switch provider {
        case .aks:
            subtitle = "Azure CLI"
            note = "Runs Azure CLI locally, validates the resulting kubeconfig, and refreshes contexts."
            fields = [
                AddClusterProviderField(.clusterName, title: "Cluster name", isRequired: true),
                AddClusterProviderField(.resourceGroup, title: "Resource group", isRequired: true),
                AddClusterProviderField(.subscription, title: "Subscription ID or name", isRequired: false)
            ]
        case .eks:
            subtitle = "AWS CLI"
            note = "Runs AWS CLI locally, validates the resulting kubeconfig, and refreshes contexts."
            fields = [
                AddClusterProviderField(.clusterName, title: "Cluster name", isRequired: true),
                AddClusterProviderField(.region, title: "Region", isRequired: true),
                AddClusterProviderField(.profile, title: "AWS profile", isRequired: false),
                AddClusterProviderField(.roleARN, title: "Role ARN", isRequired: false)
            ]
        case .gke:
            subtitle = "Google Cloud CLI"
            note = "Runs Google Cloud CLI locally, validates the resulting kubeconfig, and refreshes contexts."
            fields = [
                AddClusterProviderField(.clusterName, title: "Cluster name", isRequired: true),
                AddClusterProviderField(.location, title: "Location, region or zone", isRequired: true),
                AddClusterProviderField(.projectID, title: "Project ID", isRequired: true)
            ]
        case .local:
            return localPresentation(mode: .externalCLI)
        }

        return AddClusterProviderPresentation(
            provider: provider,
            executionMode: .externalCLI,
            title: provider.title,
            shortTitle: provider.shortTitle,
            subtitle: subtitle,
            note: note,
            symbolName: provider.symbolName,
            fields: fields,
            primaryAction: AddClusterProviderAction(
                .runExternalCLI,
                title: "Run",
                systemImage: "icloud.and.arrow.down"
            ),
            utilityActions: [
                AddClusterProviderAction(.importKubeconfig, title: "Import…", systemImage: "doc.badge.plus"),
                AddClusterProviderAction(.copyExternalCommand, title: "Copy", systemImage: "doc.on.doc"),
                AddClusterProviderAction(.refreshContexts, title: "Refresh", systemImage: "arrow.clockwise"),
                AddClusterProviderAction(.runAuthDoctor, title: "Doctor", systemImage: "stethoscope")
            ],
            showsCommandDetails: true,
            requiresCompatibleImportedContext: false
        )
    }

    private static func nativePresentation(
        provider: AddClusterProviderIdentifier,
        isNativeProfileConnected _: Bool
    ) -> AddClusterProviderPresentation {
        let fields: [AddClusterProviderField]
        let subtitle: String
        let note: String
        let primaryAction: AddClusterProviderAction

        switch provider {
        case .aks:
            subtitle = "Native Azure Public Cloud import"
            note = "Fetches cluster access from Azure Public Cloud, builds kubeconfig locally, and stores the service-principal secret only in Keychain."
            fields = [
                AddClusterProviderField(.clusterName, title: "Cluster name", isRequired: true),
                AddClusterProviderField(.resourceGroup, title: "Resource group", isRequired: true),
                AddClusterProviderField(.subscription, title: "Subscription ID", isRequired: true),
                AddClusterProviderField(.azureTenantID, title: "Tenant ID", isRequired: true),
                AddClusterProviderField(.azureClientID, title: "Service-principal client ID", isRequired: true),
                AddClusterProviderField(
                    .azureClientSecret,
                    title: "Service-principal secret",
                    isRequired: true,
                    input: .secureText
                )
            ]
            primaryAction = AddClusterProviderAction(
                .runNativeImport,
                title: "Import & Connect",
                systemImage: "icloud.and.arrow.down"
            )
        case .eks:
            subtitle = "Native AWS import"
            note = "Fetches the EKS endpoint and certificate directly from AWS and stores credential material only in Keychain."
            fields = [
                AddClusterProviderField(.clusterName, title: "Cluster name", isRequired: true),
                AddClusterProviderField(.region, title: "Region", isRequired: true),
                AddClusterProviderField(.awsAccessKeyID, title: "AWS access key ID", isRequired: true),
                AddClusterProviderField(
                    .awsSecretAccessKey,
                    title: "AWS secret access key",
                    isRequired: true,
                    input: .secureText
                ),
                AddClusterProviderField(
                    .awsSessionToken,
                    title: "AWS session token",
                    isRequired: false,
                    input: .secureText
                )
            ]
            primaryAction = AddClusterProviderAction(
                .runNativeImport,
                title: "Import & Connect",
                systemImage: "icloud.and.arrow.down"
            )
        case .gke:
            subtitle = "Native Google Cloud import"
            note = "Fetches cluster access from Google Cloud and stores the selected service-account document only in Keychain."
            fields = [
                AddClusterProviderField(.clusterName, title: "Cluster name", isRequired: true),
                AddClusterProviderField(.location, title: "Location, region or zone", isRequired: true),
                AddClusterProviderField(.projectID, title: "Project ID", isRequired: true),
                AddClusterProviderField(
                    .googleServiceAccountJSON,
                    title: "Google service-account JSON",
                    isRequired: true,
                    input: .sensitiveJSONFile
                )
            ]
            primaryAction = AddClusterProviderAction(
                .chooseServiceAccountJSON,
                title: "Choose JSON & Import…",
                systemImage: "icloud.and.arrow.down"
            )
        case .local:
            return localPresentation(mode: .nativeOnly)
        }

        let utilityActions = [
            AddClusterProviderAction(.importKubeconfig, title: "Import…", systemImage: "doc.badge.plus"),
            AddClusterProviderAction(.refreshContexts, title: "Refresh", systemImage: "arrow.clockwise"),
            AddClusterProviderAction(.runAuthDoctor, title: "Doctor", systemImage: "stethoscope")
        ]
        return AddClusterProviderPresentation(
            provider: provider,
            executionMode: .nativeOnly,
            title: provider.title,
            shortTitle: provider.shortTitle,
            subtitle: subtitle,
            note: note,
            symbolName: provider.symbolName,
            fields: fields,
            primaryAction: primaryAction,
            utilityActions: utilityActions,
            showsCommandDetails: false,
            requiresCompatibleImportedContext: false
        )
    }

    private static func localPresentation(
        mode: AddClusterProviderExecutionMode
    ) -> AddClusterProviderPresentation {
        AddClusterProviderPresentation(
            provider: .local,
            executionMode: mode,
            title: AddClusterProviderIdentifier.local.title,
            shortTitle: AddClusterProviderIdentifier.local.shortTitle,
            subtitle: "kind, k3s, k3d, minikube, Docker, OpenShift",
            note: "Imports kubeconfig written by a local cluster tool. Rune can copy setup commands but does not run them from this screen.",
            symbolName: AddClusterProviderIdentifier.local.symbolName,
            fields: [],
            primaryAction: AddClusterProviderAction(
                .importKubeconfig,
                title: "Import…",
                systemImage: "doc.badge.plus"
            ),
            utilityActions: [
                AddClusterProviderAction(
                    .copyLocalSetupCommand,
                    title: "Copy",
                    systemImage: "doc.on.doc"
                ),
                AddClusterProviderAction(.refreshContexts, title: "Refresh", systemImage: "arrow.clockwise"),
                AddClusterProviderAction(.runAuthDoctor, title: "Doctor", systemImage: "stethoscope")
            ],
            showsCommandDetails: true,
            requiresCompatibleImportedContext: false
        )
    }
}

extension AddClusterProviderIdentifier {
    var title: String {
        switch self {
        case .aks: return "Microsoft AKS"
        case .eks: return "Amazon EKS"
        case .gke: return "Google GKE"
        case .local: return "Local Cluster"
        }
    }

    var shortTitle: String {
        switch self {
        case .aks: return "AKS"
        case .eks: return "EKS"
        case .gke: return "GKE"
        case .local: return "Local"
        }
    }

    var symbolName: String {
        switch self {
        case .aks: return "square.grid.3x3.fill"
        case .eks: return "shippingbox.fill"
        case .gke: return "hexagon.fill"
        case .local: return "laptopcomputer"
        }
    }
}
