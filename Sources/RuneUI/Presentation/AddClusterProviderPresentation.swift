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

enum AddClusterProviderFieldRequirement: Sendable, Equatable {
    case required
    case optional
    case requiredForOptionalMethod
}

struct AddClusterProviderField: Sendable, Equatable, Identifiable {
    let id: AddClusterProviderFieldIdentifier
    let title: String
    let requirement: AddClusterProviderFieldRequirement
    let input: AddClusterProviderFieldInput
    let helpText: String?

    var isRequired: Bool {
        requirement == .required
    }

    init(
        _ id: AddClusterProviderFieldIdentifier,
        title: String,
        isRequired: Bool,
        input: AddClusterProviderFieldInput = .text,
        helpText: String? = nil
    ) {
        self.id = id
        self.title = title
        requirement = isRequired ? .required : .optional
        self.input = input
        self.helpText = helpText
    }

    init(
        _ id: AddClusterProviderFieldIdentifier,
        title: String,
        requirement: AddClusterProviderFieldRequirement,
        input: AddClusterProviderFieldInput = .text,
        helpText: String? = nil
    ) {
        self.id = id
        self.title = title
        self.requirement = requirement
        self.input = input
        self.helpText = helpText
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

    /// Shares cluster details with the main form while keeping each input and
    /// accessibility identity unique when the optional method is expanded.
    func credentialFields(excluding sharedFields: [AddClusterProviderField]) -> [AddClusterProviderField] {
        let sharedIdentifiers = Set(sharedFields.map(\.id))
        return fields.filter { !sharedIdentifiers.contains($0.id) }
    }

    var credentialSectionTitle: String {
        guard executionMode == .nativeOnly else { return "Credentials" }
        switch provider {
        case .aks:
            return "Service principal"
        case .eks:
            return "Access keys"
        case .gke:
            return "Service account"
        case .local:
            return "Credentials"
        }
    }

    var credentialSectionDescription: String {
        guard executionMode == .nativeOnly else { return "" }
        switch provider {
        case .aks:
            return "Uses the cluster name, resource group and subscription ID above. Complete these credentials only for this optional method; the secret is stored only in Keychain."
        case .eks:
            return "Uses the cluster name and region above. The session token is needed only for temporary credentials; secrets are stored only in Keychain."
        case .gke:
            return "Uses the cluster details above. Choose a service-account JSON document stored only in Keychain."
        case .local:
            return ""
        }
    }

    var compactSubtitle: String {
        if provider == .local {
            return "Local kubeconfig"
        }
        switch executionMode {
        case .externalCLI:
            return "Automatic CLI setup"
        case .nativeOnly:
            return "Kubeconfig + auth"
        }
    }

    var localCLISetupDescription: String {
        switch provider {
        case .aks:
            return "Install Azure CLI and sign in on this Mac. Rune uses that local session; no Rune account or server is needed."
        case .eks:
            return "Install AWS CLI v2. For IAM Identity Center, configure SSO once, then sign in with the same profile you enter above. No Rune account or server is needed."
        case .gke:
            return "Install Google Cloud CLI and the GKE auth plugin, then sign in on this Mac. Rune uses that local session; no Rune account or server is needed."
        case .local:
            return ""
        }
    }

    var localCLISetupCommands: [String] {
        switch provider {
        case .aks: return ["az login"]
        case .eks: return ["aws configure sso", "aws sso login --profile <profile>"]
        case .gke: return ["gcloud auth login"]
        case .local: return []
        }
    }

    var localCLISetupDocumentationURL: URL? {
        switch provider {
        case .aks:
            return URL(string: "https://learn.microsoft.com/en-us/cli/azure/authenticate-azure-cli-interactively")
        case .eks:
            return URL(string: "https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sso.html")
        case .gke:
            return URL(string: "https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl")
        case .local:
            return nil
        }
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
            subtitle = "Azure CLI · automatic kubeconfig"
            note = "Uses your signed-in Azure CLI. Rune creates, validates, and opens the kubeconfig for review automatically."
            fields = [
                AddClusterProviderField(.clusterName, title: "Cluster name", isRequired: true),
                AddClusterProviderField(.resourceGroup, title: "Resource group", isRequired: true),
                AddClusterProviderField(
                    .subscription,
                    title: "Subscription ID or name",
                    isRequired: false,
                    helpText: "Optional for Azure CLI. Service-principal import requires a subscription ID."
                )
            ]
        case .eks:
            subtitle = "AWS CLI · automatic kubeconfig"
            note = "Uses your configured AWS CLI profile. Rune creates, validates, and opens the kubeconfig for review automatically."
            fields = [
                AddClusterProviderField(.clusterName, title: "Cluster name", isRequired: true),
                AddClusterProviderField(.region, title: "Region", isRequired: true),
                AddClusterProviderField(.profile, title: "AWS profile", isRequired: false),
                AddClusterProviderField(.roleARN, title: "Role ARN", isRequired: false)
            ]
        case .gke:
            subtitle = "Google Cloud CLI · automatic kubeconfig"
            note = "Uses your signed-in Google Cloud CLI. Rune creates, validates, and opens the kubeconfig for review automatically."
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
                title: "Add cluster",
                systemImage: "icloud.and.arrow.down"
            ),
            utilityActions: [
                AddClusterProviderAction(.importKubeconfig, title: "Import file…", systemImage: "doc.badge.plus"),
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
            subtitle = "Kubeconfig and optional cloud access"
            note = "Start with kubeconfig. Service-principal credentials are needed only if you choose the optional native method below."
            fields = [
                AddClusterProviderField(.clusterName, title: "Cluster name", requirement: .requiredForOptionalMethod),
                AddClusterProviderField(.resourceGroup, title: "Resource group", requirement: .requiredForOptionalMethod),
                AddClusterProviderField(.subscription, title: "Subscription ID", requirement: .requiredForOptionalMethod),
                AddClusterProviderField(.azureTenantID, title: "Tenant ID", requirement: .requiredForOptionalMethod),
                AddClusterProviderField(.azureClientID, title: "Client ID", requirement: .requiredForOptionalMethod),
                AddClusterProviderField(
                    .azureClientSecret,
                    title: "Client secret",
                    requirement: .requiredForOptionalMethod,
                    input: .secureText
                )
            ]
            primaryAction = AddClusterProviderAction(
                .importKubeconfig,
                title: "Import kubeconfig…",
                systemImage: "doc.badge.plus"
            )
        case .eks:
            subtitle = "Kubeconfig and optional cloud access"
            note = "Start with kubeconfig. AWS access keys are needed only if you choose the optional native method below."
            fields = [
                AddClusterProviderField(.clusterName, title: "Cluster name", requirement: .requiredForOptionalMethod),
                AddClusterProviderField(.region, title: "Region", requirement: .requiredForOptionalMethod),
                AddClusterProviderField(.awsAccessKeyID, title: "Access key ID", requirement: .requiredForOptionalMethod),
                AddClusterProviderField(
                    .awsSecretAccessKey,
                    title: "Secret access key",
                    requirement: .requiredForOptionalMethod,
                    input: .secureText
                ),
                AddClusterProviderField(
                    .awsSessionToken,
                    title: "Session token",
                    isRequired: false,
                    input: .secureText
                )
            ]
            primaryAction = AddClusterProviderAction(
                .importKubeconfig,
                title: "Import kubeconfig…",
                systemImage: "doc.badge.plus"
            )
        case .gke:
            subtitle = "Kubeconfig and optional cloud access"
            note = "Start with kubeconfig. A Google service account is needed only if you choose the optional native method below."
            fields = [
                AddClusterProviderField(.clusterName, title: "Cluster name", requirement: .requiredForOptionalMethod),
                AddClusterProviderField(.location, title: "Location, region or zone", requirement: .requiredForOptionalMethod),
                AddClusterProviderField(.projectID, title: "Project ID", requirement: .requiredForOptionalMethod),
                AddClusterProviderField(
                    .googleServiceAccountJSON,
                    title: "Credentials JSON",
                    requirement: .requiredForOptionalMethod,
                    input: .sensitiveJSONFile
                )
            ]
            primaryAction = AddClusterProviderAction(
                .importKubeconfig,
                title: "Import kubeconfig…",
                systemImage: "doc.badge.plus"
            )
        case .local:
            return localPresentation(mode: .nativeOnly)
        }

        let utilityActions: [AddClusterProviderAction]
        switch provider {
        case .aks:
            utilityActions = [
                AddClusterProviderAction(
                    .runNativeImport,
                    title: "Import with service principal",
                    systemImage: "key.fill"
                ),
                AddClusterProviderAction(.refreshContexts, title: "Refresh", systemImage: "arrow.clockwise"),
                AddClusterProviderAction(.runAuthDoctor, title: "Doctor", systemImage: "stethoscope")
            ]
        case .eks:
            utilityActions = [
                AddClusterProviderAction(
                    .runNativeImport,
                    title: "Import with access keys",
                    systemImage: "key.fill"
                ),
                AddClusterProviderAction(.refreshContexts, title: "Refresh", systemImage: "arrow.clockwise"),
                AddClusterProviderAction(.runAuthDoctor, title: "Doctor", systemImage: "stethoscope")
            ]
        case .gke:
            utilityActions = [
                AddClusterProviderAction(
                    .chooseServiceAccountJSON,
                    title: "Import with service account…",
                    systemImage: "key.fill"
                ),
                AddClusterProviderAction(.refreshContexts, title: "Refresh", systemImage: "arrow.clockwise"),
                AddClusterProviderAction(.runAuthDoctor, title: "Doctor", systemImage: "stethoscope")
            ]
        case .local:
            utilityActions = []
        }
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

    var cloudCLIName: String? {
        switch self {
        case .aks: return "Azure CLI"
        case .eks: return "AWS CLI"
        case .gke: return "Google Cloud CLI"
        case .local: return nil
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
