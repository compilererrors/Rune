import RuneSecurity

struct AddClusterNativeContextOption: Identifiable, Sendable, Equatable {
    let request: KubernetesNativeCredentialRequest

    var id: String { request.bindingID }
    var contextName: String { request.contextName }
}

enum AddClusterNativeContextResolution: Sendable, Equatable {
    case unavailable
    case selected(AddClusterNativeContextOption)
    case requiresChoice([AddClusterNativeContextOption])
}

enum AddClusterNativeContextResolver {
    private static let supportedProviders: Set<KubernetesNativeAuthProviderKind> = [
        .awsEKS,
        .azureKubelogin,
        .googleGKE
    ]

    /// Resolves provider-compatible contexts without reading global UI selection at
    /// action time. A compatible current context wins, a sole option is automatic,
    /// and ambiguous matches must be chosen explicitly by the user.
    static func resolve(
        provider: KubernetesNativeAuthProviderKind,
        analysis: KubeConfigNativeAuthAnalysis,
        currentContextName: String? = nil
    ) -> AddClusterNativeContextResolution {
        guard supportedProviders.contains(provider) else { return .unavailable }

        let options = compatibleOptions(provider: provider, analysis: analysis)

        if let currentContextName = currentContextName ?? analysis.currentContext,
           let current = options.first(where: { $0.contextName == currentContextName }) {
            return .selected(current)
        }

        switch options.count {
        case 0:
            return .unavailable
        case 1:
            return .selected(options[0])
        default:
            return .requiresChoice(options)
        }
    }

    static func compatibleOptions(
        provider: KubernetesNativeAuthProviderKind,
        analysis: KubeConfigNativeAuthAnalysis
    ) -> [AddClusterNativeContextOption] {
        guard supportedProviders.contains(provider) else { return [] }
        return analysis.contexts.compactMap { descriptor -> AddClusterNativeContextOption? in
            guard let request = descriptor.credentialRequest,
                  request.provider == provider else {
                return nil
            }
            return AddClusterNativeContextOption(request: request)
        }
    }
}
