import Foundation
import RuneCore
import RuneSecurity

public struct AddClusterImportReviewFailure {
    public let message: String
    public let checks: [RuneHealthCheck]
}

public struct AddClusterCloudImportDiagnostic: Equatable {
    public let title: String
    public let classification: String
    public let message: String
    public let operationShape: String
    public let nextAction: String
    public let documentationTitle: String
    public let documentationURL: URL

    public var commandShape: String { operationShape }
}

public enum AddClusterCloudImportWorkflow {
    public static func runningStatus(for provider: CloudKubeConfigProvider) -> String {
        switch provider {
        case .aks: return "Running AKS import..."
        case .eks: return "Running EKS import..."
        case .gke: return "Running GKE import..."
        }
    }

    public static func importedStatus(for provider: CloudKubeConfigProvider) -> String {
        switch provider {
        case .aks: return "Imported AKS kubeconfig context."
        case .eks: return "Imported EKS kubeconfig context."
        case .gke: return "Imported GKE kubeconfig context."
        }
    }

    public static func readyForReviewStatus(for provider: CloudKubeConfigProvider) -> String {
        "\(providerTitle(provider)) kubeconfig is ready for review."
    }

    public static func nativeReadyForReviewStatus(for provider: CloudKubeConfigProvider) -> String {
        "\(nativeProviderTitle(provider)) cluster access is ready for review."
    }

    public static func nativeCancelledStatus(for provider: CloudKubeConfigProvider) -> String {
        "\(nativeProviderTitle(provider)) import cancelled."
    }

    public static func failedStatus() -> String {
        "Cloud import failed."
    }

    public static func diagnostic(for error: Error, provider: CloudKubeConfigProvider) -> AddClusterCloudImportDiagnostic {
        let title: String
        let classification: String
        let message: String
        let nextAction: String

        if let error = error as? CloudKubeConfigImportError {
            switch error {
            case .missingRequiredField(let field):
                title = "Missing required field"
                classification = "Missing field"
                message = "\(field) is required before Rune can run the provider import."
                nextAction = "Fill the highlighted provider fields, then run import again."
            case .externalCommandsUnavailable:
                title = "Provider CLI unavailable"
                classification = "App Store build"
                message = "This context requires CLI-backed auth, which this Rune build cannot run."
                nextAction = "Use static credentials or a native/import-guided flow where available, or use the direct download build for full CLI-backed auth compatibility."
            case .commandFailed(_, let exitCode, let output):
                if let providerFailure = providerCommandFailureDiagnostic(
                    provider: provider,
                    exitCode: exitCode,
                    output: output
                ) {
                    title = providerFailure.title
                    classification = providerFailure.classification
                    message = providerFailure.message
                    nextAction = providerFailure.nextAction
                } else {
                    title = "Provider CLI failed"
                    classification = "Exit code \(exitCode)"
                    message = "\(providerTitle(provider)) returned a non-zero exit code."
                    nextAction = "Check provider login, required fields, and CLI access, then run Auth Doctor."
                }
            case .commandTimedOut(_, let timeoutSeconds, _):
                title = "Provider CLI timed out"
                classification = "Timeout after \(timeoutSeconds)s"
                message = "\(providerTitle(provider)) did not finish before Rune's import timeout."
                nextAction = "Check network/VPN access and provider login, then retry or run the command manually."
            case .noKubeconfigDiscovered:
                title = "No kubeconfig found"
                classification = "No kubeconfig discovered"
                message = "\(providerTitle(provider)) completed, but Rune could not find a kubeconfig to review."
                nextAction = "Refresh contexts after confirming the provider CLI wrote kubeconfig."
            }
        } else {
            title = "Cloud import failed"
            classification = "Provider import failed"
            message = "Rune could not complete the provider import."
            nextAction = "Check provider login and required fields, then run Auth Doctor."
        }

        return AddClusterCloudImportDiagnostic(
            title: title,
            classification: classification,
            message: message,
            operationShape: commandShape(for: provider),
            nextAction: nextAction,
            documentationTitle: documentationTitle(for: provider),
            documentationURL: documentationURL(for: provider)
        )
    }

    public static func nativeDiagnostic(
        for error: Error,
        provider: CloudKubeConfigProvider
    ) -> AddClusterCloudImportDiagnostic {
        let failure = nativeFailurePresentation(for: error, provider: provider)
        return AddClusterCloudImportDiagnostic(
            title: failure.title,
            classification: failure.classification,
            message: failure.message,
            operationShape: nativeOperationShape(for: provider),
            nextAction: failure.nextAction,
            documentationTitle: documentationTitle(for: provider),
            documentationURL: documentationURL(for: provider)
        )
    }

    private struct NativeFailurePresentation {
        let title: String
        let classification: String
        let message: String
        let nextAction: String
    }

    private static func nativeFailurePresentation(
        for error: Error,
        provider: CloudKubeConfigProvider
    ) -> NativeFailurePresentation {
        switch provider {
        case .aks:
            if let error = error as? AKSNativeClusterImportError {
                return aksNativeFailurePresentation(for: error)
            }
        case .eks:
            if let error = error as? AWSEKSClusterImportError {
                return eksNativeFailurePresentation(for: error)
            }
            if let error = error as? AWSEKSNativeAuthError {
                return eksNativeAuthFailurePresentation(for: error)
            }
        case .gke:
            if let error = error as? GKENativeClusterImportError {
                return gkeNativeFailurePresentation(for: error)
            }
            if let error = error as? GCPServiceAccountAuthError {
                return gkeCredentialFailurePresentation(for: error)
            }
        }

        return NativeFailurePresentation(
            title: "Native import failed",
            classification: "Native import failed",
            message: "\(nativeProviderTitle(provider)) could not complete the secure provider connection.",
            nextAction: nativeDefaultNextAction(for: provider)
        )
    }

    private static func aksNativeFailurePresentation(
        for error: AKSNativeClusterImportError
    ) -> NativeFailurePresentation {
        switch error {
        case .invalidRequest:
            return NativeFailurePresentation(
                title: "Invalid AKS details",
                classification: "Invalid input",
                message: "One or more required Azure identifiers are missing or invalid.",
                nextAction: "Check the subscription, resource group, cluster, tenant, and client IDs, then retry import."
            )
        case .invalidCredentials:
            return NativeFailurePresentation(
                title: "Azure authentication failed",
                classification: "Authentication failed",
                message: "The Azure service-principal credential is missing or invalid.",
                nextAction: "Check the tenant ID, client ID, and client secret, then retry import."
            )
        case let .authenticationFailed(statusCode, _):
            return nativeAuthenticationFailurePresentation(
                title: "Azure authentication failed",
                providerTitle: "Microsoft Entra ID",
                statusCode: statusCode,
                message: "Microsoft Entra ID rejected the service-principal authentication request.",
                nextAction: "Check the tenant ID, client ID, and client secret, then retry import."
            )
        case let .clusterRequestFailed(statusCode, _):
            if statusCode == 401 {
                return nativeAuthenticationFailurePresentation(
                    title: "Azure authentication failed",
                    providerTitle: "Azure Resource Manager",
                    statusCode: statusCode,
                    message: "Azure Resource Manager rejected the service-principal access token.",
                    nextAction: "Check the tenant ID, client ID, and client secret, then retry import."
                )
            }
            if statusCode == 403 {
                return NativeFailurePresentation(
                    title: "AKS permission denied",
                    classification: "Authorization failed",
                    message: "Azure accepted the identity but denied access to fetch AKS user credentials.",
                    nextAction: "Grant AKS Cluster User access for this cluster, then retry import."
                )
            }
            if statusCode == 404 {
                return NativeFailurePresentation(
                    title: "AKS cluster not found",
                    classification: "Cluster not found",
                    message: "Azure could not find the requested AKS cluster in the selected scope.",
                    nextAction: "Check the subscription, resource group, and cluster name, then retry import."
                )
            }
            return rejectedNativeRequestPresentation(
                providerTitle: "Microsoft AKS",
                statusCode: statusCode,
                nextAction: "Check Azure service health and the cluster details, then retry import."
            )
        case .transport:
            return NativeFailurePresentation(
                title: "Azure connection failed",
                classification: "Network failure",
                message: "Rune could not complete the secure request to Microsoft Azure.",
                nextAction: "Check network or VPN access to Microsoft login and management endpoints, then retry import."
            )
        case .tenantMismatch:
            return NativeFailurePresentation(
                title: "AKS tenant mismatch",
                classification: "Tenant mismatch",
                message: "Azure returned cluster access configured for a different tenant.",
                nextAction: "Use the tenant ID configured for this AKS cluster, then retry import."
            )
        case .invalidEndpoint,
             .responseTooLarge,
             .invalidProviderResponse,
             .invalidKubeConfig,
             .incompatibleKubeConfig:
            return NativeFailurePresentation(
                title: "Invalid AKS response",
                classification: "Provider response rejected",
                message: "Rune rejected incomplete or incompatible cluster access data returned by Azure.",
                nextAction: "Retry after checking the cluster state; if it persists, import a reviewed kubeconfig instead."
            )
        }
    }

    private static func eksNativeFailurePresentation(
        for error: AWSEKSClusterImportError
    ) -> NativeFailurePresentation {
        switch error {
        case .invalidClusterName, .invalidRegion:
            return NativeFailurePresentation(
                title: "Invalid EKS details",
                classification: "Invalid input",
                message: "The EKS cluster name or AWS region is invalid.",
                nextAction: "Check the cluster name and region, then retry import."
            )
        case .unsupportedPartition:
            return NativeFailurePresentation(
                title: "AWS partition unsupported",
                classification: "Unsupported environment",
                message: "Rune cannot import this AWS partition through native authentication yet.",
                nextAction: "Import a reviewed kubeconfig or use Rune's direct build for this AWS environment."
            )
        case .networkFailure:
            return NativeFailurePresentation(
                title: "AWS connection failed",
                classification: "Network failure",
                message: "Rune could not complete the secure request to the Amazon EKS API.",
                nextAction: "Check network or VPN access to the regional EKS endpoint, then retry import."
            )
        case .authenticationFailed:
            return NativeFailurePresentation(
                title: "AWS authentication failed",
                classification: "Authentication failed",
                message: "Amazon EKS rejected the supplied AWS credentials.",
                nextAction: "Check the access key, secret key, and optional session token, then retry import."
            )
        case .accessDenied:
            return NativeFailurePresentation(
                title: "EKS permission denied",
                classification: "Authorization failed",
                message: "AWS accepted the credentials but denied access to describe the EKS cluster.",
                nextAction: "Grant eks:DescribeCluster for this cluster, then retry import."
            )
        case .clusterNotFound:
            return NativeFailurePresentation(
                title: "EKS cluster not found",
                classification: "Cluster not found",
                message: "Amazon EKS could not find the requested cluster in the selected region.",
                nextAction: "Check the cluster name, region, and AWS account, then retry import."
            )
        case let .requestRejected(statusCode):
            return rejectedNativeRequestPresentation(
                providerTitle: "Amazon EKS",
                statusCode: statusCode,
                nextAction: "Check AWS service health and the cluster details, then retry import."
            )
        case .clusterNotReady:
            return NativeFailurePresentation(
                title: "EKS cluster not ready",
                classification: "Cluster unavailable",
                message: "The EKS control plane is not ready to accept connections yet.",
                nextAction: "Wait for the cluster to become active, then retry import."
            )
        case .invalidHTTPResponse,
             .responseTooLarge,
             .invalidClusterResponse,
             .invalidClusterEndpoint,
             .invalidCertificateAuthority:
            return NativeFailurePresentation(
                title: "Invalid EKS response",
                classification: "Provider response rejected",
                message: "Rune rejected incomplete or unsafe cluster connection data returned by Amazon EKS.",
                nextAction: "Retry after checking the cluster state; if it persists, import a reviewed kubeconfig instead."
            )
        }
    }

    private static func eksNativeAuthFailurePresentation(
        for error: AWSEKSNativeAuthError
    ) -> NativeFailurePresentation {
        switch error {
        case .invalidCredentials:
            return NativeFailurePresentation(
                title: "Invalid AWS credentials",
                classification: "Authentication failed",
                message: "The supplied AWS credentials are missing or malformed.",
                nextAction: "Check the access key, secret key, and optional session token, then retry import."
            )
        case .expiredCredentials:
            return NativeFailurePresentation(
                title: "AWS credentials expired",
                classification: "Session expired",
                message: "The supplied temporary AWS credentials have expired.",
                nextAction: "Create fresh session credentials, including the session token, then retry import."
            )
        case .missingRegion,
             .invalidRegion,
             .missingClusterIdentifier,
             .conflictingClusterIdentifiers,
             .invalidClusterIdentifier:
            return NativeFailurePresentation(
                title: "Invalid EKS details",
                classification: "Invalid input",
                message: "The EKS cluster identifier or AWS region is missing or invalid.",
                nextAction: "Check the cluster name and region, then retry import."
            )
        case .missingOptionValue,
             .duplicateOption,
             .unsupportedOption,
             .unsupportedArgument,
             .unsupportedRoleAssumption,
             .customEndpointUnsupported,
             .unsupportedPartition,
             .tokenTooLarge:
            return NativeFailurePresentation(
                title: "AWS native authentication unsupported",
                classification: "Unsupported configuration",
                message: "The requested AWS authentication configuration is not supported by Rune's native flow.",
                nextAction: "Import a reviewed kubeconfig or use Rune's direct build for this AWS configuration."
            )
        }
    }

    private static func gkeNativeFailurePresentation(
        for error: GKENativeClusterImportError
    ) -> NativeFailurePresentation {
        switch error {
        case .missingRequiredField, .invalidResourceIdentifier:
            return NativeFailurePresentation(
                title: "Invalid GKE details",
                classification: "Invalid input",
                message: "The Google Cloud project, location, or cluster name is missing or invalid.",
                nextAction: "Check the project ID, location, and cluster name, then retry import."
            )
        case .authenticationFailed:
            return NativeFailurePresentation(
                title: "Google authentication failed",
                classification: "Authentication failed",
                message: "Google Cloud rejected the selected service-account credential.",
                nextAction: "Check the service-account JSON and select a current credential, then retry import."
            )
        case .networkFailure:
            return NativeFailurePresentation(
                title: "Google Cloud connection failed",
                classification: "Network failure",
                message: "Rune could not complete the secure request to Google Kubernetes Engine.",
                nextAction: "Check network or VPN access to Google OAuth and GKE endpoints, then retry import."
            )
        case let .requestRejected(statusCode):
            if statusCode == 403 {
                return NativeFailurePresentation(
                    title: "GKE permission denied",
                    classification: "Authorization failed",
                    message: "Google Cloud accepted the credential but denied access to read the GKE cluster.",
                    nextAction: "Grant container.clusters.get for this cluster, then retry import."
                )
            }
            if statusCode == 404 {
                return NativeFailurePresentation(
                    title: "GKE cluster not found",
                    classification: "Cluster not found",
                    message: "Google Kubernetes Engine could not find the requested cluster in the selected location.",
                    nextAction: "Check the project ID, location, and cluster name, then retry import."
                )
            }
            return rejectedNativeRequestPresentation(
                providerTitle: "Google GKE",
                statusCode: statusCode,
                nextAction: "Check Google Cloud service health and the cluster details, then retry import."
            )
        case .invalidHTTPResponse,
             .responseTooLarge,
             .invalidClusterResponse,
             .invalidClusterEndpoint,
             .invalidCertificateAuthority:
            return NativeFailurePresentation(
                title: "Invalid GKE response",
                classification: "Provider response rejected",
                message: "Rune rejected incomplete or unsafe cluster connection data returned by Google Cloud.",
                nextAction: "Retry after checking the cluster state; if it persists, import a reviewed kubeconfig instead."
            )
        }
    }

    private static func gkeCredentialFailurePresentation(
        for error: GCPServiceAccountAuthError
    ) -> NativeFailurePresentation {
        switch error {
        case .invalidJSON,
             .unsupportedCredentialType,
             .missingRequiredField,
             .invalidClientEmail,
             .invalidPrivateKey,
             .invalidTokenURI,
             .signingFailed:
            return NativeFailurePresentation(
                title: "Invalid Google credential",
                classification: "Credential rejected",
                message: "Rune could not use the selected Google service-account document.",
                nextAction: "Select a valid service-account JSON credential, then retry import."
            )
        case .networkFailure:
            return NativeFailurePresentation(
                title: "Google OAuth connection failed",
                classification: "Network failure",
                message: "Rune could not complete the secure request to Google OAuth.",
                nextAction: "Check network or VPN access to Google OAuth, then retry import."
            )
        case let .tokenEndpointRejected(statusCode):
            return nativeAuthenticationFailurePresentation(
                title: "Google authentication failed",
                providerTitle: "Google OAuth",
                statusCode: statusCode,
                message: "Google OAuth rejected the service-account assertion.",
                nextAction: "Check that the service account and private key are active, then retry import."
            )
        case .invalidHTTPResponse, .responseTooLarge, .invalidTokenResponse:
            return NativeFailurePresentation(
                title: "Invalid Google OAuth response",
                classification: "Provider response rejected",
                message: "Rune rejected an incomplete or unsafe response from Google OAuth.",
                nextAction: "Retry with a current service-account credential."
            )
        }
    }

    private static func nativeAuthenticationFailurePresentation(
        title: String,
        providerTitle: String,
        statusCode: Int,
        message: String,
        nextAction: String
    ) -> NativeFailurePresentation {
        if (400...403).contains(statusCode) {
            return NativeFailurePresentation(
                title: title,
                classification: "Authentication failed",
                message: message,
                nextAction: nextAction
            )
        }
        return rejectedNativeRequestPresentation(
            providerTitle: providerTitle,
            statusCode: statusCode,
            nextAction: "Check provider service health and network access, then retry import."
        )
    }

    private static func rejectedNativeRequestPresentation(
        providerTitle: String,
        statusCode: Int,
        nextAction: String
    ) -> NativeFailurePresentation {
        let safeStatusCode = (100...599).contains(statusCode) ? statusCode : 0
        if safeStatusCode == 429 {
            return NativeFailurePresentation(
                title: "Provider request throttled",
                classification: "HTTP 429",
                message: "\(providerTitle) temporarily throttled the native cluster access request.",
                nextAction: "Wait briefly, then retry import."
            )
        }
        if (500...599).contains(safeStatusCode) {
            return NativeFailurePresentation(
                title: "Provider temporarily unavailable",
                classification: "HTTP \(safeStatusCode)",
                message: "\(providerTitle) could not complete the native cluster access request.",
                nextAction: "Check provider service status, then retry import."
            )
        }
        return NativeFailurePresentation(
            title: "Provider request rejected",
            classification: "HTTP \(safeStatusCode)",
            message: "\(providerTitle) rejected the native cluster access request.",
            nextAction: nextAction
        )
    }

    private static func nativeDefaultNextAction(for provider: CloudKubeConfigProvider) -> String {
        switch provider {
        case .aks:
            return "Check the Azure identifiers, service-principal credential, and AKS access, then retry import."
        case .eks:
            return "Check the cluster, region, AWS credentials, and EKS access, then retry import."
        case .gke:
            return "Check the project, location, service-account credential, and GKE access, then retry import."
        }
    }

    private static func nativeOperationShape(for provider: CloudKubeConfigProvider) -> String {
        switch provider {
        case .aks:
            return "Microsoft Entra ID → Azure Resource Manager HTTPS API → kubeconfig review → Keychain"
        case .eks:
            return "AWS Signature V4 → Amazon EKS HTTPS API → kubeconfig review → Keychain"
        case .gke:
            return "Google OAuth → GKE HTTPS API → kubeconfig review → Keychain"
        }
    }

    private static func providerCommandFailureDiagnostic(
        provider: CloudKubeConfigProvider,
        exitCode: Int32,
        output: String
    ) -> (title: String, classification: String, message: String, nextAction: String)? {
        let lower = output.lowercased()

        switch provider {
        case .aks:
            if lower.contains("authorizationfailed")
                || lower.contains("listclusterusercredential")
                || lower.contains("does not have authorization to perform action") {
                return (
                    title: "Azure authorization failed",
                    classification: "AKS permission denied",
                    message: "Azure CLI is logged in, but the selected identity cannot fetch AKS user credentials for this cluster.",
                    nextAction: "Ask for AKS Cluster User access on this cluster, then retry import."
                )
            }
        case .eks, .gke:
            break
        }

        if lower.contains("not logged in")
            || lower.contains("login required")
            || lower.contains("please run")
            || lower.contains("az login")
            || lower.contains("gcloud auth login")
            || lower.contains("aws sso login") {
            return (
                title: "Provider login required",
                classification: "Login required",
                message: "\(providerTitle(provider)) could not use an active provider login session.",
                nextAction: "Sign in with the provider CLI, then retry import."
            )
        }

        return nil
    }

    public static func blockingIssues(in reviews: [KubeConfigImportReview]) -> [KubeConfigImportIssue] {
        var issues: [KubeConfigImportIssue] = []
        issues.reserveCapacity(reviews.count)
        for review in reviews {
            for issue in review.issues where issue.severity == .error {
                issues.append(issue)
            }
        }
        return issues
    }

    public static func blockingImportErrorMessage(for issues: [KubeConfigImportIssue]) -> String {
        var message = ""
        for issue in issues.prefix(3) {
            if !message.isEmpty {
                message.append(" ")
            }
            message.append(safeImportReviewIssueMessage(for: issue))
        }
        return message
    }

    public static func blockingFailure(in reviews: [KubeConfigImportReview]) -> AddClusterImportReviewFailure? {
        var issues: [KubeConfigImportIssue] = []
        issues.reserveCapacity(5)
        var message = ""
        var messageCount = 0

        for review in reviews {
            for issue in review.issues where issue.severity == .error {
                if issues.count < 5 {
                    issues.append(issue)
                }
                if messageCount < 3 {
                    if !message.isEmpty {
                        message.append(" ")
                    }
                    message.append(safeImportReviewIssueMessage(for: issue))
                    messageCount += 1
                }
                if issues.count == 5 && messageCount == 3 {
                    return AddClusterImportReviewFailure(
                        message: message,
                        checks: importReviewFailureChecks(for: issues)
                    )
                }
            }
        }

        guard !issues.isEmpty else { return nil }
        return AddClusterImportReviewFailure(
            message: message,
            checks: importReviewFailureChecks(for: issues)
        )
    }

    public static func importReviewFailureChecks(for issues: [KubeConfigImportIssue]) -> [RuneHealthCheck] {
        var checks: [RuneHealthCheck] = []
        checks.reserveCapacity(1 + min(5, issues.count))
        checks.append(RuneHealthCheck(
            id: "kubeconfig-import",
            title: "Kubeconfig import",
            status: .failed,
            message: "Import was stopped before saving access because the kubeconfig review found blocking issues."
        ))
        for issue in issues.prefix(5) {
            checks.append(RuneHealthCheck(
                id: "kubeconfig-import-\(safeHealthCheckIDSuffix(for: issue.id))",
                title: "Import review",
                status: .failed,
                message: safeImportReviewIssueMessage(for: issue)
            ))
        }
        return checks
    }

    public static func safeImportReviewIssueMessage(for issue: KubeConfigImportIssue) -> String {
        let id = issue.id
        if id == "missing-current-context" {
            return "Kubeconfig is missing current-context."
        }
        if id == "missing-contexts" {
            return "Kubeconfig does not contain any contexts."
        }
        if id == "missing-current-context-reference" {
            return "Kubeconfig current-context does not match any context."
        }
        if id.hasPrefix("duplicate-context-") {
            return "Kubeconfig contains a duplicate context name. Choose update, copy, or skip before importing."
        }
        if id.hasPrefix("duplicate-cluster-") {
            return "Kubeconfig contains a duplicate cluster name. Choose update, copy, or skip before importing."
        }
        if id.hasPrefix("duplicate-user-") {
            return "Kubeconfig contains a duplicate user name. Choose update, copy, or skip before importing."
        }
        if id.hasPrefix("missing-cluster-") {
            return "A kubeconfig context references a missing cluster."
        }
        if id.hasPrefix("missing-server-") {
            return "A kubeconfig context is missing a valid cluster server URL."
        }
        if id.hasPrefix("missing-exec-plugin-") {
            return "A kubeconfig context uses an exec auth command Rune cannot find on PATH."
        }
        switch id {
        case "empty":
            return "Kubeconfig is empty."
        case "cluster-missing-name":
            return "Kubeconfig contains a cluster entry without a name."
        case "context-missing-name":
            return "Kubeconfig contains a context entry without a name."
        case "user-missing-name":
            return "Kubeconfig contains a user entry without a name."
        case "unsupported-yaml-feature":
            return "Kubeconfig uses YAML features Rune does not import safely yet."
        case "unsupported-multi-document":
            return "Kubeconfig must contain exactly one YAML document."
        case "malformed-yaml":
            return "Kubeconfig contains malformed YAML."
        case "list-without-section":
            return "Kubeconfig contains a list item outside clusters, contexts, or users."
        default:
            return "Kubeconfig import review found a blocking issue."
        }
    }

    public static func cloudLoginFailureChecks(for provider: CloudKubeConfigProvider) -> [RuneHealthCheck] {
        [
            RuneHealthCheck(
                id: cloudLoginCheckID(for: provider),
                title: cloudLoginCheckTitle(for: provider),
                status: .failed,
                message: "Cloud kubeconfig import did not complete. Check the provider CLI login and required fields, then run Auth Doctor again."
            )
        ]
    }

    public static func nativeImportFailureChecks(
        for provider: CloudKubeConfigProvider,
        diagnostic: AddClusterCloudImportDiagnostic? = nil
    ) -> [RuneHealthCheck] {
        let message: String
        if let diagnostic {
            message = "\(diagnostic.message) Classification: \(diagnostic.classification). Next action: \(diagnostic.nextAction)"
        } else {
            switch provider {
            case .aks:
                message = "Native AKS import did not complete. Check the IDs, service-principal credentials, and permission to list cluster user credentials."
            case .eks:
                message = "Native EKS import did not complete. Check the cluster, region, AWS credentials, and eks:DescribeCluster permission."
            case .gke:
                message = "Native GKE import did not complete. Check the cluster location, service-account credentials, and container.clusters.get permission."
            }
        }
        return [
            RuneHealthCheck(
                id: cloudLoginCheckID(for: provider),
                title: cloudLoginCheckTitle(for: provider),
                status: .failed,
                message: message
            )
        ]
    }

    public static func commandShape(for provider: CloudKubeConfigProvider) -> String {
        switch provider {
        case .aks:
            return "az aks get-credentials --resource-group <resource-group> --name <cluster-name> --overwrite-existing"
        case .eks:
            return "aws eks update-kubeconfig --region <region> --name <cluster-name> [--profile <profile>] [--role-arn <role-arn>]"
        case .gke:
            return "gcloud container clusters get-credentials <cluster-name> --location <location> --project <project-id>"
        }
    }

    private static func providerTitle(_ provider: CloudKubeConfigProvider) -> String {
        switch provider {
        case .aks: return "Azure CLI"
        case .eks: return "AWS CLI"
        case .gke: return "Google Cloud CLI"
        }
    }

    private static func nativeProviderTitle(_ provider: CloudKubeConfigProvider) -> String {
        switch provider {
        case .aks: return "Microsoft AKS"
        case .eks: return "Amazon EKS"
        case .gke: return "Google GKE"
        }
    }

    private static func documentationTitle(for provider: CloudKubeConfigProvider) -> String {
        switch provider {
        case .aks: return "AKS Login Docs"
        case .eks: return "EKS Login Docs"
        case .gke: return "GKE Login Docs"
        }
    }

    private static func documentationURL(for provider: CloudKubeConfigProvider) -> URL {
        switch provider {
        case .aks:
            return URL(string: "https://learn.microsoft.com/en-us/azure/aks/kubelogin-authentication")!
        case .eks:
            return URL(string: "https://docs.aws.amazon.com/eks/latest/userguide/create-kubeconfig.html")!
        case .gke:
            return URL(string: "https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl")!
        }
    }

    private static func cloudLoginCheckID(for provider: CloudKubeConfigProvider) -> String {
        switch provider {
        case .aks: return "cloud-login-aks"
        case .eks: return "cloud-login-eks"
        case .gke: return "cloud-login-gke"
        }
    }

    private static func cloudLoginCheckTitle(for provider: CloudKubeConfigProvider) -> String {
        switch provider {
        case .aks: return "AKS cloud login"
        case .eks: return "EKS cloud login"
        case .gke: return "GKE cloud login"
        }
    }

    private static func safeHealthCheckIDSuffix(for rawID: String) -> String {
        var hash: UInt64 = 14_695_981_039_346_656_037
        var count = 0
        var isSafe = !rawID.isEmpty
        for byte in rawID.utf8 {
            hash ^= UInt64(byte)
            hash &*= 1_099_511_628_211
            count += 1
            isSafe = isSafe
                && count <= 64
                && ((48...57).contains(byte)
                || (65...90).contains(byte)
                || (97...122).contains(byte)
                || byte == 45
                || byte == 95)
        }
        if isSafe {
            return rawID
        }
        return "issue-\(String(hash, radix: 16))"
    }
}
