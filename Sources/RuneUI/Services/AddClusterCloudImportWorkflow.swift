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
    public let commandShape: String
    public let nextAction: String
    public let documentationTitle: String
    public let documentationURL: URL
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
            commandShape: commandShape(for: provider),
            nextAction: nextAction,
            documentationTitle: documentationTitle(for: provider),
            documentationURL: documentationURL(for: provider)
        )
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

    public static func nativeImportFailureChecks(for provider: CloudKubeConfigProvider) -> [RuneHealthCheck] {
        let message: String
        switch provider {
        case .aks:
            message = "Native AKS import did not complete. Check the IDs, service-principal credentials, and permission to list cluster user credentials."
        case .eks:
            message = "Native EKS import did not complete. Check the cluster, region, AWS credentials, and eks:DescribeCluster permission."
        case .gke:
            message = "Native GKE import did not complete. Check the cluster location, service-account credentials, and container.clusters.get permission."
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
