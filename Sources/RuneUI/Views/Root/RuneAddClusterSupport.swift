import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

enum RuneAddClusterProviderActionLayout {
    static let dialogWidth: CGFloat = RuneUILayoutMetrics.standardDialogWidth
    static let horizontalPadding: CGFloat = RuneUILayoutMetrics.dialogContentPadding * 2
    static let columnSpacing: CGFloat = 8
    static let minimumButtonWidth: CGFloat = 150
    static let minimumCredentialFieldWidth: CGFloat = 220

    static func contentWidth(dialogWidth: CGFloat = Self.dialogWidth) -> CGFloat {
        max(0, dialogWidth - horizontalPadding)
    }

    static func columnCount(for actionCount: Int, dialogWidth: CGFloat = Self.dialogWidth) -> Int {
        guard actionCount > 0 else { return 0 }
        let availableWidth = contentWidth(dialogWidth: dialogWidth)
        let fitted = Int((availableWidth + columnSpacing) / (minimumButtonWidth + columnSpacing))
        return min(actionCount, max(1, fitted))
    }

    static func rowCount(for actionCount: Int, dialogWidth: CGFloat = Self.dialogWidth) -> Int {
        let columns = columnCount(for: actionCount, dialogWidth: dialogWidth)
        guard columns > 0 else { return 0 }
        return Int(ceil(Double(actionCount) / Double(columns)))
    }
}

typealias RuneAddClusterProvider = AddClusterProviderIdentifier

extension AddClusterProviderIdentifier {
    var cloudProvider: CloudKubeConfigProvider? {
        switch self {
        case .aks: return .aks
        case .eks: return .eks
        case .gke: return .gke
        case .local: return nil
        }
    }

    var command: String {
        switch self {
        case .aks:
            return "az aks get-credentials --resource-group <resource-group> --name <cluster-name> --overwrite-existing"
        case .eks:
            return "aws eks update-kubeconfig --region <region> --name <cluster-name>"
        case .gke:
            return "gcloud container clusters get-credentials <cluster-name> --location <location> --project <project-id>"
        case .local:
            return "kind create cluster --name rune-dev"
        }
    }

    var auxiliaryCommands: [(title: String, command: String)] {
        switch self {
        case .local:
            return [
                ("Status", "kind get clusters && minikube status && k3d cluster list && k3s kubectl config current-context"),
                ("K3s config", "sudo cat /etc/rancher/k3s/k3s.yaml"),
                ("K3d config", "k3d kubeconfig get <cluster-name>"),
                ("Kind config", "kind get kubeconfig --name <cluster-name>"),
                ("Docker", "kubectl config use-context docker-desktop && kubectl config view --minify --raw"),
                ("OrbStack", "kubectl config use-context orbstack && kubectl config view --minify --raw"),
                ("OpenShift", "oc login <api-server> && oc config view --minify --raw"),
                ("CRC", "crc start && crc oc-env"),
                ("Start", "minikube start"),
                ("Stop", "minikube stop")
            ]
        case .aks, .eks, .gke:
            return []
        }
    }

    var nativeAuthProvider: KubernetesNativeAuthProviderKind? {
        switch self {
        case .aks: return .azureKubelogin
        case .eks: return .awsEKS
        case .gke: return .googleGKE
        case .local: return nil
        }
    }
}

struct CloudCredentialDraft {
    private static let fieldWhitespace = CharacterSet.whitespacesAndNewlines

    var clusterName = ""
    var regionOrLocation = ""
    var resourceGroup = ""
    var projectID = ""
    var profileOrSubscription = ""
    var roleARN = ""
    var nativeAWSAccessKeyID = ""
    var nativeAWSSecretAccessKey = ""
    var nativeAWSSessionToken = ""
    var nativeAKSTenantID = ""
    var nativeAKSClientID = ""
    var nativeAKSClientSecret = ""

    func request(provider: CloudKubeConfigProvider) -> CloudKubeConfigImportRequest {
        CloudKubeConfigImportRequest(
            provider: provider,
            clusterName: trimmed(clusterName),
            regionOrLocation: trimmed(regionOrLocation),
            resourceGroup: trimmed(resourceGroup),
            projectID: trimmed(projectID),
            profileOrSubscription: trimmed(profileOrSubscription),
            roleARN: trimmed(roleARN)
        )
    }

    func hasRequiredFields(for provider: CloudKubeConfigProvider) -> Bool {
        switch provider {
        case .aks:
            return hasValue(clusterName) && hasValue(resourceGroup)
        case .eks:
            return hasValue(clusterName) && hasValue(regionOrLocation)
        case .gke:
            return hasValue(clusterName) && hasValue(regionOrLocation) && hasValue(projectID)
        }
    }

    func missingRequiredFieldSummary(for provider: CloudKubeConfigProvider) -> String? {
        var summary = ""
        func appendMissing(_ label: String) {
            if !summary.isEmpty {
                summary.append(", ")
            }
            summary.append(label)
        }

        if !hasValue(clusterName) {
            appendMissing("cluster name")
        }
        switch provider {
        case .aks:
            if !hasValue(resourceGroup) {
                appendMissing("resource group")
            }
        case .eks:
            if !hasValue(regionOrLocation) {
                appendMissing("region")
            }
        case .gke:
            if !hasValue(regionOrLocation) {
                appendMissing("location")
            }
            if !hasValue(projectID) {
                appendMissing("project ID")
            }
        }
        return summary.isEmpty ? nil : summary
    }

    private func hasValue(_ value: String) -> Bool {
        value.unicodeScalars.contains { !Self.fieldWhitespace.contains($0) }
    }

    var hasNativeAWSCredentials: Bool {
        hasValue(nativeAWSAccessKeyID) && hasValue(nativeAWSSecretAccessKey)
    }

    var hasNativeAKSClientSecret: Bool {
        hasValue(nativeAKSClientSecret)
    }

    func hasRequiredNativeFields(for provider: CloudKubeConfigProvider) -> Bool {
        guard hasRequiredFields(for: provider) else { return false }
        switch provider {
        case .aks:
            return hasValue(profileOrSubscription)
                && hasValue(nativeAKSTenantID)
                && hasValue(nativeAKSClientID)
                && hasValue(nativeAKSClientSecret)
        case .eks:
            return hasNativeAWSCredentials
        case .gke:
            return true
        }
    }

    func missingRequiredNativeFieldSummary(for provider: CloudKubeConfigProvider) -> String? {
        var fields: [String] = []
        if !hasValue(clusterName) { fields.append("cluster name") }
        switch provider {
        case .aks:
            if !hasValue(resourceGroup) { fields.append("resource group") }
            if !hasValue(profileOrSubscription) { fields.append("subscription ID") }
            if !hasValue(nativeAKSTenantID) { fields.append("tenant ID") }
            if !hasValue(nativeAKSClientID) { fields.append("client ID") }
            if !hasValue(nativeAKSClientSecret) { fields.append("client secret") }
        case .eks:
            if !hasValue(regionOrLocation) { fields.append("region") }
            if !hasValue(nativeAWSAccessKeyID) { fields.append("access key ID") }
            if !hasValue(nativeAWSSecretAccessKey) { fields.append("secret access key") }
        case .gke:
            if !hasValue(regionOrLocation) { fields.append("location") }
            if !hasValue(projectID) { fields.append("project ID") }
        }
        return fields.isEmpty ? nil : fields.joined(separator: ", ")
    }

    private func trimmed(_ value: String) -> String {
        value.trimmingCharacters(in: Self.fieldWhitespace)
    }
}
