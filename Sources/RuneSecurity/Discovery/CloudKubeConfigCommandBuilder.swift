import Foundation
import RuneCore

public struct CloudKubeConfigCommandBuilder: Sendable {
    public init() {}

    public func preview(for request: CloudKubeConfigImportRequest) throws -> CloudKubeConfigCommandPreview {
        let clusterName = try required(request.clusterName, "Cluster name")
        let targetKubeconfigPath = request.targetKubeconfigPath.trimmingCharacters(in: .whitespacesAndNewlines)
        let executable: String
        var arguments: [String]
        var environment: [String: String] = [:]

        switch request.provider {
        case .aks:
            executable = "az"
            let resourceGroup = try required(request.resourceGroup, "Resource group")
            arguments = [
                "aks", "get-credentials",
                "--resource-group", resourceGroup,
                "--name", clusterName
            ]
            if request.overwriteExisting {
                arguments.append("--overwrite-existing")
            }
            appendOptional("--file", targetKubeconfigPath, to: &arguments)
            appendOptional("--subscription", request.profileOrSubscription, to: &arguments)

        case .eks:
            executable = "aws"
            let region = try required(request.regionOrLocation, "Region")
            arguments = [
                "eks", "update-kubeconfig",
                "--region", region,
                "--name", clusterName
            ]
            appendOptional("--kubeconfig", targetKubeconfigPath, to: &arguments)
            appendOptional("--profile", request.profileOrSubscription, to: &arguments)
            appendOptional("--role-arn", request.roleARN, to: &arguments)

        case .gke:
            executable = "gcloud"
            let location = try required(request.regionOrLocation, "Location")
            let projectID = try required(request.projectID, "Project ID")
            arguments = [
                "container", "clusters", "get-credentials",
                clusterName,
                "--location", location,
                "--project", projectID
            ]
            if !targetKubeconfigPath.isEmpty {
                environment["KUBECONFIG"] = targetKubeconfigPath
            }
        }

        return CloudKubeConfigCommandPreview(
            executable: executable,
            arguments: arguments,
            displayCommand: ShellCommandFormatting.displayCommand(executable: executable, arguments: arguments),
            environment: environment
        )
    }

    private func required(_ value: String, _ name: String) throws -> String {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            throw CloudKubeConfigImportError.missingRequiredField(name)
        }
        return trimmed
    }

    private func appendOptional(_ flag: String, _ value: String, to arguments: inout [String]) {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        arguments.append(flag)
        arguments.append(trimmed)
    }

}
