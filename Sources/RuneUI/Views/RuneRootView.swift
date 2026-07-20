import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

/// Layout metrics for regression tests (`RuneRootViewLayoutRegressionTests`) and optional `RUNE_DEBUG_LAYOUT` logging — not the removed step-by-step scenario runner.
public struct RuneRootLayoutSnapshot: Equatable, Sendable {
    public let section: RuneSection
    public let workloadKind: KubeResourceKind
    public let measuredWindowTopInset: CGFloat?
    public let resolvedWindowTopInset: CGFloat
    public let contentMinY: CGFloat?
    public let headerMinY: CGFloat?
    public let detailMinY: CGFloat?
    /// Leading edges of layout probes (window space). Used by UI tests to catch horizontal drift / “offset” when swapping inspectors or editors.
    public let contentMinX: CGFloat?
    public let headerMinX: CGFloat?
    public let detailMinX: CGFloat?

    public init(
        section: RuneSection,
        workloadKind: KubeResourceKind,
        measuredWindowTopInset: CGFloat?,
        resolvedWindowTopInset: CGFloat,
        contentMinY: CGFloat?,
        headerMinY: CGFloat?,
        detailMinY: CGFloat?,
        contentMinX: CGFloat? = nil,
        headerMinX: CGFloat? = nil,
        detailMinX: CGFloat? = nil
    ) {
        self.section = section
        self.workloadKind = workloadKind
        self.measuredWindowTopInset = measuredWindowTopInset
        self.resolvedWindowTopInset = resolvedWindowTopInset
        self.contentMinY = contentMinY
        self.headerMinY = headerMinY
        self.detailMinY = detailMinY
        self.contentMinX = contentMinX
        self.headerMinX = headerMinX
        self.detailMinX = detailMinX
    }
}

private enum RuneRootLayoutProbeKind: Hashable {
    case content
    case header
    case detail
}

private enum RuneRootPaneWidthKind: Hashable {
    case sidebar
    case detail
}

private enum RuneRootLiveDebugScenarioStep: String, CaseIterable {
    case overview
    case workloadPodOverview
    case workloadPodLogs
    case workloadPodExec
    case workloadPodPortForward
    case workloadPodYAMLReadOnly
    case workloadPodYAMLQuickEdit
    case workloadPodYAMLEditorSheet
    case workloadPodDescribe
    case workloadDeploymentOverview
    case workloadDeploymentUnifiedLogs
    case workloadDeploymentRollout
    case workloadDeploymentYAMLReadOnly
    case workloadDeploymentYAMLQuickEdit
    case workloadDeploymentDescribe
    case networkingServiceOverview
    case networkingServiceUnifiedLogs
    case networkingServicePortForward
    case networkingServiceYAMLReadOnly
    case networkingServiceYAMLQuickEdit
    case networkingServiceDescribe
    case configConfigMapPrepare
    case configConfigMapYAMLReadOnly
    case configConfigMapYAMLQuickEdit
    case configConfigMapDescribe
    case storagePVCDescribe
    case storagePVCYAML
    case eventsDetail
    case rbacRole
    case terminal
    case terminalLogs

    var presentsYAMLEditorSheet: Bool {
        switch self {
        case .workloadPodYAMLEditorSheet:
            return true
        default:
            return false
        }
    }

    var requiresExternalInteractionAcknowledgement: Bool {
        switch self {
        case .overview, .workloadPodLogs, .workloadPodDescribe, .configConfigMapPrepare:
            return true
        default:
            return false
        }
    }
}

private struct RuneRootLayoutProbeFrame: Equatable {
    let generation: Int
    let rect: CGRect
}

private struct RuneRootPaneWidthPreferenceKey: PreferenceKey {
    static let defaultValue: [RuneRootPaneWidthKind: CGFloat] = [:]

    static func reduce(
        value: inout [RuneRootPaneWidthKind: CGFloat],
        nextValue: () -> [RuneRootPaneWidthKind: CGFloat]
    ) {
        value.merge(nextValue(), uniquingKeysWith: { _, new in new })
    }
}

private struct RuneRootPaneWidthReporter: View {
    let kind: RuneRootPaneWidthKind

    var body: some View {
        GeometryReader { proxy in
            Color.clear.preference(
                key: RuneRootPaneWidthPreferenceKey.self,
                value: [kind: proxy.size.width]
            )
        }
    }
}

private enum RuneRootLayoutDebug {
    static let coordinateSpaceName = "RuneRootLayoutSpace"
    static let isEnabled = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT"] == "1"
    static let liveScenarioEnabled = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_LIVE_SCENARIO"] == "1"
    static let liveScenarioExitWhenDone = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_LIVE_SCENARIO_EXIT"] == "1"
    static let liveScenarioContextName = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_CONTEXT"]?
        .trimmingCharacters(in: .whitespacesAndNewlines)
    static let liveScenarioNamespace = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_NAMESPACE"]?
        .trimmingCharacters(in: .whitespacesAndNewlines)
    static let liveScenarioInteractionAcknowledgementDirectory =
        ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_INTERACTION_ACK_DIR"]?
            .trimmingCharacters(in: .whitespacesAndNewlines)
    static let initialDetailWidth: CGFloat? = {
        guard let rawValue = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_DETAIL_WIDTH"]?
            .trimmingCharacters(in: .whitespacesAndNewlines),
              let value = Double(rawValue),
              value > 0 else {
            return nil
        }
        return CGFloat(value)
    }()
    static let liveScenarioPodDwellNanoseconds: UInt64 = {
        guard let rawValue = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_POD_DWELL_MS"]?
            .trimmingCharacters(in: .whitespacesAndNewlines),
              let milliseconds = UInt64(rawValue),
              milliseconds > 0 else {
            return 3_500_000_000
        }
        return milliseconds * 1_000_000
    }()
    static let liveScenarioSnapshotHoldNanoseconds: UInt64 = {
        guard let rawValue = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_SNAPSHOT_HOLD_MS"]?
            .trimmingCharacters(in: .whitespacesAndNewlines),
              let milliseconds = UInt64(rawValue),
              milliseconds > 0 else {
            return 1_800_000_000
        }
        return milliseconds * 1_000_000
    }()

    static func log(
        _ snapshot: RuneRootLayoutSnapshot,
        shellVariant: RuneRootShellVariant,
        inlineEditorImplementation: ManifestInlineEditorImplementation
    ) {
        guard isEnabled else { return }

        let measuredTopInset = snapshot.measuredWindowTopInset.map { String(format: "%.1f", $0) } ?? "nil"
        let resolvedTopInset = String(format: "%.1f", snapshot.resolvedWindowTopInset)
        let contentFrame = String(format: "(%.1f,%.1f)", snapshot.contentMinX ?? -1, snapshot.contentMinY ?? -1)
        let headerFrame = String(format: "(%.1f,%.1f)", snapshot.headerMinX ?? -1, snapshot.headerMinY ?? -1)
        let detailFrame = String(format: "(%.1f,%.1f)", snapshot.detailMinX ?? -1, snapshot.detailMinY ?? -1)
        RuneLoggers.layout.debug(
            "shell=\(shellVariant.debugLabel, privacy: .public) editor=\(inlineEditorImplementation.debugLabel, privacy: .public) section=\(snapshot.section.rawValue, privacy: .public) kind=\(snapshot.workloadKind.kubernetesResourceName, privacy: .public) measuredTopInset=\(measuredTopInset, privacy: .public) resolvedTopInset=\(resolvedTopInset, privacy: .public) content=\(contentFrame, privacy: .public) header=\(headerFrame, privacy: .public) detail=\(detailFrame, privacy: .public)"
        )
    }

    static func logScenario(_ step: RuneRootLiveDebugScenarioStep, status: String, detail: String = "") {
        guard isEnabled || liveScenarioEnabled else { return }
        RuneLoggers.layout.debug(
            "scenario step=\(step.rawValue, privacy: .public) status=\(status, privacy: .public) detail=\(detail, privacy: .private)"
        )
        if liveScenarioEnabled {
            let line = "[Rune] scenario step=\(step.rawValue) status=\(status) detail=\(detail)\n"
            if let data = line.data(using: .utf8) {
                FileHandle.standardError.write(data)
            }
        }
    }

}

private struct RuneRootLayoutFramePreferenceKey: PreferenceKey {
    static let defaultValue: [RuneRootLayoutProbeKind: RuneRootLayoutProbeFrame] = [:]

    static func reduce(
        value: inout [RuneRootLayoutProbeKind: RuneRootLayoutProbeFrame],
        nextValue: () -> [RuneRootLayoutProbeKind: RuneRootLayoutProbeFrame]
    ) {
        value.merge(nextValue(), uniquingKeysWith: { _, new in new })
    }
}

private struct RuneRootLayoutProbe: View {
    let kind: RuneRootLayoutProbeKind
    let generation: Int

    var body: some View {
        GeometryReader { proxy in
            Color.clear.preference(
                key: RuneRootLayoutFramePreferenceKey.self,
                value: [kind: RuneRootLayoutProbeFrame(
                    generation: generation,
                    rect: proxy.frame(in: .named(RuneRootLayoutDebug.coordinateSpaceName))
                )]
            )
        }
    }
}

enum PodInspectorTab: String, CaseIterable, Identifiable {
    case overview
    case logs
    case exec
    case portForward
    case describe
    case yaml

    var id: String { rawValue }

    var title: String {
        switch self {
        case .overview: return "Overview"
        case .logs: return "Logs"
        case .exec: return "Exec"
        case .portForward: return "Port Forward"
        case .describe: return "Describe"
        case .yaml: return "YAML"
        }
    }
}

enum TerminalInspectorTab: String, CaseIterable, Identifiable {
    case commands
    case logs
    case yaml

    var id: String { rawValue }

    var title: String {
        switch self {
        case .commands: return "Commands"
        case .logs: return "Logs"
        case .yaml: return "YAML"
        }
    }
}

enum ServiceInspectorTab: String, CaseIterable, Identifiable {
    case overview
    case unifiedLogs
    case portForward
    case describe
    case yaml

    var id: String { rawValue }

    var title: String {
        switch self {
        case .overview: return "Overview"
        case .unifiedLogs: return "Unified Logs"
        case .portForward: return "Port Forward"
        case .describe: return "Describe"
        case .yaml: return "YAML"
        }
    }
}

enum DeploymentInspectorTab: String, CaseIterable, Identifiable {
    case overview
    case unifiedLogs
    case rollout
    case describe
    case yaml

    var id: String { rawValue }

    var title: String {
        switch self {
        case .overview: return "Overview"
        case .unifiedLogs: return "Unified Logs"
        case .rollout: return "Rollout"
        case .describe: return "Describe"
        case .yaml: return "YAML"
        }
    }
}

enum GenericResourceManifestTab: String, CaseIterable, Identifiable {
    case overview
    case describe
    case yaml

    var id: String { rawValue }

    var title: String {
        switch self {
        case .overview: return "Overview"
        case .describe: return "Describe"
        case .yaml: return "YAML"
        }
    }
}

enum RuneAddClusterProviderActionLayout {
    static let dialogWidth: CGFloat = RuneUILayoutMetrics.standardDialogWidth
    static let horizontalPadding: CGFloat = RuneUILayoutMetrics.dialogContentPadding * 2
    static let columnSpacing: CGFloat = 8
    static let minimumButtonWidth: CGFloat = 150

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

private typealias RuneAddClusterProvider = AddClusterProviderIdentifier

private extension AddClusterProviderIdentifier {
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

enum HelmInspectorTab: String, CaseIterable, Identifiable, Sendable {
    case overview
    case values
    case manifest
    case history

    var id: String { rawValue }

    var title: String {
        switch self {
        case .overview: return "Overview"
        case .values: return "Values"
        case .manifest: return "Manifest"
        case .history: return "History"
        }
    }
}

enum HelmBrowserTab: String, CaseIterable, Identifiable, Sendable {
    case releases
    case operatorResources

    var id: String { rawValue }

    var resourceListFamily: RuneResourceListFamily {
        switch self {
        case .releases: return .helmReleases
        case .operatorResources: return .operatorResources
        }
    }

    var title: String {
        switch self {
        case .releases: return "Releases"
        case .operatorResources: return "Operator resources"
        }
    }
}

enum RuneHelmInspectorMode: Equatable, Sendable {
    case none
    case release
    case operatorResource

    static func resolve(hasRelease: Bool, hasOperatorResource: Bool) -> RuneHelmInspectorMode {
        if hasOperatorResource {
            return .operatorResource
        }
        if hasRelease {
            return .release
        }
        return .none
    }

    var browserTab: HelmBrowserTab? {
        switch self {
        case .none: return nil
        case .release: return .releases
        case .operatorResource: return .operatorResources
        }
    }
}

enum RuneInspectorRefreshRoute: Equatable, Sendable {
    case currentView
    case resourceInspector
    case logs
    case helmInspector
}

struct RuneInspectorRefreshRouting {
    static func route(
        section: RuneSection,
        workloadKind: KubeResourceKind,
        podTab: PodInspectorTab,
        deploymentTab: DeploymentInspectorTab,
        serviceTab: ServiceInspectorTab,
        genericTab: GenericResourceManifestTab,
        helmTab: HelmInspectorTab,
        helmMode: RuneHelmInspectorMode
    ) -> RuneInspectorRefreshRoute {
        switch section {
        case .workloads:
            switch workloadKind {
            case .pod:
                switch podTab {
                case .logs: return .logs
                case .describe, .yaml: return .resourceInspector
                case .overview, .exec, .portForward: return .currentView
                }
            case .deployment:
                switch deploymentTab {
                case .unifiedLogs: return .logs
                case .describe, .yaml: return .resourceInspector
                case .overview, .rollout: return .currentView
                }
            default:
                return genericRoute(for: genericTab)
            }

        case .networking:
            if workloadKind == .service {
                switch serviceTab {
                case .unifiedLogs: return .logs
                case .describe, .yaml: return .resourceInspector
                case .overview, .portForward: return .currentView
                }
            }
            return genericRoute(for: genericTab)

        case .config, .storage, .rbac:
            return genericRoute(for: genericTab)

        case .helm:
            switch helmMode {
            case .operatorResource:
                return genericTab == .overview ? .currentView : .helmInspector
            case .release:
                return helmTab == .overview ? .currentView : .helmInspector
            case .none:
                return .currentView
            }

        case .overview, .events, .terminal:
            return .currentView
        }
    }

    private static func genericRoute(
        for tab: GenericResourceManifestTab
    ) -> RuneInspectorRefreshRoute {
        tab == .overview ? .currentView : .resourceInspector
    }
}

private struct RuneHelmSelectionIdentity: Equatable {
    let releaseID: String?
    let operatorResourceID: String?
}

private struct RuneHelmSelectionSyncModifier: ViewModifier {
    let restoreRequest: SavedWorkspaceInspectorRestoreRequest?
    let selectionIdentity: RuneHelmSelectionIdentity
    let applyRestore: (SavedWorkspaceInspectorState?) -> Void
    let syncBrowserSelection: () -> Void

    func body(content: Content) -> some View {
        content
            .onChange(of: restoreRequest) { _, request in
                applyRestore(request?.inspectorState)
                syncBrowserSelection()
            }
            .onChange(of: selectionIdentity) { _, _ in
                syncBrowserSelection()
            }
    }
}

private extension RuneSection {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .overview: return string(.overview)
        case .workloads: return string(.workloads)
        case .networking: return string(.networking)
        case .storage: return string(.storage)
        case .config: return string(.config)
        case .rbac: return string(.rbac)
        case .events: return string(.events)
        case .helm: return string(.helm)
        case .terminal: return string(.terminal)
        }
    }
}

private extension KubeResourceKind {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .pod: return string(.pods)
        case .deployment: return string(.deployments)
        case .statefulSet: return string(.statefulSets)
        case .daemonSet: return string(.daemonSets)
        case .job: return string(.jobs)
        case .cronJob: return string(.cronJobs)
        case .replicaSet: return string(.replicaSets)
        case .service: return string(.services)
        case .endpoint: return string(.endpoints)
        case .ingress: return string(.ingresses)
        case .configMap: return string(.configMaps)
        case .secret: return string(.secrets)
        case .node: return string(.nodes)
        case .event: return string(.events)
        case .serviceAccount: return string(.serviceAccounts)
        case .role: return string(.roles)
        case .roleBinding: return string(.roleBindings)
        case .clusterRole: return string(.clusterRoles)
        case .clusterRoleBinding: return string(.clusterRoleBindings)
        case .persistentVolumeClaim: return string(.persistentVolumeClaims)
        case .persistentVolume: return string(.persistentVolumes)
        case .storageClass: return string(.storageClasses)
        case .horizontalPodAutoscaler: return string(.horizontalPodAutoscalers)
        case .networkPolicy: return string(.networkPolicies)
        }
    }
}

private extension PodInspectorTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .overview: return string(.overview)
        case .logs: return string(.logs)
        case .exec: return string(.exec)
        case .portForward: return string(.portForward)
        case .describe: return string(.describe)
        case .yaml: return string(.yaml)
        }
    }
}

private extension TerminalInspectorTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .commands: return string(.commands)
        case .logs: return string(.logs)
        case .yaml: return string(.yaml)
        }
    }
}

private extension ServiceInspectorTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .overview: return string(.overview)
        case .unifiedLogs: return string(.unifiedLogs)
        case .portForward: return string(.portForward)
        case .describe: return string(.describe)
        case .yaml: return string(.yaml)
        }
    }
}

private extension DeploymentInspectorTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .overview: return string(.overview)
        case .unifiedLogs: return string(.unifiedLogs)
        case .rollout: return string(.rollout)
        case .describe: return string(.describe)
        case .yaml: return string(.yaml)
        }
    }
}

private extension GenericResourceManifestTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .overview: return string(.overview)
        case .describe: return string(.describe)
        case .yaml: return string(.yaml)
        }
    }
}

private extension HelmInspectorTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .overview: return string(.overview)
        case .values: return string(.values)
        case .manifest: return string(.manifest)
        case .history: return string(.history)
        }
    }
}

private extension HelmBrowserTab {
    func localizedTitle(_ string: (RuneLocalizedStringKey) -> String) -> String {
        switch self {
        case .releases: return string(.releases)
        case .operatorResources: return string(.operatorResources)
        }
    }
}

enum PodTableLayout {
    static let metricsSpacing: CGFloat = 10
    static let rowHorizontalPadding: CGFloat = 10
    static let rowVerticalPadding: CGFloat = 5
    static let listRowEdgeInset: CGFloat = 4
    static let nameColumnDefaultWidth: CGFloat = 260
    static let nameColumnMinimumWidth: CGFloat = 104
    static let nameColumnMaximumWidth: CGFloat = 820
    static let nameColumnResizeHandleWidth: CGFloat = 14
    static let cpuWidth: CGFloat = 58
    static let memoryWidth: CGFloat = 64
    static let restartsWidth: CGFloat = 72
    static let ageWidth: CGFloat = 54
    static let statusTextWidth: CGFloat = 94
    static let statusHorizontalPadding: CGFloat = 8
    static let statusTotalWidth: CGFloat = statusTextWidth + (statusHorizontalPadding * 2)
    static let headerHorizontalInset: CGFloat = rowHorizontalPadding
    static let selectionColumnWidth: CGFloat = 30
    static let favoriteColumnWidth: CGFloat = 34
    static let minimumScrollableWidth: CGFloat = 620
    /// Space between column headers and first row — enough to avoid a cramped look without excess air.
    static let headerBottomSpacing: CGFloat = 10

    static func clampedNameColumnWidth(_ width: CGFloat) -> CGFloat {
        let pixelAlignedWidth = width.rounded(.toNearestOrAwayFromZero)
        return min(nameColumnMaximumWidth, max(nameColumnMinimumWidth, pixelAlignedWidth))
    }

    static func minimumScrollableWidth(nameColumnWidth: CGFloat) -> CGFloat {
        max(
            minimumScrollableWidth,
            rowHorizontalPadding * 2
                + selectionColumnWidth
                + nameColumnFrameWidth(nameColumnWidth)
                + metricsColumnGroupWidth
                + statusTotalWidth
                + favoriteColumnWidth
                + (metricsSpacing * 4)
        )
    }

    static func nameColumnFrameWidth(_ width: CGFloat) -> CGFloat {
        clampedNameColumnWidth(width) + nameColumnResizeHandleWidth
    }

    static var metricsColumnGroupWidth: CGFloat {
        cpuWidth
            + memoryWidth
            + restartsWidth
            + ageWidth
            + (metricsSpacing * 3)
    }

    static var defaultAppKitTableWidth: CGFloat {
        selectionColumnWidth
            + nameColumnDefaultWidth
            + cpuWidth
            + memoryWidth
            + restartsWidth
            + ageWidth
            + statusTotalWidth
            + favoriteColumnWidth
    }

    static func resizePreviewWidth(committedWidth: CGFloat, translation: CGFloat) -> CGFloat {
        clampedNameColumnWidth(committedWidth + translation)
    }

    static func resizeCommitWidth(committedWidth: CGFloat, translation: CGFloat) -> CGFloat {
        clampedNameColumnWidth(committedWidth + translation)
    }
}

enum RuneRootKeyboardPane: CaseIterable, Equatable {
    case sidebarSections
    case sidebarContexts
    case content
    case detail
}

struct RuneRootKeyboardPaneNavigation {
    static func availablePanes(
        sidebarVisible: Bool,
        detailVisible: Bool,
        skipsClusterPane: Bool
    ) -> [RuneRootKeyboardPane] {
        var panes: [RuneRootKeyboardPane] = []
        if sidebarVisible {
            panes.append(.sidebarSections)
            if !skipsClusterPane {
                panes.append(.sidebarContexts)
            }
        }
        panes.append(.content)
        if detailVisible {
            panes.append(.detail)
        }
        return panes
    }

    static func advanced(
        from current: RuneRootKeyboardPane,
        forward: Bool,
        in panes: [RuneRootKeyboardPane]
    ) -> RuneRootKeyboardPane {
        guard let index = panes.firstIndex(of: current) else {
            return panes.first ?? .content
        }
        if forward {
            return panes[(index + 1) % panes.count]
        }
        return panes[(index + panes.count - 1) % panes.count]
    }
}

struct RuneRootKeyboardWindowScope {
    static func owns(
        eventWindowNumber: Int,
        workspaceWindowNumber: Int?,
        keyWindowNumber: Int?
    ) -> Bool {
        guard let workspaceWindowNumber else { return false }
        if eventWindowNumber == workspaceWindowNumber {
            return true
        }
        // Quartz/Accessibility-posted key events can arrive without a window number.
        // In that case, only the workspace that owns the key window may handle them.
        return eventWindowNumber == 0 && keyWindowNumber == workspaceWindowNumber
    }
}

private enum RuneRootTextInputFocus: Hashable {
    case contextSearch
    case resourceFilter
}

/// Keeps the hosting window identity out of SwiftUI's render state. Window attachment can
/// happen after the first layout pass; publishing that identity as `@State` needlessly
/// invalidates the entire three-pane workspace while it is settling.
private final class RuneWorkspaceWindowReference {
    var windowNumber: Int?
    var measuredTopInset: CGFloat?
}

struct RuneRootKeyboardNavigationContext {
    let commandPalettePresented: Bool
    let yamlEditorSheetPresented: Bool
    let yamlManifestIsEditing: Bool
    let kubeConfigImportReviewPresented: Bool
    let addClusterPopoverPresented: Bool
    let addClusterProviderPresented: Bool
    let manualNamespaceSheetPresented: Bool
    let pendingWriteConfirmationPresented: Bool
    let launchExperiencePresented: Bool
    let appSheetAttached: Bool
    let editableTextResponderActive: Bool

    var hasBlockingPresentation: Bool {
        commandPalettePresented
            || yamlEditorSheetPresented
            || yamlManifestIsEditing
            || kubeConfigImportReviewPresented
            || addClusterPopoverPresented
            || addClusterProviderPresented
            || manualNamespaceSheetPresented
            || pendingWriteConfirmationPresented
            || launchExperiencePresented
            || appSheetAttached
    }

    var isSuspended: Bool {
        hasBlockingPresentation || editableTextResponderActive
    }
}

public struct RuneRootView: View {
    @Environment(\.openSettings) private var openSettings
    @ObservedObject private var viewModel: RuneAppViewModel

    private let onLayoutSnapshotChange: ((RuneRootLayoutSnapshot) -> Void)?
    private let debugDisableBootstrap: Bool
    private let debugDisableLayoutPersistence: Bool
    private let forcedShellVariant: RuneRootShellVariant?
    private let forcedManifestInlineEditorImplementation: ManifestInlineEditorImplementation?
    private let forcedInitialSidebarWidth: Double?
    private let forcedInitialDetailWidth: Double?
    private let workspaceChromeMountDelayNanoseconds: UInt64 = 80_000_000

    @AppStorage(RuneSettingsKeys.layoutSidebarWidth) private var persistedSidebarWidth = 280.0
    @AppStorage(RuneSettingsKeys.layoutDetailWidth) private var persistedDetailWidth = 440.0
    @AppStorage(RuneSettingsKeys.layoutPodNameColumnWidth) private var persistedPodNameColumnWidth = Double(PodTableLayout.nameColumnDefaultWidth)
    @AppStorage(RuneSettingsKeys.persistTerminalWorkspaceState) private var persistTerminalWorkspaceState = false
    @AppStorage(RuneSettingsKeys.showHoverTooltips) private var showHoverTooltips = true
    @AppStorage(RuneSettingsKeys.simpleMode) private var simpleMode = false
    @AppStorage(RuneSettingsKeys.skipClusterOnTabNavigationFromSections) private var skipClusterOnTabNavigationFromSections = false
    @AppStorage(RuneSettingsKeys.appearanceTheme) private var appearanceThemeRaw = RuneSettingsKeys.appearanceThemeDefault
    @AppStorage(RuneSettingsKeys.interfaceLanguage) private var interfaceLanguageRaw =
        RuneSettingsKeys.interfaceLanguageDefault
    @State private var layoutGeneration = 0
    @State private var layoutProbeFrames: [RuneRootLayoutProbeKind: CGRect] = [:]
    @State private var lastLayoutSnapshot: RuneRootLayoutSnapshot?
    @State private var podInspectorTab: PodInspectorTab = .overview
    @State private var serviceInspectorTab: ServiceInspectorTab = .overview
    @State private var deploymentInspectorTab: DeploymentInspectorTab = .overview
    @State private var showsHistoricalDeploymentReplicaSets = false
    @State private var helmInspectorTab: HelmInspectorTab = .overview
    @State private var helmBrowserTab: HelmBrowserTab = .releases
    @State private var genericResourceManifestTab: GenericResourceManifestTab = .overview
    @State private var yamlManifestIsEditing = false
    @State private var isYAMLEditorSheetPresented = false
    @State private var terminalShellPodID = ""
    @State private var terminalPortForwardPodID = ""
    @State private var terminalLogTabState = TerminalPodLogTabState()
    @State private var terminalInspectorTab: TerminalInspectorTab = .commands
    @State private var hasRestoredTerminalWorkspaceState = false
    @State private var liveDebugScenarioStarted = false
    @State private var keyboardPaneFocus: RuneRootKeyboardPane = .sidebarSections
    @State private var isGenericResourceComparisonPresented = false
    @State private var didCopyGenericResourceComparison = false
    @State private var overviewCardSelectionIndex = 0
    @State private var expandedOverviewInsightPanels: Set<OverviewInsightPanelID> = []
    @State private var localKeyEventMonitor: Any?
    @State private var workspaceWindowReference = RuneWorkspaceWindowReference()
    @State private var addClusterPopoverPresented = false
    @State private var selectedAddClusterProvider: RuneAddClusterProvider?
    @State private var cloudCredentialDraft = CloudCredentialDraft()
    @State private var addClusterNativeContextOptions: [AddClusterNativeContextOption] = []
    @State private var selectedAddClusterNativeContextBindingID: String?
    @State private var connectedAddClusterNativeContextBindingIDs: Set<String> = []
    @State private var isCheckingAddClusterNativeProfiles = false
    @State private var addClusterNativeContextAnalysisMessage: String?
    @State private var isManualAddClusterExpanded = false
    @State private var isAddClusterProviderCommandDetailsExpanded = false
    @State private var isAddClusterProviderLoginOutputExpanded = false
    @State private var hasMountedWorkspaceChrome = false
    @State private var isManualNamespaceSheetPresented = false
    @State private var manualNamespaceInput = ""
    @State private var isAuthDoctorPanelExpanded = false
    @State private var isConfirmingPendingWriteActionFromDialog = false
    @FocusState private var textInputFocus: RuneRootTextInputFocus?

    private var interfaceLanguage: RuneLanguage {
        RuneLanguage.resolved(interfaceLanguageRaw)
    }

    private func appString(_ key: RuneLocalizedStringKey) -> String {
        RuneLocalizedStrings.shared.string(key, language: interfaceLanguage)
    }

    public init(
        viewModel: RuneAppViewModel = RuneAppViewModel(),
        onLayoutSnapshotChange: ((RuneRootLayoutSnapshot) -> Void)? = nil
    ) {
        self.init(
            viewModel: viewModel,
            onLayoutSnapshotChange: onLayoutSnapshotChange,
            debugDisableBootstrap: false,
            initialPodInspectorTab: .overview,
            initialServiceInspectorTab: .overview,
            initialDeploymentInspectorTab: .overview,
            initialGenericResourceManifestTab: .overview,
            shellVariant: nil,
            manifestInlineEditorImplementation: nil
        )
    }

    init(
        viewModel: RuneAppViewModel,
        onLayoutSnapshotChange: ((RuneRootLayoutSnapshot) -> Void)?,
        debugDisableBootstrap: Bool,
        debugDisableLayoutPersistence: Bool? = nil,
        initialPodInspectorTab: PodInspectorTab = .overview,
        initialServiceInspectorTab: ServiceInspectorTab = .overview,
        initialDeploymentInspectorTab: DeploymentInspectorTab = .overview,
        initialGenericResourceManifestTab: GenericResourceManifestTab = .overview,
        shellVariant: RuneRootShellVariant? = nil,
        manifestInlineEditorImplementation: ManifestInlineEditorImplementation? = nil,
        initialYAMLInlineEditing: Bool = false,
        initialSidebarWidthOverride: Double? = nil,
        initialDetailWidthOverride: Double? = nil
    ) {
        self.viewModel = viewModel
        self.onLayoutSnapshotChange = onLayoutSnapshotChange
        self.debugDisableBootstrap = debugDisableBootstrap
        self.debugDisableLayoutPersistence = debugDisableLayoutPersistence ?? debugDisableBootstrap
        self.forcedShellVariant = shellVariant
        self.forcedManifestInlineEditorImplementation = manifestInlineEditorImplementation
        self.forcedInitialSidebarWidth = initialSidebarWidthOverride
        self.forcedInitialDetailWidth = initialDetailWidthOverride
        _podInspectorTab = State(initialValue: initialPodInspectorTab)
        _serviceInspectorTab = State(initialValue: initialServiceInspectorTab)
        _deploymentInspectorTab = State(initialValue: initialDeploymentInspectorTab)
        _genericResourceManifestTab = State(initialValue: initialGenericResourceManifestTab)
        _yamlManifestIsEditing = State(initialValue: initialYAMLInlineEditing)
    }

    public var body: some View {
        ZStack(alignment: .topLeading) {
            background
            WindowChromeConfigurator(
                onMeasuredTopInsetChange: { inset in
                    workspaceWindowReference.measuredTopInset = inset
                },
                onOwningWindowChange: { windowNumber in
                    workspaceWindowReference.windowNumber = windowNumber
                }
            )
                .frame(width: 0, height: 0)
            keyboardNavigationBridge

            GeometryReader { geometry in
                let resolvedTopInset = RuneUILayoutMetrics.resolvedWindowContentTopInset(
                    measuredInset: workspaceWindowReference.measuredTopInset
                )
                let viewportHeight = max(0, geometry.size.height - resolvedTopInset)

                if shouldMountWorkspaceChrome {
                    configuredMainSplitContainer
                        .frame(width: geometry.size.width, height: viewportHeight, alignment: .topLeading)
                        .offset(y: resolvedTopInset)
                        .clipped()
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                }
            }

            if shouldShowLaunchExperience {
                launchExperienceOverlay
                    .zIndex(10)
            }
        }
        .coordinateSpace(name: RuneRootLayoutDebug.coordinateSpaceName)
        .onAppear {
            handleRootAppear()
        }
        .onDisappear {
            handleRootDisappear()
        }
        .sheet(isPresented: kubeConfigImportReviewPresentedBinding) {
            if let review = pendingKubeConfigImportReview {
                KubeConfigImportReviewSheet(
                    review: review,
                    duplicateHandlingChoice: $viewModel.kubeConfigDuplicateHandlingChoice,
                    metadataDrafts: viewModel.kubeConfigImportContextMetadataDrafts,
                    isCommitInProgress: viewModel.isCommittingKubeConfigImport,
                    canConfirm: viewModel.canConfirmKubeConfigImport,
                    onUpdateMetadata: { contextName, metadata in
                        viewModel.setKubeConfigImportContextMetadata(
                            contextName: contextName,
                            alias: metadata.alias,
                            colorKey: metadata.colorKey,
                            iconName: metadata.iconName,
                            tags: metadata.tags,
                            group: metadata.group
                        )
                    },
                    onConfirm: viewModel.confirmKubeConfigImport,
                    onCancel: viewModel.cancelKubeConfigImport
                )
            }
        }
        .onPreferenceChange(RuneRootLayoutFramePreferenceKey.self) { frames in
            layoutProbeFrames = frames.compactMapValues { frame in
                guard frame.generation == layoutGeneration else { return nil }
                return frame.rect
            }
            emitLayoutSnapshotIfNeeded()
        }
        .onChange(of: viewModel.state.selectedSection) { _, section in
            advanceLayoutGeneration()
            guard section == .overview, !overviewCardModules.isEmpty else { return }
            overviewCardSelectionIndex = min(overviewCardSelectionIndex, overviewCardModules.count - 1)
        }
        .onChange(of: viewModel.state.selectedWorkloadKind) { _, _ in
            advanceLayoutGeneration()
        }
        .modifier(helmSelectionSyncModifier)
        .onChange(of: podInspectorTab) { _, tab in
            syncSavedWorkspaceInspectorState()
            if tab == .logs {
                viewModel.reloadLogsForSelection()
            }
        }
        .onChange(of: deploymentInspectorTab) { _, tab in
            syncSavedWorkspaceInspectorState()
            if tab == .unifiedLogs {
                viewModel.reloadLogsForSelection()
            } else if tab == .rollout, simpleMode {
                viewModel.refreshReplicaSetsForCurrentNamespace()
            }
        }
        .onChange(of: serviceInspectorTab) { _, tab in
            syncSavedWorkspaceInspectorState()
            if tab == .unifiedLogs {
                viewModel.reloadLogsForSelection()
            }
        }
        .onChange(of: genericResourceManifestTab) { _, _ in
            syncSavedWorkspaceInspectorState()
        }
        .onChange(of: helmInspectorTab) { _, _ in
            syncSavedWorkspaceInspectorState()
        }
        .onChange(of: terminalInspectorTab) { _, _ in
            syncSavedWorkspaceInspectorState()
        }
        .onChange(of: yamlManifestIsEditing) { _, _ in
            syncSavedWorkspaceInspectorState()
        }
        .modifier(terminalWorkspacePersistenceLifecycleModifier)
        .onChange(of: viewModel.isSidebarVisible) { _, isVisible in
            if !isVisible, keyboardPaneFocus == .sidebarSections || keyboardPaneFocus == .sidebarContexts {
                keyboardPaneFocus = .content
            }
        }
        .onChange(of: viewModel.isDetailPaneVisible) { _, isVisible in
            if !isVisible, keyboardPaneFocus == .detail {
                keyboardPaneFocus = .content
            }
        }
        .onChange(of: skipClusterOnTabNavigationFromSections) { _, shouldSkip in
            if shouldSkip, keyboardPaneFocus == .sidebarContexts {
                keyboardPaneFocus = .sidebarSections
            }
        }
        .onPreferenceChange(RuneRootPaneWidthPreferenceKey.self) { paneWidths in
            persistPaneWidthsIfNeeded(paneWidths)
        }
        .animation(.easeInOut(duration: 0.2), value: viewModel.isProductionContext)
        .animation(.easeOut(duration: 0.16), value: shouldShowLaunchExperience)
        .runeAppearanceTheme(activeAppearanceTheme)
    }

    private var activeAppearanceTheme: RuneResolvedTheme {
        RuneAppearanceTheme.resolved(appearanceThemeRaw)
    }

    private var helmSelectionSyncModifier: RuneHelmSelectionSyncModifier {
        RuneHelmSelectionSyncModifier(
            restoreRequest: viewModel.savedWorkspaceInspectorRestoreRequest,
            selectionIdentity: RuneHelmSelectionIdentity(
                releaseID: viewModel.state.selectedHelmRelease?.id,
                operatorResourceID: viewModel.state.selectedOperatorResource?.id
            ),
            applyRestore: applySavedWorkspaceInspectorState,
            syncBrowserSelection: syncHelmBrowserTabWithSelection
        )
    }

    private var kubeConfigImportReviewPresentedBinding: Binding<Bool> {
        Binding(
            get: { viewModel.isKubeConfigImportConfirmationPending },
            set: { isPresented in
                if !isPresented {
                    viewModel.cancelKubeConfigImport()
                }
            }
        )
    }

    private var pendingKubeConfigImportReview: KubeConfigImportReview? {
        KubeConfigImportReviewAggregator.aggregate(viewModel.kubeConfigImportReviews)
    }

    private var shouldShowLaunchExperience: Bool {
        !debugDisableBootstrap && viewModel.isLaunchExperienceVisible
    }

    private var shouldMountWorkspaceChrome: Bool {
        debugDisableBootstrap || hasMountedWorkspaceChrome || !shouldShowLaunchExperience
    }

    private func handleRootAppear() {
        keyboardPaneFocus = .sidebarSections
        textInputFocus = nil
        DispatchQueue.main.async {
            NSApp.keyWindow?.makeFirstResponder(nil)
        }
        installLocalKeyboardMonitorIfNeeded()
        scheduleWorkspaceChromeMount()
        if RuneRootLayoutDebug.isEnabled {
            RuneLoggers.layout.debug(
                "configured shell=\(resolvedShellVariant.debugLabel, privacy: .public) editor=\(resolvedManifestInlineEditorImplementation.debugLabel, privacy: .public)"
            )
        }
        startLiveDebugScenarioIfNeeded()
        syncSavedWorkspaceInspectorState()
        syncHelmBrowserTabWithSelection()
        restoreTerminalWorkspaceStateIfNeeded()
        handlePendingLaunchActionIfNeeded()
        guard !debugDisableBootstrap else { return }
        viewModel.bootstrapIfNeeded()
    }

    private func restoreTerminalWorkspaceStateIfNeeded() {
        guard persistTerminalWorkspaceState, !hasRestoredTerminalWorkspaceState else { return }
        hasRestoredTerminalWorkspaceState = true
        guard let snapshot = JSONTerminalWorkspaceStateStore().loadTerminalWorkspaceState() else { return }

        terminalShellPodID = snapshot.shellPodID ?? terminalShellPodID
        terminalPortForwardPodID = snapshot.portForwardPodID ?? terminalPortForwardPodID
        if let inspectorTabID = snapshot.inspectorTabID,
           let tab = TerminalInspectorTab(rawValue: inspectorTabID) {
            terminalInspectorTab = tab
        }
        terminalLogTabState.restore(
            tabs: snapshot.logTabs,
            activeTabID: snapshot.activeLogTabID,
            selectedPodID: snapshot.selectedLogPodID
        )

        if viewModel.state.terminalSessions.isEmpty {
            for session in snapshot.sessions.map(\.restoredSession) {
                viewModel.state.setTerminalSession(session)
            }
            if let activeID = snapshot.activeSessionID {
                viewModel.state.selectTerminalSession(id: activeID)
            }
        }
    }

    private func persistTerminalWorkspaceStateIfNeeded() {
        guard persistTerminalWorkspaceState else { return }
        JSONTerminalWorkspaceStateStore().saveTerminalWorkspaceState(currentTerminalWorkspaceStateSnapshot)
    }

    private var terminalWorkspacePersistenceLifecycleModifier: TerminalWorkspacePersistenceLifecycleModifier {
        TerminalWorkspacePersistenceLifecycleModifier(
            isEnabled: persistTerminalWorkspaceState,
            terminalInspectorTab: terminalInspectorTab,
            terminalShellPodID: terminalShellPodID,
            terminalPortForwardPodID: terminalPortForwardPodID,
            terminalLogTabState: terminalLogTabState,
            terminalSessions: viewModel.state.terminalSessions,
            activeTerminalSessionID: viewModel.state.activeTerminalSessionID,
            onEnable: {
                restoreTerminalWorkspaceStateIfNeeded()
                persistTerminalWorkspaceStateIfNeeded()
            },
            onDisable: {
                JSONTerminalWorkspaceStateStore().clearTerminalWorkspaceState()
            },
            onPersist: persistTerminalWorkspaceStateIfNeeded
        )
    }

    private var currentTerminalWorkspaceStateSnapshot: TerminalWorkspaceStateSnapshot {
        TerminalWorkspaceStateSnapshot(
            sessions: viewModel.state.terminalSessions.map(TerminalWorkspaceSessionSnapshot.init(session:)),
            activeSessionID: viewModel.state.activeTerminalSessionID,
            logTabs: terminalLogTabState.snapshotTabs,
            activeLogTabID: terminalLogTabState.activeTabID,
            selectedLogPodID: terminalLogTabState.selectedPodID,
            shellPodID: terminalShellPodID,
            portForwardPodID: terminalPortForwardPodID,
            inspectorTabID: terminalInspectorTab.rawValue
        )
    }

    private var currentSavedWorkspaceInspectorState: SavedWorkspaceInspectorState {
        SavedWorkspaceInspectorState(
            podTabID: podInspectorTab.rawValue,
            serviceTabID: serviceInspectorTab.rawValue,
            deploymentTabID: deploymentInspectorTab.rawValue,
            genericManifestTabID: genericResourceManifestTab.rawValue,
            helmTabID: helmInspectorTab.rawValue,
            terminalTabID: terminalInspectorTab.rawValue,
            isYAMLInlineEditing: yamlManifestIsEditing
        )
    }

    private var selectedHelmInspectorMode: RuneHelmInspectorMode {
        RuneHelmInspectorMode.resolve(
            hasRelease: viewModel.state.selectedHelmRelease != nil,
            hasOperatorResource: viewModel.state.selectedOperatorResource != nil
        )
    }

    private func syncHelmBrowserTabWithSelection() {
        guard let browserTab = selectedHelmInspectorMode.browserTab else { return }
        helmBrowserTab = browserTab
        viewModel.setHelmBrowserResourceFamily(browserTab.resourceListFamily)
    }

    private func syncSavedWorkspaceInspectorState() {
        viewModel.updateSavedWorkspaceInspectorState(currentSavedWorkspaceInspectorState)
    }

    private func applySavedWorkspaceInspectorState(_ inspectorState: SavedWorkspaceInspectorState?) {
        guard let inspectorState else { return }

        if let podTabID = inspectorState.podTabID,
           let tab = PodInspectorTab(rawValue: podTabID) {
            podInspectorTab = tab
        }
        if let serviceTabID = inspectorState.serviceTabID,
           let tab = ServiceInspectorTab(rawValue: serviceTabID) {
            serviceInspectorTab = tab
        }
        if let deploymentTabID = inspectorState.deploymentTabID,
           let tab = DeploymentInspectorTab(rawValue: deploymentTabID) {
            deploymentInspectorTab = tab
        }
        if let genericManifestTabID = inspectorState.genericManifestTabID,
           let tab = GenericResourceManifestTab(rawValue: genericManifestTabID) {
            genericResourceManifestTab = tab
        }
        if let helmTabID = inspectorState.helmTabID,
           let tab = HelmInspectorTab(rawValue: helmTabID) {
            helmInspectorTab = tab
        }
        if let terminalTabID = inspectorState.terminalTabID,
           let tab = TerminalInspectorTab(rawValue: terminalTabID) {
            terminalInspectorTab = tab
        }
        if let isYAMLInlineEditing = inspectorState.isYAMLInlineEditing {
            yamlManifestIsEditing = isYAMLInlineEditing && resolvedManifestInlineEditorImplementation.supportsInlineEditing
        }
        syncSavedWorkspaceInspectorState()
    }

    private func handlePendingLaunchActionIfNeeded() {
        guard let request = UserDefaults.standard.consumeRunePendingLaunchRequest() else { return }

        DispatchQueue.main.async {
            switch request.action {
            case .authDoctor:
                guard !simpleMode else { return }
                isAuthDoctorPanelExpanded = true
                viewModel.runAuthDoctor()
            case .commandPalette:
                viewModel.presentCommandPalette(prefilledQuery: request.query)
            case .recentContexts:
                viewModel.presentCommandPalette(prefilledQuery: Self.launchCommandPaletteQuery(prefix: ":ctx", query: request.query))
            case .savedWorkspaces:
                viewModel.presentCommandPalette(prefilledQuery: Self.launchCommandPaletteQuery(prefix: ":ws", query: request.query))
            }
        }
    }

    private static func launchCommandPaletteQuery(prefix: String, query: String?) -> String {
        let trimmed = query?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return trimmed.isEmpty ? "\(prefix) " : "\(prefix) \(trimmed)"
    }

    private func handleRootDisappear() {
        removeLocalKeyboardMonitor()
    }

    private func scheduleWorkspaceChromeMount() {
        guard !debugDisableBootstrap else {
            hasMountedWorkspaceChrome = true
            return
        }
        guard !hasMountedWorkspaceChrome else { return }
        Task { @MainActor in
            try? await Task.sleep(nanoseconds: workspaceChromeMountDelayNanoseconds)
            hasMountedWorkspaceChrome = true
        }
    }

    private var launchExperienceOverlay: some View {
        ZStack {
            RuneGlassPaneSurface(role: .window)
                .ignoresSafeArea()

            VStack(spacing: 16) {
                launchLogo

                VStack(spacing: 5) {
                    Text("Loading workspace")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                ProgressView()
                    .controlSize(.small)
                    .padding(.top, 2)
            }
            .padding(22)
        }
        .allowsHitTesting(false)
        .accessibilityElement(children: .combine)
        .accessibilityLabel("Rune loading workspace")
    }

    @ViewBuilder
    private var launchLogo: some View {
        if let image = launchLogoImage {
            Image(nsImage: image)
                .resizable()
                .scaledToFit()
                .frame(width: 112, height: 112)
                .accessibilityHidden(true)
        } else {
            Image(systemName: "hexagon")
                .font(.system(size: 58, weight: .semibold))
                .foregroundStyle(Color.accentColor)
                .frame(width: 112, height: 112)
                .accessibilityHidden(true)
        }
    }

    private var launchLogoImage: NSImage? {
        for bundle in launchLogoCandidateBundles {
            if let url = bundle.url(forResource: "rune_logo_main", withExtension: "png"),
               let image = NSImage(contentsOf: url) {
                return image
            }
        }
        if let applicationIcon = NSApp.applicationIconImage,
           applicationIcon.size.width > 0,
           applicationIcon.size.height > 0 {
            return applicationIcon
        }
        return NSImage(named: "rune_logo_main")
    }

    private var launchLogoCandidateBundles: [Bundle] {
        var bundles = [Bundle.main]
        guard let resourceURL = Bundle.main.resourceURL else { return bundles }

        let explicitBundleNames = [
            "Rune_RuneUI.bundle",
            "RuneUI_RuneUI.bundle",
            "RuneUI.bundle"
        ]
        for bundleName in explicitBundleNames {
            if let bundle = Bundle(url: resourceURL.appendingPathComponent(bundleName)) {
                bundles.append(bundle)
            }
        }

        guard let resourceBundles = try? FileManager.default.contentsOfDirectory(
            at: resourceURL,
            includingPropertiesForKeys: nil,
            options: [.skipsHiddenFiles]
        ) else {
            return bundles
        }

        for bundleURL in resourceBundles where bundleURL.pathExtension == "bundle" {
            if let bundle = Bundle(url: bundleURL), !bundles.contains(where: { $0.bundleURL == bundle.bundleURL }) {
                bundles.append(bundle)
            }
        }
        return bundles
    }

    private var configuredMainSplitContainer: some View {
        mainSplitContainer
            .toolbar {
                ToolbarItemGroup(placement: .navigation) {
                    Button {
                        viewModel.navigateBack()
                    } label: {
                        Image(systemName: "chevron.left")
                    }
                    .help("Back (Command-Option-Left Arrow)")
                    .keyboardShortcut(.leftArrow, modifiers: [.command, .option])
                    .disabled(!viewModel.canNavigateBack)

                    Button {
                        viewModel.navigateForward()
                    } label: {
                        Image(systemName: "chevron.right")
                    }
                    .help("Forward (Command-Option-Right Arrow)")
                    .keyboardShortcut(.rightArrow, modifiers: [.command, .option])
                    .disabled(!viewModel.canNavigateForward)

                    Button {
                        viewModel.toggleSidebarVisibility()
                    } label: {
                        Image(systemName: "sidebar.left")
                    }
                    .help(viewModel.isSidebarVisible ? "Hide Sidebar" : "Show Sidebar")

                    Button {
                        viewModel.toggleDetailPaneVisibility()
                    } label: {
                        Image(systemName: "sidebar.right")
                    }
                    .help(viewModel.isDetailPaneVisible ? "Hide Inspector" : "Show Inspector")

                    Menu(viewModel.state.selectedContext?.name ?? "No Context") {
                        ForEach(viewModel.contextMenuOptions) { context in
                            Button(context.name) {
                                viewModel.setContext(context)
                            }
                        }
                        if let context = viewModel.state.selectedContext {
                            Divider()
                            Button {
                                viewModel.toggleProductionMark(for: context)
                            } label: {
                                Label(
                                    viewModel.isManuallyMarkedProduction(context) ? "Unmark Production" : "Mark as Production",
                                    systemImage: viewModel.isManuallyMarkedProduction(context) ? "shield.slash" : "exclamationmark.shield"
                                )
                            }
                        }
                    }

                    Menu(namespaceMenuTitle) {
                        ForEach(namespaceSuggestions, id: \.self) { namespace in
                            Button(namespace) {
                                viewModel.setNamespace(namespace)
                            }
                        }
                        if !manualNamespaceMenuOptions.isEmpty {
                            Divider()
                            Section("Manual namespaces") {
                                ForEach(manualNamespaceMenuOptions, id: \.self) { namespace in
                                    Button {
                                        viewModel.setNamespace(namespace)
                                    } label: {
                                        Label(namespace, systemImage: "square.and.pencil")
                                    }
                                }
                            }
                        }
                        Divider()
                        Button {
                            viewModel.toggleFavoriteNamespace(viewModel.state.selectedNamespace)
                        } label: {
                            Label(
                                viewModel.isFavoriteNamespace(viewModel.state.selectedNamespace) ? "Remove Namespace Favorite" : "Favorite Namespace",
                                systemImage: viewModel.isFavoriteNamespace(viewModel.state.selectedNamespace) ? "star.slash" : "star"
                            )
                        }
                        .disabled(viewModel.state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                        Button("Enter Namespace...") {
                            manualNamespaceInput = viewModel.state.selectedNamespace
                            isManualNamespaceSheetPresented = true
                        }
                    }
                }

                ToolbarItemGroup(placement: .primaryAction) {
                    Button {
                        viewModel.refreshCurrentView(debounced: false)
                    } label: {
                        Image(systemName: "arrow.clockwise")
                    }
                    .help("Reload")
                    .keyboardShortcut("r", modifiers: .command)

                    Toggle(isOn: $viewModel.isLiveStatusUpdatesEnabled) {
                        Image(systemName: viewModel.isLiveStatusUpdatesEnabled ? "dot.radiowaves.left.and.right" : "pause.circle")
                    }
                    .toggleStyle(.button)
                    .help(viewModel.isLiveStatusUpdatesEnabled ? "Live status updates are on" : "Turn on live status updates")

                    Button {
                        viewModel.presentCommandPalette()
                    } label: {
                        Image(systemName: "command")
                    }
                    .help("Command Palette")
                    .keyboardShortcut("k", modifiers: .command)

                    Button {
                        openSettingsWindow()
                    } label: {
                        Image(systemName: "gearshape")
                    }
                    .help("Settings")
                }
            }
            .toolbarBackground(.visible, for: .windowToolbar)
            .sheet(isPresented: commandPalettePresentedBinding) {
                CommandPaletteView(viewModel: viewModel)
            }
            .sheet(isPresented: $isYAMLEditorSheetPresented) {
                yamlManifestEditorSheet()
                    .runePendingWriteConfirmation(
                        isPresented: pendingWriteActionPresentedBinding,
                        title: viewModel.pendingWriteActionTitle,
                        confirmLabel: viewModel.pendingWriteActionConfirmLabel,
                        isDestructive: viewModel.pendingWriteActionIsDestructive,
                        message: pendingWriteActionDialogMessage,
                        showsCopyCommandAction: !viewModel.pendingWriteActionKubectlCommand.isEmpty,
                        onConfirm: confirmPendingWriteActionFromDialog,
                        onCancel: cancelPendingWriteActionFromDialog,
                        onCopyCommand: viewModel.copyPendingWriteActionKubectlCommand
                    )
            }
            .sheet(item: $selectedAddClusterProvider, onDismiss: {
                viewModel.cancelNativeCloudClusterImport()
            }) { provider in
                addClusterProviderSheet(provider)
            }
            .sheet(isPresented: $isManualNamespaceSheetPresented) {
                manualNamespaceSheet
            }
            .runePendingWriteConfirmation(
                isPresented: pendingWriteActionPresentedBinding,
                title: viewModel.pendingWriteActionTitle,
                confirmLabel: viewModel.pendingWriteActionConfirmLabel,
                isDestructive: viewModel.pendingWriteActionIsDestructive,
                message: pendingWriteActionDialogMessage,
                showsCopyCommandAction: !viewModel.pendingWriteActionKubectlCommand.isEmpty,
                onConfirm: confirmPendingWriteActionFromDialog,
                onCancel: cancelPendingWriteActionFromDialog,
                onCopyCommand: viewModel.copyPendingWriteActionKubectlCommand
            )
    }

    private var pendingWriteActionDialogMessage: String {
        let command = viewModel.pendingWriteActionKubectlCommand
        guard !command.isEmpty else { return viewModel.pendingWriteActionMessage }
        return viewModel.pendingWriteActionMessage + "\n\nkubectl preview:\n" + command
    }

    private var resolvedShellVariant: RuneRootShellVariant {
        RuneRootShellVariant.resolved(override: forcedShellVariant)
    }

    private var resolvedManifestInlineEditorImplementation: ManifestInlineEditorImplementation {
        ManifestInlineEditorImplementation.resolved(override: forcedManifestInlineEditorImplementation)
    }

    private var manualNamespaceSheet: some View {
        return VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogSectionSpacing) {
            Text("Enter Namespace")
                .font(.title3.weight(.semibold))
                .accessibilityAddTraits(.isHeader)
            Text("Use this when RBAC does not allow listing namespaces, or when the namespace is not in the cached menu yet.")
                .font(.footnote)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)

            TextField("namespace", text: $manualNamespaceInput)
                .textFieldStyle(.roundedBorder)

            RuneDialogActionBar {
                Button {
                    isManualNamespaceSheetPresented = false
                } label: {
                    RuneDialogButtonLabel("Cancel")
                }
                .buttonStyle(.bordered)
                .keyboardShortcut(.cancelAction)

                Button {
                    viewModel.setNamespace(manualNamespaceInput)
                    isManualNamespaceSheetPresented = false
                } label: {
                    RuneDialogButtonLabel("Use Namespace")
                }
                .buttonStyle(.borderedProminent)
                .keyboardShortcut(.defaultAction)
                .disabled(manualNamespaceInput.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }
        }
        .padding(RuneUILayoutMetrics.dialogContentPadding)
        .frame(width: RuneUILayoutMetrics.compactDialogWidth)
    }

    private func startLiveDebugScenarioIfNeeded() {
        guard RuneRootLayoutDebug.liveScenarioEnabled else { return }
        guard !liveDebugScenarioStarted else { return }
        liveDebugScenarioStarted = true

        Task { @MainActor in
            await runLiveDebugScenario()
        }
    }

    @MainActor
    private func runLiveDebugScenario() async {
        RuneRootLayoutDebug.logScenario(
            .overview,
            status: "start",
            detail: "shell=\(resolvedShellVariant.debugLabel) editor=\(resolvedManifestInlineEditorImplementation.debugLabel)"
        )

        await waitForLiveScenarioReady()

        for step in RuneRootLiveDebugScenarioStep.allCases {
            await performLiveDebugScenarioStep(step)
        }

        RuneRootLayoutDebug.logScenario(.overview, status: "finished")

        if RuneRootLayoutDebug.liveScenarioExitWhenDone {
            NSApp.terminate(nil)
        }
    }

    @MainActor
    private func waitForLiveScenarioReady() async {
        let timeout = Date().addingTimeInterval(20)
        while Date() < timeout {
            if viewModel.state.selectedContext != nil, !viewModel.visibleContexts.isEmpty {
                var appliedOverride = false

                if let contextName = RuneRootLayoutDebug.liveScenarioContextName, !contextName.isEmpty,
                   let context = viewModel.visibleContexts.first(where: { $0.name == contextName }),
                   viewModel.state.selectedContext != context {
                    viewModel.setContext(context)
                    appliedOverride = true
                }
                if let namespace = RuneRootLayoutDebug.liveScenarioNamespace, !namespace.isEmpty,
                   viewModel.state.selectedNamespace != namespace {
                    viewModel.setNamespace(namespace)
                    appliedOverride = true
                }

                let contextSettled =
                    RuneRootLayoutDebug.liveScenarioContextName == nil
                    || RuneRootLayoutDebug.liveScenarioContextName?.isEmpty == true
                    || viewModel.state.selectedContext?.name == RuneRootLayoutDebug.liveScenarioContextName
                let namespaceSettled =
                    RuneRootLayoutDebug.liveScenarioNamespace == nil
                    || RuneRootLayoutDebug.liveScenarioNamespace?.isEmpty == true
                    || viewModel.state.selectedNamespace == RuneRootLayoutDebug.liveScenarioNamespace

                if appliedOverride, contextSettled, namespaceSettled {
                    viewModel.refreshCurrentView()
                }

                if contextSettled && namespaceSettled {
                    try? await Task.sleep(nanoseconds: 700_000_000)
                    return
                }
            }
            try? await Task.sleep(nanoseconds: 200_000_000)
        }
    }

    @MainActor
    private func performLiveDebugScenarioStep(_ step: RuneRootLiveDebugScenarioStep) async {
        RuneRootLayoutDebug.logScenario(step, status: "begin")

        let applied = await waitUntilLiveDebugScenarioStepCanApply(step)
        if !applied {
            RuneRootLayoutDebug.logScenario(step, status: "skip-timeout")
            return
        }

        let dwellNanoseconds = liveDebugScenarioDwellNanoseconds(for: step)
        RuneRootLayoutDebug.logScenario(step, status: "settling", detail: "dwellMs=\(dwellNanoseconds / 1_000_000)")
        try? await Task.sleep(nanoseconds: dwellNanoseconds)
        await waitUntilLiveDebugScenarioStepSettled(step)

        if let snapshot = lastLayoutSnapshot {
            RuneRootLayoutDebug.logScenario(
                step,
                status: "snapshot",
                detail: "content=(\(snapshot.contentMinX ?? -1),\(snapshot.contentMinY ?? -1)) detail=(\(snapshot.detailMinX ?? -1),\(snapshot.detailMinY ?? -1))"
            )
        } else {
            RuneRootLayoutDebug.logScenario(step, status: "snapshot-missing")
        }
        await waitForLiveDebugScenarioInteractionAcknowledgement(step)
    }

    @MainActor
    private func waitForLiveDebugScenarioInteractionAcknowledgement(
        _ step: RuneRootLiveDebugScenarioStep
    ) async {
        guard step.requiresExternalInteractionAcknowledgement,
              let directory = RuneRootLayoutDebug.liveScenarioInteractionAcknowledgementDirectory,
              !directory.isEmpty else {
            try? await Task.sleep(nanoseconds: RuneRootLayoutDebug.liveScenarioSnapshotHoldNanoseconds)
            return
        }

        let acknowledgementURL = URL(fileURLWithPath: directory, isDirectory: true)
            .appendingPathComponent("\(step.rawValue).ack", isDirectory: false)
        let timeout = Date().addingTimeInterval(45)
        while Date() < timeout {
            if FileManager.default.fileExists(atPath: acknowledgementURL.path) {
                RuneRootLayoutDebug.logScenario(step, status: "interaction-acknowledged")
                return
            }
            try? await Task.sleep(nanoseconds: 50_000_000)
        }
        RuneRootLayoutDebug.logScenario(step, status: "interaction-ack-timeout")
    }

    @MainActor
    private func waitUntilLiveDebugScenarioStepSettled(_ step: RuneRootLiveDebugScenarioStep) async {
        let timeout = Date().addingTimeInterval(20)
        while Date() < timeout {
            if isLiveDebugScenarioStepSettled(step) {
                return
            }
            try? await Task.sleep(nanoseconds: 250_000_000)
        }
        RuneRootLayoutDebug.logScenario(step, status: "settle-timeout")
    }

    private func isLiveDebugScenarioStepSettled(_ step: RuneRootLiveDebugScenarioStep) -> Bool {
        switch step {
        case .workloadPodLogs, .workloadDeploymentUnifiedLogs, .networkingServiceUnifiedLogs, .terminalLogs:
            return !viewModel.state.isLoading && !viewModel.state.isLoadingLogs
        case .workloadPodYAMLReadOnly,
             .workloadPodYAMLQuickEdit,
             .workloadPodYAMLEditorSheet,
             .workloadDeploymentYAMLReadOnly,
             .workloadDeploymentYAMLQuickEdit,
             .networkingServiceYAMLReadOnly,
             .networkingServiceYAMLQuickEdit,
             .configConfigMapYAMLReadOnly,
             .configConfigMapYAMLQuickEdit,
             .storagePVCYAML:
            return !viewModel.state.isLoadingResourceDetails
                && (!viewModel.state.resourceYAML.isEmpty || viewModel.state.lastResourceYAMLError != nil)
        case .workloadPodDescribe,
             .workloadDeploymentDescribe,
             .networkingServiceDescribe,
             .configConfigMapDescribe,
             .storagePVCDescribe,
             .rbacRole:
            return !viewModel.state.isLoadingResourceDetails
                && (!viewModel.state.resourceDescribe.isEmpty || viewModel.state.lastResourceDescribeError != nil)
        default:
            return !viewModel.state.isLoading
        }
    }

    @MainActor
    private func waitUntilLiveDebugScenarioStepCanApply(_ step: RuneRootLiveDebugScenarioStep) async -> Bool {
        let timeout = Date().addingTimeInterval(25)
        repeat {
            if applyLiveDebugScenarioStep(step) {
                return true
            }
            viewModel.refreshCurrentView()
            try? await Task.sleep(nanoseconds: 500_000_000)
        } while Date() < timeout
        return false
    }

    private func liveDebugScenarioDwellNanoseconds(for step: RuneRootLiveDebugScenarioStep) -> UInt64 {
        switch step {
        case .workloadPodOverview,
             .workloadPodLogs,
             .workloadPodExec,
             .workloadPodPortForward,
             .workloadPodYAMLReadOnly,
             .workloadPodYAMLQuickEdit,
             .workloadPodYAMLEditorSheet,
             .workloadPodDescribe:
            return RuneRootLayoutDebug.liveScenarioPodDwellNanoseconds
        case .workloadDeploymentUnifiedLogs, .networkingServiceUnifiedLogs:
            return max(RuneRootLayoutDebug.liveScenarioPodDwellNanoseconds, 2_500_000_000)
        default:
            return 900_000_000
        }
    }

    @MainActor
    private func applyLiveDebugScenarioStep(_ step: RuneRootLiveDebugScenarioStep) -> Bool {
        if !step.presentsYAMLEditorSheet {
            isYAMLEditorSheetPresented = false
        }

        switch step {
        case .overview:
            viewModel.setSection(.overview)
            return true
        case .workloadPodOverview:
            guard let pod = viewModel.visiblePods.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.selectPod(pod)
            podInspectorTab = .overview
            yamlManifestIsEditing = false
            return true
        case .workloadPodLogs:
            guard let pod = viewModel.state.selectedPod ?? viewModel.visiblePods.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.setWorkloadKind(.pod)
            viewModel.selectPod(pod)
            podInspectorTab = .logs
            yamlManifestIsEditing = false
            viewModel.reloadLogsForSelection()
            return true
        case .workloadPodExec:
            guard let pod = viewModel.state.selectedPod ?? viewModel.visiblePods.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.setWorkloadKind(.pod)
            viewModel.selectPod(pod)
            podInspectorTab = .exec
            yamlManifestIsEditing = false
            return true
        case .workloadPodPortForward:
            guard let pod = viewModel.state.selectedPod ?? viewModel.visiblePods.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.setWorkloadKind(.pod)
            viewModel.selectPod(pod)
            podInspectorTab = .portForward
            yamlManifestIsEditing = false
            return true
        case .workloadPodYAMLReadOnly:
            guard viewModel.state.selectedPod != nil || viewModel.visiblePods.first != nil else { return false }
            if viewModel.state.selectedPod == nil, let pod = viewModel.visiblePods.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.pod)
                viewModel.selectPod(pod)
            }
            podInspectorTab = .yaml
            yamlManifestIsEditing = false
            return true
        case .workloadPodYAMLQuickEdit:
            guard viewModel.state.selectedPod != nil || viewModel.visiblePods.first != nil else { return false }
            if viewModel.state.selectedPod == nil, let pod = viewModel.visiblePods.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.pod)
                viewModel.selectPod(pod)
            }
            podInspectorTab = .yaml
            yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
            return true
        case .workloadPodYAMLEditorSheet:
            guard viewModel.state.selectedPod != nil || viewModel.visiblePods.first != nil else { return false }
            if viewModel.state.selectedPod == nil, let pod = viewModel.visiblePods.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.pod)
                viewModel.selectPod(pod)
            }
            podInspectorTab = .yaml
            yamlManifestIsEditing = false
            isYAMLEditorSheetPresented = true
            return true
        case .workloadPodDescribe:
            guard viewModel.state.selectedPod != nil || viewModel.visiblePods.first != nil else { return false }
            if viewModel.state.selectedPod == nil, let pod = viewModel.visiblePods.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.pod)
                viewModel.selectPod(pod)
            }
            podInspectorTab = .describe
            yamlManifestIsEditing = false
            return true
        case .workloadDeploymentOverview:
            guard let deployment = viewModel.visibleDeployments.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.selectDeployment(deployment)
            deploymentInspectorTab = .overview
            yamlManifestIsEditing = false
            return true
        case .workloadDeploymentUnifiedLogs:
            guard let deployment = viewModel.state.selectedDeployment ?? viewModel.visibleDeployments.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.setWorkloadKind(.deployment)
            viewModel.selectDeployment(deployment)
            deploymentInspectorTab = .unifiedLogs
            yamlManifestIsEditing = false
            viewModel.reloadLogsForSelection()
            return true
        case .workloadDeploymentRollout:
            guard let deployment = viewModel.state.selectedDeployment ?? viewModel.visibleDeployments.first else { return false }
            viewModel.setSection(.workloads)
            viewModel.setWorkloadKind(.deployment)
            viewModel.selectDeployment(deployment)
            deploymentInspectorTab = .rollout
            yamlManifestIsEditing = false
            return true
        case .workloadDeploymentYAMLReadOnly:
            guard viewModel.state.selectedDeployment != nil || viewModel.visibleDeployments.first != nil else { return false }
            if viewModel.state.selectedDeployment == nil, let deployment = viewModel.visibleDeployments.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.deployment)
                viewModel.selectDeployment(deployment)
            }
            deploymentInspectorTab = .yaml
            yamlManifestIsEditing = false
            return true
        case .workloadDeploymentYAMLQuickEdit:
            guard viewModel.state.selectedDeployment != nil || viewModel.visibleDeployments.first != nil else { return false }
            if viewModel.state.selectedDeployment == nil, let deployment = viewModel.visibleDeployments.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.deployment)
                viewModel.selectDeployment(deployment)
            }
            deploymentInspectorTab = .yaml
            yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
            return true
        case .workloadDeploymentDescribe:
            guard viewModel.state.selectedDeployment != nil || viewModel.visibleDeployments.first != nil else { return false }
            if viewModel.state.selectedDeployment == nil, let deployment = viewModel.visibleDeployments.first {
                viewModel.setSection(.workloads)
                viewModel.setWorkloadKind(.deployment)
                viewModel.selectDeployment(deployment)
            }
            deploymentInspectorTab = .describe
            yamlManifestIsEditing = false
            return true
        case .networkingServiceOverview:
            guard let service = viewModel.visibleServices.first else { return false }
            viewModel.setSection(.networking)
            viewModel.selectService(service)
            serviceInspectorTab = .overview
            yamlManifestIsEditing = false
            return true
        case .networkingServiceUnifiedLogs:
            guard let service = viewModel.state.selectedService ?? viewModel.visibleServices.first else { return false }
            viewModel.setSection(.networking)
            viewModel.setWorkloadKind(.service)
            viewModel.selectService(service)
            serviceInspectorTab = .unifiedLogs
            yamlManifestIsEditing = false
            viewModel.reloadLogsForSelection()
            return true
        case .networkingServicePortForward:
            guard let service = viewModel.state.selectedService ?? viewModel.visibleServices.first else { return false }
            viewModel.setSection(.networking)
            viewModel.setWorkloadKind(.service)
            viewModel.selectService(service)
            serviceInspectorTab = .portForward
            yamlManifestIsEditing = false
            return true
        case .networkingServiceYAMLReadOnly:
            guard viewModel.state.selectedService != nil || viewModel.visibleServices.first != nil else { return false }
            if viewModel.state.selectedService == nil, let service = viewModel.visibleServices.first {
                viewModel.setSection(.networking)
                viewModel.setWorkloadKind(.service)
                viewModel.selectService(service)
            }
            serviceInspectorTab = .yaml
            yamlManifestIsEditing = false
            return true
        case .networkingServiceYAMLQuickEdit:
            guard viewModel.state.selectedService != nil || viewModel.visibleServices.first != nil else { return false }
            if viewModel.state.selectedService == nil, let service = viewModel.visibleServices.first {
                viewModel.setSection(.networking)
                viewModel.setWorkloadKind(.service)
                viewModel.selectService(service)
            }
            serviceInspectorTab = .yaml
            yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
            return true
        case .networkingServiceDescribe:
            guard viewModel.state.selectedService != nil || viewModel.visibleServices.first != nil else { return false }
            if viewModel.state.selectedService == nil, let service = viewModel.visibleServices.first {
                viewModel.setSection(.networking)
                viewModel.setWorkloadKind(.service)
                viewModel.selectService(service)
            }
            serviceInspectorTab = .describe
            yamlManifestIsEditing = false
            return true
        case .configConfigMapPrepare:
            viewModel.setSection(.config)
            viewModel.setWorkloadKind(.configMap)
            if let configMap = viewModel.visibleConfigMaps.first {
                viewModel.selectConfigMap(configMap)
            } else {
                viewModel.refreshCurrentView()
            }
            yamlManifestIsEditing = false
            return true
        case .configConfigMapYAMLReadOnly:
            viewModel.setSection(.config)
            viewModel.setWorkloadKind(.configMap)
            guard let configMap = viewModel.visibleConfigMaps.first else { return false }
            viewModel.selectConfigMap(configMap)
            genericResourceManifestTab = .yaml
            yamlManifestIsEditing = false
            return true
        case .configConfigMapYAMLQuickEdit:
            viewModel.setSection(.config)
            viewModel.setWorkloadKind(.configMap)
            guard let configMap = viewModel.visibleConfigMaps.first else { return false }
            viewModel.selectConfigMap(configMap)
            genericResourceManifestTab = .yaml
            yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
            return true
        case .configConfigMapDescribe:
            viewModel.setSection(.config)
            viewModel.setWorkloadKind(.configMap)
            guard viewModel.state.selectedConfigMap != nil || viewModel.visibleConfigMaps.first != nil else { return false }
            if viewModel.state.selectedConfigMap == nil, let configMap = viewModel.visibleConfigMaps.first {
                viewModel.selectConfigMap(configMap)
            }
            genericResourceManifestTab = .describe
            yamlManifestIsEditing = false
            return true
        case .storagePVCDescribe:
            viewModel.setSection(.storage)
            viewModel.setWorkloadKind(.persistentVolumeClaim)
            guard let pvc = viewModel.visiblePersistentVolumeClaims.first else { return false }
            viewModel.selectPersistentVolumeClaim(pvc)
            genericResourceManifestTab = .describe
            yamlManifestIsEditing = false
            return true
        case .storagePVCYAML:
            viewModel.setSection(.storage)
            viewModel.setWorkloadKind(.persistentVolumeClaim)
            guard let pvc = viewModel.visiblePersistentVolumeClaims.first else { return false }
            viewModel.selectPersistentVolumeClaim(pvc)
            genericResourceManifestTab = .yaml
            yamlManifestIsEditing = false
            return true
        case .eventsDetail:
            viewModel.setSection(.events)
            guard let event = viewModel.visibleEvents.first else { return false }
            viewModel.selectEvent(event)
            yamlManifestIsEditing = false
            return true
        case .rbacRole:
            viewModel.setSection(.rbac)
            viewModel.setWorkloadKind(.role)
            guard let resource = viewModel.visibleRBACResources.first else { return false }
            viewModel.selectRBACResource(resource)
            genericResourceManifestTab = .describe
            yamlManifestIsEditing = false
            return true
        case .terminal:
            viewModel.setSection(.terminal)
            terminalInspectorTab = .commands
            yamlManifestIsEditing = false
            return true
        case .terminalLogs:
            viewModel.setSection(.terminal)
            terminalInspectorTab = .logs
            yamlManifestIsEditing = false
            guard let pod = terminalLogActivePod ?? terminalInitialLogPod else { return false }
            ensureTerminalLogTabs(for: pod)
            viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
            return true
        }
    }

    /// Three-column workspace — shell can be tested under both native `NavigationSplitView` and AppKit-backed split behavior.
    @ViewBuilder
    private var mainSplitContainer: some View {
        switch resolvedShellVariant {
        case .navigationSplitView:
            if !viewModel.isSidebarVisible || !viewModel.isDetailPaneVisible {
                compactPanelSplitContainer
            } else {
                NavigationSplitView {
                    sidebar
                        .runeAppKitFrameReporter("sidebar")
                        .background(RuneRootPaneWidthReporter(kind: .sidebar))
                        .overlay(alignment: .trailing) {
                            splitColumnResizeHandle
                                .offset(x: 7)
                        }
                        .navigationSplitViewColumnWidth(
                            min: RuneUILayoutMetrics.splitSidebarMinWidth,
                            ideal: resolvedSidebarWidth,
                            max: RuneUILayoutMetrics.splitSidebarMaxWidth
                        )
                } content: {
                    contentPane
                        .runeAppKitFrameReporter("content")
                        .overlay(alignment: .trailing) {
                            splitColumnResizeHandle
                                .offset(x: 7)
                        }
                        .navigationSplitViewColumnWidth(
                            min: RuneUILayoutMetrics.splitContentColumnMinWidth,
                            ideal: 760,
                            max: RuneUILayoutMetrics.splitContentColumnMaxWidth
                        )
                } detail: {
                    detailPane
                        .runeAppKitFrameReporter("detail")
                        .background(RuneRootPaneWidthReporter(kind: .detail))
                        .navigationSplitViewColumnWidth(
                            min: RuneUILayoutMetrics.splitDetailColumnMinWidth,
                            ideal: resolvedDetailWidth,
                            max: RuneUILayoutMetrics.splitDetailColumnMaxWidth
                        )
                }
                .navigationSplitViewStyle(.balanced)
            }
        case .appKitSplitView:
            AppKitTripleSplitView(
                sidebar: AnyView(
                    sidebar
                        .runeAppKitFrameReporter("sidebar")
                        .overlay(alignment: .trailing) {
                            splitColumnResizeHandle
                                .offset(x: 7)
                        }
                ),
                content: AnyView(
                    contentPane
                        .runeAppKitFrameReporter("content")
                        .overlay(alignment: .trailing) {
                            splitColumnResizeHandle
                                .offset(x: 7)
                        }
                ),
                detail: AnyView(
                    detailPane
                        .runeAppKitFrameReporter("detail")
                ),
                sidebarVisible: viewModel.isSidebarVisible,
                detailVisible: viewModel.isDetailPaneVisible,
                sidebarWidth: resolvedSidebarWidth,
                detailWidth: resolvedDetailWidth,
                onSidebarWidthChange: { width in
                    persistSidebarWidthIfNeeded(width)
                },
                onDetailWidthChange: { width in
                    persistDetailWidthIfNeeded(width)
                }
            )
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
    }

    @ViewBuilder
    private var compactPanelSplitContainer: some View {
        if !viewModel.isSidebarVisible && !viewModel.isDetailPaneVisible {
            contentPane
                .runeAppKitFrameReporter("content")
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        } else {
            HSplitView {
                if viewModel.isSidebarVisible {
                    sidebar
                        .runeAppKitFrameReporter("sidebar")
                        .background(RuneRootPaneWidthReporter(kind: .sidebar))
                        .overlay(alignment: .trailing) {
                            splitColumnResizeHandle
                                .offset(x: 7)
                        }
                        .frame(
                            minWidth: RuneUILayoutMetrics.splitSidebarMinWidth,
                            idealWidth: resolvedSidebarWidth,
                            maxWidth: RuneUILayoutMetrics.splitSidebarMaxWidth,
                            maxHeight: .infinity,
                            alignment: .topLeading
                        )
                }

                contentPane
                    .runeAppKitFrameReporter("content")
                    .frame(
                        minWidth: RuneUILayoutMetrics.splitContentColumnMinWidth,
                        maxWidth: .infinity,
                        maxHeight: .infinity,
                        alignment: .topLeading
                    )

                if viewModel.isDetailPaneVisible {
                    detailPane
                        .runeAppKitFrameReporter("detail")
                        .background(RuneRootPaneWidthReporter(kind: .detail))
                        .frame(
                            minWidth: RuneUILayoutMetrics.splitDetailColumnMinWidth,
                            idealWidth: compactDetailIdealWidth,
                            maxWidth: compactDetailMaxWidth,
                            maxHeight: .infinity,
                            alignment: .topLeading
                        )
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
    }

    private var background: some View {
        RuneGlassPaneSurface(role: .window)
            .ignoresSafeArea()
    }

    private var sidebar: some View {
        VStack(alignment: .leading, spacing: 12) {
            TextField("Search contexts", text: Binding(get: {
                viewModel.state.contextSearchQuery
            }, set: { newValue in
                viewModel.setContextSearchQuery(newValue)
            }))
            .textFieldStyle(.roundedBorder)
            .focused($textInputFocus, equals: .contextSearch)

            Text("Sections")
                .font(.headline)
                .foregroundStyle(keyboardPaneFocus == .sidebarSections ? Color.accentColor : .secondary)

            VStack(alignment: .leading, spacing: 3) {
                ForEach(RuneSection.allCases) { section in
                    sectionRow(section)
                }
            }

            Divider()
                .overlay(Color(nsColor: .separatorColor))

            HStack(spacing: 8) {
                Text("Contexts")
                    .font(.headline)
                    .foregroundStyle(keyboardPaneFocus == .sidebarContexts ? Color.accentColor : .secondary)
                Spacer(minLength: 0)
                addClusterButton
            }

            ScrollView {
                LazyVStack(alignment: .leading, spacing: 5) {
                    if !hasAvailableKubernetesContexts {
                        RuneContentStateView(
                            .empty(
                                title: "No contexts yet",
                                message: "Connect from Overview or use Add Cluster above."
                            ),
                            variant: .inline
                        )
                    } else if viewModel.visibleContexts.isEmpty {
                        RuneContentStateView(
                            .filteredEmpty(
                                title: "No matching contexts",
                                message: "Clear the search to show all loaded contexts."
                            ),
                            variant: .inline,
                            action: RuneContentStateAction("Clear", systemImage: "xmark.circle") {
                                viewModel.setContextSearchQuery("")
                            }
                        )
                    } else {
                        ForEach(viewModel.visibleContexts) { context in
                            contextRow(context)
                        }
                    }
                }
                .frame(maxWidth: .infinity, alignment: .topLeading)
            }
            .accessibilityIdentifier("rune.sidebar.contexts.scroll")
            .frame(minHeight: 80, maxHeight: .infinity, alignment: .topLeading)
            .layoutPriority(1)

            Toggle(isOn: Binding(get: {
                viewModel.state.isReadOnlyMode
            }, set: { value in
                viewModel.setReadOnlyMode(value)
            })) {
                Text("Read-only mode")
                    .font(.headline)
            }

            Spacer(minLength: 0)
        }
        .padding(.horizontal, RuneUILayoutMetrics.sidebarPadding)
        .padding(.bottom, RuneUILayoutMetrics.sidebarPadding)
        .padding(.top, RuneUILayoutMetrics.sidebarPadding)
        .frame(minWidth: 0, maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background {
            ZStack(alignment: .trailing) {
                RuneGlassPaneSurface(role: .sidebar)
                RuneGlassPaneBorder(role: .sidebar)
                paneFocusOutline(isFocused: keyboardPaneFocus == .sidebarSections || keyboardPaneFocus == .sidebarContexts)
            }
        }
    }

    private func sectionRow(_ section: RuneSection) -> some View {
        Button {
            viewModel.setSection(section)
        } label: {
            HStack(spacing: 8) {
                Image(systemName: section.symbolName)
                    .frame(width: 16)
                Text(section.localizedTitle(appString))
                    .font(.body.weight(.medium))
                    .lineLimit(1)
                Spacer(minLength: 8)
                Text("⌘" + String(section.commandShortcut))
                    .font(.caption.monospacedDigit())
                    .foregroundStyle(.secondary)
                    .accessibilityLabel("Command " + String(section.commandShortcut))
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 7)
            .runeSidebarSelection(isSelected: viewModel.state.selectedSection == section)
        }
        .buttonStyle(.plain)
    }

    private var addClusterButton: some View {
        Button {
            addClusterPopoverPresented.toggle()
        } label: {
            Label("Add Cluster", systemImage: "plus.circle.fill")
                .font(.caption.weight(.semibold))
                .labelStyle(.titleAndIcon)
                .lineLimit(1)
                .padding(.horizontal, 9)
                .padding(.vertical, 5)
                .frame(minHeight: RuneUILayoutMetrics.iconButtonSize)
                .background(Capsule().fill(Color.accentColor.opacity(0.16)))
                .foregroundStyle(Color.accentColor)
        }
        .accessibilityIdentifier("rune.add-cluster.button")
        .buttonStyle(.plain)
        .help("Add cluster or kubeconfig")
        .popover(isPresented: $addClusterPopoverPresented, arrowEdge: .trailing) {
            addClusterPopover
        }
        .onChange(of: addClusterPopoverPresented) { _, isPresented in
            guard !isPresented else { return }
            isManualAddClusterExpanded = false
            viewModel.clearManualKubeConfigSecret()
        }
    }

    private var addClusterPopover: some View {
        AddClusterPopoverView(
            kubeConfigSourceCount: viewModel.state.kubeConfigSources.count,
            contextCount: viewModel.state.contexts.filter { $0.name != "rune-demo" }.count,
            isLoading: viewModel.state.isLoading || viewModel.isLaunchExperienceVisible,
            externalCommandsAllowed: RuneExternalCommandPolicy.allowsExternalCommands,
            favoriteImportedContexts: $viewModel.favoriteImportedKubeConfigContexts,
            isManualTokenExpanded: $isManualAddClusterExpanded,
            manualContextName: $viewModel.manualKubeConfigName,
            manualServerURL: $viewModel.manualKubeConfigServer,
            manualNamespace: $viewModel.manualKubeConfigNamespace,
            manualBearerToken: $viewModel.manualKubeConfigToken,
            onRefresh: {
                viewModel.refreshKubeConfigSourcesFromDiscovery()
            },
            onImportFile: {
                addClusterPopoverPresented = false
                viewModel.importKubeConfig()
            },
            onPasteKubeconfig: {
                addClusterPopoverPresented = false
                viewModel.importKubeConfigFromPasteboard()
            },
            onImportFolder: {
                addClusterPopoverPresented = false
                viewModel.importKubeConfigFolder()
            },
            onUseDefaultKubeconfig: {
                addClusterPopoverPresented = false
                viewModel.addDefaultKubeConfig()
            },
            onSelectProvider: { provider in
                openAddClusterProviderSheet(provider)
            },
            onImportManualToken: {
                addClusterPopoverPresented = false
                viewModel.importManualTokenKubeConfig()
            }
        )
    }
    private func contextRow(_ context: KubeContext) -> some View {
        let displayName = viewModel.contextDisplayName(for: context)
        let secondaryText = viewModel.contextSecondaryDisplayText(for: context)
        let iconName = viewModel.contextDisplayIconName(for: context)
        return ContextSidebarRow(
            displayName: displayName,
            rawName: context.name,
            secondaryText: secondaryText,
            iconName: iconName,
            isSelected: viewModel.state.selectedContext == context,
            isFavorite: viewModel.state.isFavorite(context),
            isProduction: viewModel.isProductionContext(context),
            isManuallyMarkedProduction: viewModel.isManuallyMarkedProduction(context),
            onSelect: {
                viewModel.setContext(context)
            },
            onToggleProduction: {
                viewModel.toggleProductionMark(for: context)
            },
            onToggleFavorite: {
                viewModel.toggleFavorite(for: context)
            }
        )
    }

    private func addClusterProviderSheet(_ provider: RuneAddClusterProvider) -> some View {
        let selectedNativeContext = selectedAddClusterNativeContextOption
        let isNativeProfileConnected = selectedNativeContext.map {
            connectedAddClusterNativeContextBindingIDs.contains($0.id)
        } ?? false
        let presentation = AddClusterProviderPresentation.resolve(
            provider: provider,
            externalCommandsAllowed: RuneExternalCommandPolicy.allowsExternalCommands,
            isNativeProfileConnected: isNativeProfileConnected
        )
        let hasCompatibleImportedContext = !addClusterNativeContextOptions.isEmpty
        let primaryAction = presentation.primaryAction(
            hasCompatibleImportedContext: hasCompatibleImportedContext
        )
        let utilityActions = presentation.utilityActions(
            hasCompatibleImportedContext: hasCompatibleImportedContext
        )
        let canRunCredentialImport = canRunProviderCredentialImport(provider)
        let credentialCommand = providerCredentialCommand(provider, canRunCredentialImport: canRunCredentialImport)
        let runHelp = providerCredentialRunHelp(provider, canRunCredentialImport: canRunCredentialImport)

        return VStack(alignment: .leading, spacing: 0) {
            HStack(alignment: .top, spacing: 12) {
                Button {
                    closeAddClusterProviderSheet(showPopover: true)
                } label: {
                    Image(systemName: "chevron.left")
                        .font(.system(size: 11, weight: .semibold))
                        .frame(
                            width: RuneUILayoutMetrics.dialogIconButtonSize,
                            height: RuneUILayoutMetrics.dialogIconButtonSize
                        )
                        .contentShape(Rectangle())
                }
                .buttonStyle(.borderless)
                .help("Back to Add Cluster")
                .accessibilityLabel("Back to Add Cluster")
                .disabled(viewModel.isRunningNativeCloudClusterImport)

                Image(systemName: presentation.symbolName)
                    .font(.system(size: 24, weight: .semibold))
                    .foregroundStyle(provider.accent)
                    .frame(width: 34, height: 34)
                    .background(Circle().fill(provider.accent.opacity(0.14)))
                VStack(alignment: .leading, spacing: 3) {
                    Text(presentation.title)
                        .font(.title3.weight(.semibold))
                        .accessibilityAddTraits(.isHeader)
                    Text(presentation.subtitle)
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
                Spacer(minLength: 0)
            }
            .padding(.horizontal, RuneUILayoutMetrics.dialogContentPadding)
            .padding(.top, RuneUILayoutMetrics.dialogContentPadding)
            .padding(.bottom, RuneUILayoutMetrics.dialogSectionSpacing)

            Divider()

            ScrollView {
                VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogSectionSpacing) {
                    Text(presentation.note)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)

                    if presentation.requiresCompatibleImportedContext {
                        AddClusterNativeContextSection(
                            options: addClusterNativeContextOptions,
                            selectedBindingID: $selectedAddClusterNativeContextBindingID,
                            connectedBindingIDs: connectedAddClusterNativeContextBindingIDs,
                            isCheckingProfiles: isCheckingAddClusterNativeProfiles,
                            analysisMessage: addClusterNativeContextAnalysisMessage
                        )
                    }

                    if provider != .local
                        && (!presentation.requiresCompatibleImportedContext || selectedNativeContext != nil) {
                        providerCredentialFields(presentation.fields)
                            .disabled(viewModel.isRunningCloudKubeConfigImport)
                    }

                    LazyVGrid(columns: addClusterProviderActionColumns, spacing: RuneUILayoutMetrics.dialogControlSpacing) {
                        ForEach(utilityActions) { action in
                            if action.id != .runAuthDoctor || !simpleMode {
                                addClusterProviderUtilityAction(
                                    action,
                                    provider: provider,
                                    credentialCommand: credentialCommand,
                                    selectedNativeContext: selectedNativeContext
                                )
                            }
                        }
                    }
                    .controlSize(.regular)
                    .disabled(viewModel.isRunningCloudKubeConfigImport)

                    if provider.cloudProvider != nil,
                       let status = viewModel.cloudKubeConfigImportStatus {
                        Text(status)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .accessibilityLabel("Provider login status: \(status)")
                    }

                    if let nativeStatus = viewModel.nativeKubernetesAuthStatus {
                        Text(nativeStatus)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                            .accessibilityLabel("Authentication status: \(nativeStatus)")
                    }

                    if presentation.executionMode == .externalCLI,
                       provider.cloudProvider != nil,
                       !viewModel.cloudKubeConfigImportOutput.isEmpty {
                        addClusterCloudImportOutputView(viewModel.cloudKubeConfigImportOutput)
                    }

                    if presentation.executionMode == .externalCLI,
                       let diagnostic = viewModel.cloudKubeConfigImportDiagnostic {
                        addClusterCloudImportDiagnosticView(diagnostic)
                    }

                    if presentation.showsCommandDetails {
                        RuneDisclosureSection(
                            "Command Details",
                            isExpanded: $isAddClusterProviderCommandDetailsExpanded,
                            accessibilityIdentifier: "rune.add-cluster.provider.command-details"
                        ) {
                            VStack(alignment: .leading, spacing: 8) {
                                Text(credentialCommand)
                                    .font(.system(.caption, design: .monospaced))
                                    .textSelection(.enabled)
                                    .padding(10)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .background(RuneSurfaceBackground(kind: .inset))

                                ForEach(provider.auxiliaryCommands, id: \.title) { item in
                                    HStack(spacing: 8) {
                                        Text(item.title)
                                            .font(.caption.weight(.semibold))
                                            .frame(width: 54, alignment: .leading)
                                        Text(item.command)
                                            .font(.system(.caption, design: .monospaced))
                                            .lineLimit(1)
                                            .truncationMode(.middle)
                                            .textSelection(.enabled)
                                        Spacer(minLength: 0)
                                        Button {
                                            copyToPasteboard(item.command)
                                        } label: {
                                            Image(systemName: "doc.on.doc")
                                                .frame(
                                                    width: RuneUILayoutMetrics.dialogIconButtonSize,
                                                    height: RuneUILayoutMetrics.dialogIconButtonSize
                                                )
                                                .contentShape(Rectangle())
                                        }
                                        .buttonStyle(.borderless)
                                        .help("Copy \(item.title.lowercased()) command")
                                        .accessibilityLabel("Copy \(item.title) command")
                                    }
                                    .padding(8)
                                    .background(RuneSurfaceBackground(kind: .inset))
                                }
                            }
                            .padding(.top, 8)
                        } label: {
                            Label("Command Details", systemImage: "terminal")
                                .font(.caption.weight(.semibold))
                        }
                    }
                }
                .padding(.horizontal, RuneUILayoutMetrics.dialogContentPadding)
                .padding(.vertical, RuneUILayoutMetrics.dialogSectionSpacing)
                .frame(maxWidth: .infinity, alignment: .leading)
            }
            .frame(
                minHeight: RuneUILayoutMetrics.providerDialogBodyMinHeight,
                idealHeight: RuneUILayoutMetrics.providerDialogBodyIdealHeight,
                maxHeight: RuneUILayoutMetrics.providerDialogBodyMaxHeight
            )

            RuneDialogActionBar {
                if viewModel.isRunningNativeCloudClusterImport {
                    Button(role: .cancel) {
                        viewModel.cancelNativeCloudClusterImport()
                    } label: {
                        RuneDialogButtonLabel("Cancel Import")
                    }
                    .buttonStyle(.bordered)
                    .keyboardShortcut(.cancelAction)
                } else {
                    Button {
                        closeAddClusterProviderSheet()
                    } label: {
                        RuneDialogButtonLabel("Close")
                    }
                    .buttonStyle(.bordered)
                    .keyboardShortcut(.cancelAction)
                }

                providerPrimaryAction(
                    primaryAction,
                    provider: provider,
                    canRunCredentialImport: canRunCredentialImport,
                    runHelp: runHelp
                )
            }
            .padding(.horizontal, RuneUILayoutMetrics.dialogContentPadding)
            .padding(.bottom, RuneUILayoutMetrics.dialogContentPadding)
        }
        .frame(width: RuneAddClusterProviderActionLayout.dialogWidth)
        .frame(maxHeight: RuneUILayoutMetrics.providerDialogMaxHeight)
        .interactiveDismissDisabled(viewModel.isRunningNativeCloudClusterImport)
        .task(id: "\(provider.rawValue)-\(viewModel.isConnectingNativeKubernetesAuth)") {
            guard presentation.requiresCompatibleImportedContext else { return }
            await refreshAddClusterNativeContexts(for: provider)
        }
    }

    private func addClusterCloudImportOutputView(_ output: String) -> some View {
        RuneDisclosureSection(
            "Login Output",
            isExpanded: $isAddClusterProviderLoginOutputExpanded,
            accessibilityIdentifier: "rune.add-cluster.provider.login-output"
        ) {
            ScrollView {
                Text(output)
                    .font(.system(.caption2, design: .monospaced))
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(10)
            }
            .frame(maxHeight: 140)
            .background(RuneSurfaceBackground(kind: .inset))
            .padding(.top, 8)
        } label: {
            Label("Login Output", systemImage: "terminal")
                .font(.caption.weight(.semibold))
        }
    }

    private func addClusterCloudImportDiagnosticView(_ diagnostic: AddClusterCloudImportDiagnostic) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 8) {
                Image(systemName: "exclamationmark.triangle")
                    .foregroundStyle(.orange)
                Text(diagnostic.title)
                    .font(.caption.weight(.semibold))
                Spacer(minLength: 0)
                Text(diagnostic.classification)
                    .font(.caption2.weight(.semibold))
                    .foregroundStyle(.secondary)
            }

            Text(diagnostic.message)
                .font(.caption)
                .foregroundStyle(.secondary)

            Text(diagnostic.commandShape)
                .font(.system(.caption2, design: .monospaced))
                .lineLimit(2)
                .textSelection(.enabled)

            HStack(spacing: 8) {
                Label(diagnostic.nextAction, systemImage: "arrow.triangle.2.circlepath")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                    .lineLimit(2)

                Spacer(minLength: 0)

                Link(destination: diagnostic.documentationURL) {
                    Label(diagnostic.documentationTitle, systemImage: "book")
                }
                .font(.caption2.weight(.semibold))
            }
        }
        .padding(10)
        .background(RuneSurfaceBackground(kind: .inset))
    }

    private var addClusterProviderActionColumns: [GridItem] {
        [GridItem(
            .adaptive(minimum: RuneAddClusterProviderActionLayout.minimumButtonWidth),
            spacing: RuneAddClusterProviderActionLayout.columnSpacing
        )]
    }

    private var selectedAddClusterNativeContextOption: AddClusterNativeContextOption? {
        guard let selectedAddClusterNativeContextBindingID else { return nil }
        return addClusterNativeContextOptions.first { $0.id == selectedAddClusterNativeContextBindingID }
    }

    @ViewBuilder
    private func addClusterProviderUtilityAction(
        _ action: AddClusterProviderAction,
        provider: RuneAddClusterProvider,
        credentialCommand: String,
        selectedNativeContext: AddClusterNativeContextOption?
    ) -> some View {
        switch action.id {
        case .importKubeconfig:
            Button {
                closeAddClusterProviderSheet()
                viewModel.importKubeConfig()
            } label: {
                Label(action.title, systemImage: action.systemImage)
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.bordered)
            .help("Import another kubeconfig and review its contexts before adding them.")

        case .copyExternalCommand, .copyLocalSetupCommand:
            Button {
                copyToPasteboard(credentialCommand)
            } label: {
                Label(action.title, systemImage: action.systemImage)
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.bordered)
            .help(action.id == .copyLocalSetupCommand
                ? "Copy the local cluster setup command."
                : "Copy the provider CLI command.")

        case .refreshContexts:
            Button {
                viewModel.refreshKubeConfigSourcesFromDiscovery()
                Task { @MainActor in
                    try? await Task.sleep(nanoseconds: 250_000_000)
                    await refreshAddClusterNativeContexts(for: provider)
                }
            } label: {
                Label(action.title, systemImage: action.systemImage)
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.bordered)
            .help(provider == .local
                ? "Refresh detected contexts after your local cluster tool writes kubeconfig."
                : "Refresh imported contexts and native credential status.")

        case .runAuthDoctor:
            Button {
                viewModel.runAuthDoctor()
            } label: {
                Label(action.title, systemImage: action.systemImage)
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.bordered)
            .disabled(viewModel.state.isRunningAuthDoctor)
            .help("Run Auth Doctor for provider login, kubeconfig, RBAC, and API access checks.")

        case .disconnectNativeCredentials:
            if let selectedNativeContext, let expectedProvider = provider.nativeAuthProvider {
                Button(role: .destructive) {
                    viewModel.disconnectNativeAuth(
                        request: selectedNativeContext.request,
                        expectedProvider: expectedProvider
                    )
                } label: {
                    Label(action.title, systemImage: action.systemImage)
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.bordered)
                .disabled(viewModel.isConnectingNativeKubernetesAuth)
                .help("Remove native credentials only for \(selectedNativeContext.contextName).")
            }

        case .runExternalCLI, .runNativeImport, .connectNativeCredentials, .chooseServiceAccountJSON:
            EmptyView()
        }
    }

    @MainActor
    private func refreshAddClusterNativeContexts(for provider: RuneAddClusterProvider) async {
        guard !RuneExternalCommandPolicy.allowsExternalCommands,
              let nativeProvider = provider.nativeAuthProvider else {
            addClusterNativeContextOptions = []
            selectedAddClusterNativeContextBindingID = nil
            connectedAddClusterNativeContextBindingIDs = []
            addClusterNativeContextAnalysisMessage = nil
            isCheckingAddClusterNativeProfiles = false
            return
        }

        isCheckingAddClusterNativeProfiles = true
        addClusterNativeContextAnalysisMessage = nil
        defer {
            if selectedAddClusterProvider == provider {
                isCheckingAddClusterNativeProfiles = false
            }
        }

        do {
            let sources = viewModel.state.kubeConfigSources
            let analysis = try await Task.detached(priority: .userInitiated) {
                try KubeConfigNativeAuthAnalyzer().analyze(sources: sources)
            }.value
            try Task.checkCancellation()
            guard selectedAddClusterProvider == provider else { return }

            let options = AddClusterNativeContextResolver.compatibleOptions(
                provider: nativeProvider,
                analysis: analysis
            )
            addClusterNativeContextOptions = options

            if let selectedAddClusterNativeContextBindingID,
               options.contains(where: { $0.id == selectedAddClusterNativeContextBindingID }) {
                // Preserve an explicit picker choice while refreshing profile status.
            } else {
                switch AddClusterNativeContextResolver.resolve(
                    provider: nativeProvider,
                    analysis: analysis,
                    currentContextName: viewModel.state.selectedContext?.name
                ) {
                case .selected(let option):
                    selectedAddClusterNativeContextBindingID = option.id
                case .requiresChoice, .unavailable:
                    selectedAddClusterNativeContextBindingID = nil
                }
            }

            var connected = Set<String>()
            for option in options {
                try Task.checkCancellation()
                if let status = try? await viewModel.nativeAuthProfileStatus(for: option.request),
                   status.isConnected {
                    connected.insert(option.id)
                }
            }
            guard selectedAddClusterProvider == provider else { return }
            connectedAddClusterNativeContextBindingIDs = connected
            if options.isEmpty {
                addClusterNativeContextAnalysisMessage = "Import a \(provider.title) kubeconfig to add this cluster. Rune checks its authentication settings after import."
            }
        } catch is CancellationError {
            return
        } catch {
            guard selectedAddClusterProvider == provider else { return }
            addClusterNativeContextOptions = []
            selectedAddClusterNativeContextBindingID = nil
            connectedAddClusterNativeContextBindingIDs = []
            addClusterNativeContextAnalysisMessage = "Rune could not read the imported contexts. Import a kubeconfig or try Refresh."
        }
    }

    private func openAddClusterProviderSheet(_ provider: RuneAddClusterProvider) {
        viewModel.clearManualKubeConfigSecret()
        if !viewModel.isRunningCloudKubeConfigImport {
            viewModel.clearCloudKubeConfigImportStatus()
        }
        viewModel.clearNativeKubernetesAuthStatus()
        addClusterNativeContextOptions = []
        selectedAddClusterNativeContextBindingID = nil
        connectedAddClusterNativeContextBindingIDs = []
        addClusterNativeContextAnalysisMessage = nil
        isAddClusterProviderCommandDetailsExpanded = false
        isAddClusterProviderLoginOutputExpanded = false
        addClusterPopoverPresented = false
        selectedAddClusterProvider = provider
    }

    private func closeAddClusterProviderSheet(showPopover: Bool = false) {
        selectedAddClusterProvider = nil
        addClusterNativeContextOptions = []
        selectedAddClusterNativeContextBindingID = nil
        connectedAddClusterNativeContextBindingIDs = []
        addClusterNativeContextAnalysisMessage = nil
        isAddClusterProviderCommandDetailsExpanded = false
        isAddClusterProviderLoginOutputExpanded = false
        if !showPopover {
            resetAddClusterProviderSheetStateIfIdle()
        }
        guard showPopover else { return }
        DispatchQueue.main.async {
            addClusterPopoverPresented = true
        }
    }

    private func resetAddClusterProviderSheetStateIfIdle() {
        guard !viewModel.isRunningCloudKubeConfigImport else { return }
        cloudCredentialDraft = CloudCredentialDraft()
        viewModel.clearCloudKubeConfigImportStatus()
    }

    @ViewBuilder
    private func providerCredentialFields(_ fields: [AddClusterProviderField]) -> some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.dialogControlSpacing) {
            ForEach(fields) { field in
                providerCredentialInput(field)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    @ViewBuilder
    private func providerCredentialInput(_ field: AddClusterProviderField) -> some View {
        switch field.id {
        case .clusterName:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.clusterName)
        case .resourceGroup:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.resourceGroup)
        case .subscription, .profile:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.profileOrSubscription)
        case .region, .location:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.regionOrLocation)
        case .roleARN:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.roleARN)
        case .projectID:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.projectID)
        case .awsAccessKeyID:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.nativeAWSAccessKeyID)
        case .awsSecretAccessKey:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.nativeAWSSecretAccessKey)
        case .awsSessionToken:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.nativeAWSSessionToken)
        case .azureTenantID:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.nativeAKSTenantID)
        case .azureClientID:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.nativeAKSClientID)
        case .azureClientSecret:
            AddClusterProviderCredentialTextInput(field: field, text: $cloudCredentialDraft.nativeAKSClientSecret)
        case .googleServiceAccountJSON:
            AddClusterProviderCredentialField(field: field) {
                Label("Choose the JSON document with the primary action below.", systemImage: "doc.badge.plus")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                    .accessibilityValue("Choose with the primary action below")
            }
        }
    }

    @ViewBuilder
    private func providerPrimaryAction(
        _ action: AddClusterProviderAction,
        provider: RuneAddClusterProvider,
        canRunCredentialImport: Bool,
        runHelp: String
    ) -> some View {
        switch action.id {
        case .importKubeconfig:
            Button {
                selectedAddClusterProvider = nil
                viewModel.importKubeConfig()
            } label: {
                Label(action.title, systemImage: action.systemImage)
                    .frame(
                        minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                        minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                    )
            }
            .buttonStyle(.borderedProminent)
            .keyboardShortcut(.defaultAction)
            .help("Import kubeconfig from a file.")
        case .runExternalCLI:
            if let cloudProvider = provider.cloudProvider {
                Button {
                    viewModel.runCloudKubeConfigImport(cloudCredentialDraft.request(provider: cloudProvider))
                } label: {
                    Label {
                        Text(viewModel.isRunningCloudKubeConfigImport ? "Running" : "Run")
                    } icon: {
                        Image(systemName: "icloud.and.arrow.down")
                    }
                    .frame(
                        minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                        minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                    )
                }
                .buttonStyle(.borderedProminent)
                .keyboardShortcut(.defaultAction)
                .disabled(!canRunCredentialImport || viewModel.isRunningCloudKubeConfigImport)
                .help(runHelp)
            }
        case .runNativeImport:
            Button {
                runNativeCloudImport(provider)
            } label: {
                Label {
                    Text(viewModel.isRunningCloudKubeConfigImport ? "Importing" : action.title)
                } icon: {
                    Image(systemName: action.systemImage)
                }
                .frame(
                    minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                    minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                )
            }
            .buttonStyle(.borderedProminent)
            .keyboardShortcut(.defaultAction)
            .disabled(!canRunCredentialImport || viewModel.isRunningCloudKubeConfigImport)
            .help(runHelp)
        case .connectNativeCredentials, .chooseServiceAccountJSON:
            if action.id == .chooseServiceAccountJSON,
               !RuneExternalCommandPolicy.allowsExternalCommands {
                Button {
                    chooseAndRunNativeGKEImport()
                } label: {
                    Label(
                        viewModel.isRunningCloudKubeConfigImport ? "Importing" : action.title,
                        systemImage: action.systemImage
                    )
                    .frame(
                        minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                        minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                    )
                }
                .buttonStyle(.borderedProminent)
                .keyboardShortcut(.defaultAction)
                .disabled(!canRunCredentialImport || viewModel.isRunningCloudKubeConfigImport)
                .help(runHelp)
            } else {
                nativeCloudAuthConnectButton(provider)
            }
        case .copyExternalCommand,
             .copyLocalSetupCommand,
             .refreshContexts,
             .runAuthDoctor,
             .disconnectNativeCredentials:
            EmptyView()
        }
    }

    private func runNativeCloudImport(_ provider: RuneAddClusterProvider) {
        switch provider {
        case .eks:
            let request = cloudCredentialDraft.request(provider: .eks)
            let accessKeyID = cloudCredentialDraft.nativeAWSAccessKeyID
            let secretAccessKey = cloudCredentialDraft.nativeAWSSecretAccessKey
            let sessionToken = cloudCredentialDraft.nativeAWSSessionToken
            cloudCredentialDraft.nativeAWSAccessKeyID = ""
            cloudCredentialDraft.nativeAWSSecretAccessKey = ""
            cloudCredentialDraft.nativeAWSSessionToken = ""
            viewModel.runNativeEKSClusterImport(
                clusterName: request.clusterName,
                region: request.regionOrLocation,
                accessKeyID: accessKeyID,
                secretAccessKey: secretAccessKey,
                sessionToken: sessionToken
            )
        case .aks:
            let request = cloudCredentialDraft.request(provider: .aks)
            let tenantID = cloudCredentialDraft.nativeAKSTenantID
            let clientID = cloudCredentialDraft.nativeAKSClientID
            let clientSecret = cloudCredentialDraft.nativeAKSClientSecret
            cloudCredentialDraft.nativeAKSClientSecret = ""
            viewModel.runNativeAKSClusterImport(
                subscriptionID: request.profileOrSubscription,
                resourceGroup: request.resourceGroup,
                clusterName: request.clusterName,
                tenantID: tenantID,
                clientID: clientID,
                clientSecret: clientSecret
            )
        case .gke:
            chooseAndRunNativeGKEImport()
        case .local:
            break
        }
    }

    private func chooseAndRunNativeGKEImport() {
        let request = cloudCredentialDraft.request(provider: .gke)
        viewModel.chooseAndRunNativeGKEClusterImport(
            projectID: request.projectID,
            location: request.regionOrLocation,
            clusterName: request.clusterName
        )
    }

    @ViewBuilder
    private func nativeCloudAuthConnectButton(_ provider: RuneAddClusterProvider) -> some View {
        switch provider {
        case .eks:
            Button {
                guard let nativeContext = selectedAddClusterNativeContextOption else { return }
                let accessKeyID = cloudCredentialDraft.nativeAWSAccessKeyID
                let secretAccessKey = cloudCredentialDraft.nativeAWSSecretAccessKey
                let sessionToken = cloudCredentialDraft.nativeAWSSessionToken
                cloudCredentialDraft.nativeAWSAccessKeyID = ""
                cloudCredentialDraft.nativeAWSSecretAccessKey = ""
                cloudCredentialDraft.nativeAWSSessionToken = ""
                viewModel.connectEKSNativeAuth(
                    request: nativeContext.request,
                    accessKeyID: accessKeyID,
                    secretAccessKey: secretAccessKey,
                    sessionToken: sessionToken
                )
            } label: {
                Label(viewModel.isConnectingNativeKubernetesAuth ? "Connecting" : "Connect", systemImage: "key.fill")
                    .frame(
                        minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                        minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                    )
            }
            .buttonStyle(.borderedProminent)
            .keyboardShortcut(.defaultAction)
            .disabled(
                selectedAddClusterNativeContextOption == nil
                    || !cloudCredentialDraft.hasNativeAWSCredentials
                    || viewModel.isConnectingNativeKubernetesAuth
            )
            .help("Bind AWS credentials to the compatible imported EKS context selected above.")
        case .aks:
            Button {
                guard let nativeContext = selectedAddClusterNativeContextOption else { return }
                let secret = cloudCredentialDraft.nativeAKSClientSecret
                cloudCredentialDraft.nativeAKSClientSecret = ""
                viewModel.connectAKSNativeAuth(request: nativeContext.request, clientSecret: secret)
            } label: {
                Label(viewModel.isConnectingNativeKubernetesAuth ? "Connecting" : "Connect", systemImage: "key.fill")
                    .frame(
                        minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                        minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                    )
            }
            .buttonStyle(.borderedProminent)
            .keyboardShortcut(.defaultAction)
            .disabled(
                selectedAddClusterNativeContextOption == nil
                    || !cloudCredentialDraft.hasNativeAKSClientSecret
                    || viewModel.isConnectingNativeKubernetesAuth
            )
            .help("Bind an Azure service-principal secret to the compatible imported AKS context selected above.")
        case .gke:
            Button {
                guard let nativeContext = selectedAddClusterNativeContextOption else { return }
                viewModel.chooseAndConnectGKENativeAuth(request: nativeContext.request)
            } label: {
                Label(viewModel.isConnectingNativeKubernetesAuth ? "Connecting" : "Choose JSON…", systemImage: "key.fill")
                    .frame(
                        minWidth: RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth,
                        minHeight: RuneUILayoutMetrics.dialogButtonLabelMinHeight
                    )
            }
            .buttonStyle(.borderedProminent)
            .keyboardShortcut(.defaultAction)
            .disabled(selectedAddClusterNativeContextOption == nil || viewModel.isConnectingNativeKubernetesAuth)
            .help("Choose and bind a Google service-account JSON file to the compatible imported GKE context selected above.")
        case .local:
            EmptyView()
        }
    }

    private func providerCredentialCommand(_ provider: RuneAddClusterProvider, canRunCredentialImport: Bool) -> String {
        guard canRunCredentialImport, let cloudProvider = provider.cloudProvider else {
            return provider.command
        }
        return viewModel.cloudKubeConfigCommandPreview(for: cloudCredentialDraft.request(provider: cloudProvider))
    }

    private func canRunProviderCredentialImport(_ provider: RuneAddClusterProvider) -> Bool {
        guard let cloudProvider = provider.cloudProvider else { return false }
        if RuneExternalCommandPolicy.allowsExternalCommands {
            return cloudCredentialDraft.hasRequiredFields(for: cloudProvider)
        }
        return cloudCredentialDraft.hasRequiredNativeFields(for: cloudProvider)
    }

    private func providerCredentialRunHelp(
        _ provider: RuneAddClusterProvider,
        canRunCredentialImport: Bool
    ) -> String {
        guard let cloudProvider = provider.cloudProvider else {
            return "Run the provider CLI locally, validate kubeconfig, and refresh contexts."
        }
        if viewModel.isRunningCloudKubeConfigImport {
            return "Provider import is already running."
        }
        if !RuneExternalCommandPolicy.allowsExternalCommands {
            if canRunCredentialImport {
                return "Fetch cluster access through the provider API, review kubeconfig, then store credentials in Keychain."
            }
            if let missingFields = cloudCredentialDraft.missingRequiredNativeFieldSummary(for: cloudProvider) {
                return "Enter \(missingFields) to import this cluster."
            }
        }
        if canRunCredentialImport {
            return "Run the provider CLI locally, validate kubeconfig, and refresh contexts."
        }
        if let missingFields = cloudCredentialDraft.missingRequiredFieldSummary(for: cloudProvider) {
            return "Enter \(missingFields) to run provider import."
        }
        return "Run the provider CLI locally, validate kubeconfig, and refresh contexts."
    }

    private func copyToPasteboard(_ value: String) {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(value, forType: .string)
    }

    private var contentPane: some View {
        VStack(alignment: .leading, spacing: 12) {
            if let notice = viewModel.state.activeNotice {
                RuneNoticeBanner(notice: notice) {
                    viewModel.state.clearError()
                }
                .transition(.move(edge: .top).combined(with: .opacity))
            }

            if viewModel.isProductionContext {
                HStack {
                    productionBanner
                    Spacer(minLength: 0)
                }
                .transition(.move(edge: .top).combined(with: .opacity))
            }

            contentHeader

            switch viewModel.state.selectedSection {
            case .overview:
                overviewPane
            case .workloads:
                workloadsPane
            case .networking:
                networkingPane
            case .config:
                configPane
            case .storage:
                storagePane
            case .events:
                eventsPane
            case .helm:
                helmPane
            case .terminal:
                terminalPane
            case .rbac:
                rbacPane
            }
        }
        .padding(RuneUILayoutMetrics.paneOuterPadding)
        .frame(minWidth: 0, maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .background {
            ZStack(alignment: .trailing) {
                RuneGlassPaneSurface(role: .content)
                RuneGlassPaneBorder(role: .content)
                RuneRootLayoutProbe(kind: .content, generation: layoutGeneration)
                paneFocusOutline(isFocused: keyboardPaneFocus == .content)
            }
        }
    }

    private var contentHeader: some View {
        VStack(alignment: .leading, spacing: 8) {
            RuneAdaptiveToolbar("Content header") {
                HStack(spacing: 8) {
                    Text(viewModel.state.selectedSection.localizedTitle(appString))
                        .font(.title2.weight(.bold))
                        .lineLimit(1)

                    if hasAvailableKubernetesContexts, viewModel.state.isLoading {
                        ProgressView()
                            .controlSize(.small)
                            .accessibilityLabel("Loading section")
                    }
                }
            } secondary: {
                contentHeaderPrimaryAction
            }

            if hasAvailableKubernetesContexts {
                contentHeaderStatusStrip
            }

            if hasAvailableKubernetesContexts, showsResourceFilterControls {
                resourceFilterControls
            }

            sectionSpecificControls
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(RuneRootLayoutProbe(kind: .header, generation: layoutGeneration))
    }

    @ViewBuilder
    private var contentHeaderPrimaryAction: some View {
        if viewModel.state.selectedSection == .events, hasAvailableKubernetesContexts {
            Button("Save Events") {
                viewModel.saveVisibleEvents()
            }
            .buttonStyle(.bordered)
        }
    }

    private var contentHeaderStatusStrip: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 8) {
                if viewModel.state.selectedSection != .terminal {
                    contextUsageBadge(label: "CPU", value: contextUsageValue(viewModel.state.overviewClusterCPUPercent))
                    contextUsageBadge(label: "MEM", value: contextUsageValue(viewModel.state.overviewClusterMemoryPercent))
                    if let freshness = currentResourceListFreshness {
                        ResourceListFreshnessBadge(freshness: freshness)
                    }
                }

                if viewModel.state.isReadOnlyMode {
                    RuneHeaderCapsule(
                        "Read-only",
                        role: .status,
                        systemImage: "lock.fill",
                        tint: .orange,
                        foregroundColor: .orange,
                        fill: Color.orange.opacity(0.16),
                        accessibilityLabel: "Mode: Read-only"
                    )
                }
            }
            .fixedSize(horizontal: true, vertical: false)
        }
        .accessibilityLabel("Section status")
    }

    private var showsResourceFilterControls: Bool {
        switch viewModel.state.selectedSection {
        case .overview, .terminal:
            return false
        default:
            return true
        }
    }

    private var resourceFilterControls: some View {
        HStack(spacing: 4) {
            TextField("/ filter resources", text: Binding(get: {
                viewModel.state.resourceSearchQuery
            }, set: { newValue in
                viewModel.setResourceSearchQuery(newValue)
            }))
            .textFieldStyle(.roundedBorder)
            .font(.caption)
            .controlSize(.small)
            .frame(maxWidth: 280)
            .focused($textInputFocus, equals: .resourceFilter)

            RuneIconButton(
                "Clear resource filter",
                systemImage: "xmark.circle.fill",
                isDisabled: viewModel.state.resourceSearchQuery.isEmpty
            ) {
                viewModel.setResourceSearchQuery("")
                textInputFocus = .resourceFilter
            }
            .opacity(viewModel.state.resourceSearchQuery.isEmpty ? 0 : 1)
        }
        .frame(maxWidth: 312, alignment: .leading)
    }

    @ViewBuilder
    private var sectionSpecificControls: some View {
        switch viewModel.state.selectedSection {
        case .workloads:
            resourceKindPicker(kinds: viewModel.workloadKinds)
        case .networking:
            resourceKindPicker(kinds: viewModel.networkingKinds)
        case .config:
            resourceKindPicker(kinds: viewModel.configKinds)
        case .storage:
            resourceKindPicker(kinds: viewModel.storageKinds)
        case .rbac:
            resourceKindPicker(kinds: viewModel.rbacKinds)
        case .helm:
            Toggle("All namespaces", isOn: Binding(get: {
                viewModel.state.isHelmAllNamespaces
            }, set: { value in
                viewModel.setHelmAllNamespaces(value)
            }))
            .toggleStyle(.switch)
            .controlSize(.small)
        default:
            EmptyView()
        }
    }

    private func resourceKindPicker(kinds: [KubeResourceKind]) -> some View {
        RuneSegmentedPickerInScroll(
            appString(.kind),
            selection: Binding(get: {
                viewModel.state.selectedWorkloadKind
            }, set: { kind in
                viewModel.setWorkloadKind(kind)
            })
        ) {
            ForEach(kinds) { kind in
                Text(kind.localizedTitle(appString)).tag(kind)
            }
        }
        .accessibilityLabel("Resource kind")
    }

    private var podBulkSelectionControls: some View {
        RuneBulkSelectionBar(
            selectedCount: viewModel.selectedPodCount,
            visibleCount: viewModel.visiblePods.count,
            allVisibleSelected: viewModel.areAllVisiblePodsSelectedForBulkActions,
            onToggleVisibleSelection: viewModel.toggleAllVisiblePodsForBulkActions
        ) {
            Menu {
                Button {
                    viewModel.saveSelectedPodLogsZip()
                } label: {
                    Label("Full Logs ZIP", systemImage: "archivebox")
                }
                .disabled(viewModel.state.isLoadingLogs)

                Button {
                    viewModel.saveSelectedPodLogsZipToExportFolder(openAfterSave: false)
                } label: {
                    Label("Full Logs ZIP to Export Folder", systemImage: "folder.badge.plus")
                }
                .disabled(viewModel.state.isLoadingLogs)

                Button {
                    viewModel.saveSelectedPodLogsZipToExportFolder(openAfterSave: true)
                } label: {
                    Label("Full Logs ZIP and Open", systemImage: "archivebox")
                }
                .disabled(viewModel.state.isLoadingLogs)

                Button {
                    viewModel.saveSelectedPodYAMLZip()
                } label: {
                    Label("YAML ZIP", systemImage: "doc.zipper")
                }
                .disabled(viewModel.state.isLoadingResourceDetails)
            } label: {
                Label("Export", systemImage: "square.and.arrow.up")
                    .runeMinimumInteractiveTarget()
            }
            .disabled(viewModel.selectedPodCount == 0)
            .help("Export selected pods")
        }
    }

    private var genericResourceBulkSelectionControls: some View {
        RuneBulkSelectionBar(
            selectedCount: viewModel.selectedGenericResourceCount,
            visibleCount: viewModel.visibleGenericResourcesForBulkActions.count,
            allVisibleSelected: viewModel.areAllVisibleGenericResourcesSelectedForBulkActions,
            onToggleVisibleSelection: viewModel.toggleAllVisibleGenericResourcesForBulkActions
        ) {
            Button {
                didCopyGenericResourceComparison = false
                isGenericResourceComparisonPresented = true
            } label: {
                Label("Compare", systemImage: "rectangle.split.2x1")
            }
            .disabled(!viewModel.canCopySelectedGenericResourceComparison)
            .help("Compare the selected resources")
            .popover(isPresented: $isGenericResourceComparisonPresented, arrowEdge: .bottom) {
                genericResourceComparisonPopover
            }

            Button(role: .destructive) {
                viewModel.requestDeleteSelectedGenericResources()
            } label: {
                Label("Delete Selected", systemImage: "trash")
            }
            .disabled(!viewModel.canApplyClusterMutations || viewModel.selectedGenericResourceCount == 0)
            .help("Delete selected resources in the active context and namespace scope")
        }
    }

    private var genericResourceComparisonPopover: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(spacing: 8) {
                Image(systemName: "rectangle.split.2x1")
                    .foregroundStyle(Color.accentColor)
                Text("Compare selected resources")
                    .font(.headline)
            }

            Text("Review key fields for the selected resources in one place. Copy the summary when you want to share it.")
                .font(.footnote)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)

            ScrollView([.vertical, .horizontal]) {
                Text(viewModel.selectedGenericResourceComparisonText)
                    .font(.system(size: 12, design: .monospaced))
                    .textSelection(.enabled)
                    .fixedSize(horizontal: true, vertical: true)
                    .frame(maxWidth: .infinity, alignment: .topLeading)
                    .padding(10)
            }
            .background(RuneSurfaceBackground(kind: .editor))

            HStack(spacing: 8) {
                if didCopyGenericResourceComparison {
                    Label("Copied", systemImage: "checkmark")
                        .font(.footnote.weight(.medium))
                        .foregroundStyle(.secondary)
                }

                Spacer()

                Button("Done") {
                    isGenericResourceComparisonPresented = false
                }
                .keyboardShortcut(.cancelAction)

                Button("Copy Summary") {
                    viewModel.copySelectedGenericResourceComparisonToClipboard()
                    didCopyGenericResourceComparison = true
                }
                .buttonStyle(.borderedProminent)
            }
        }
        .padding(16)
        .frame(width: 430, height: 340)
    }

    private var overviewCardModules: [OverviewModule] {
        var modules: [OverviewModule] = [.pods, .deployments, .services, .ingresses, .configMaps, .cronJobs, .nodes]
        if !simpleMode {
            modules.append(.events)
        }
        return modules
    }

    private func isOverviewCardKeyboardFocused(_ index: Int) -> Bool {
        keyboardPaneFocus == .content
            && viewModel.state.selectedSection == .overview
            && overviewCardSelectionIndex == index
    }

    private var overviewPane: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 14) {
                if !hasAvailableKubernetesContexts {
                    KubernetesConnectionOnboardingView(
                        favoriteImportedContexts: $viewModel.favoriteImportedKubeConfigContexts,
                        onImportFile: viewModel.importKubeConfig,
                        onPaste: viewModel.importKubeConfigFromPasteboard,
                        onImportFolder: viewModel.importKubeConfigFolder,
                        onUseDefault: viewModel.addDefaultKubeConfig,
                        onShowMoreOptions: {
                            addClusterPopoverPresented = true
                        }
                    )
                } else {
                    overviewStatusBanner
                    manualNamespaceBanner
                    if !simpleMode {
                        authDoctorPanel
                    }

                    LazyVGrid(columns: [GridItem(.adaptive(minimum: 160), spacing: 10)], spacing: 10) {
                    OverviewStatCard(
                        title: "Pods",
                        count: viewModel.state.overviewPods.count,
                        symbol: "cube.box.fill",
                        tint: .cyan,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(0),
                        showsHoverHelp: showHoverTooltips
                    ) {
                        overviewCardSelectionIndex = 0
                        viewModel.openOverviewModule(.pods)
                    }
                    OverviewStatCard(
                        title: "Deployments",
                        count: viewModel.state.overviewDeploymentsCount,
                        symbol: "shippingbox.fill",
                        tint: .blue,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(1),
                        showsHoverHelp: showHoverTooltips
                    ) {
                        overviewCardSelectionIndex = 1
                        viewModel.openOverviewModule(.deployments)
                    }
                    OverviewStatCard(
                        title: "Services",
                        count: viewModel.state.overviewServicesCount,
                        symbol: "point.3.connected.trianglepath.dotted",
                        tint: .purple,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(2),
                        showsHoverHelp: showHoverTooltips
                    ) {
                        overviewCardSelectionIndex = 2
                        viewModel.openOverviewModule(.services)
                    }
                    OverviewStatCard(
                        title: "Ingresses",
                        count: viewModel.state.overviewIngressesCount,
                        symbol: "network",
                        tint: .indigo,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(3),
                        showsHoverHelp: showHoverTooltips
                    ) {
                        overviewCardSelectionIndex = 3
                        viewModel.openOverviewModule(.ingresses)
                    }
                    OverviewStatCard(
                        title: "ConfigMaps",
                        count: viewModel.state.overviewConfigMapsCount,
                        symbol: "doc.text.fill",
                        tint: .teal,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(4),
                        showsHoverHelp: showHoverTooltips
                    ) {
                        overviewCardSelectionIndex = 4
                        viewModel.openOverviewModule(.configMaps)
                    }
                    OverviewStatCard(
                        title: "CronJobs",
                        count: viewModel.state.overviewCronJobsCount,
                        symbol: "calendar.badge.clock",
                        tint: .mint,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(5),
                        showsHoverHelp: showHoverTooltips
                    ) {
                        overviewCardSelectionIndex = 5
                        viewModel.openOverviewModule(.cronJobs)
                    }
                    OverviewStatCard(
                        title: "Nodes",
                        count: viewModel.state.overviewNodesCount,
                        symbol: "server.rack",
                        tint: .gray,
                        isLoading: viewModel.state.isLoading,
                        isKeyboardFocused: isOverviewCardKeyboardFocused(6),
                        showsHoverHelp: showHoverTooltips
                    ) {
                        overviewCardSelectionIndex = 6
                        viewModel.openOverviewModule(.nodes)
                    }
                    if !simpleMode {
                        OverviewStatCard(
                            title: "Events",
                            count: viewModel.state.overviewEvents.count,
                            symbol: "bolt.badge.clock.fill",
                            tint: .orange,
                            isLoading: viewModel.state.isLoading,
                            isKeyboardFocused: isOverviewCardKeyboardFocused(7),
                            help: overviewEventsCardHelp,
                            showsHoverHelp: showHoverTooltips
                        ) {
                            overviewCardSelectionIndex = 7
                            viewModel.openOverviewModule(.events)
                        }
                    }
                }

                    if !simpleMode {
                        OverviewClusterSignalsPanelView(
                            unhealthy: viewModel.overviewUnhealthyItems,
                            gitOpsRollups: viewModel.overviewGitOpsRollupItems,
                            incidents: viewModel.overviewIncidentTimelineItems,
                            dependencies: viewModel.overviewDependencyItems,
                            expandedPanels: $expandedOverviewInsightPanels,
                            onOpenSignal: viewModel.openOverviewSignal,
                            onOpenGitOpsRollup: viewModel.openOverviewGitOpsRollup,
                            onOpenDependency: viewModel.openOverviewDependency
                        )
                    }

                    VStack(alignment: .leading, spacing: 8) {
                        Text("Pod Health")
                            .font(.headline)

                        HStack(spacing: 8) {
                            healthBadge(label: "Running", value: podStatusCount("running"), color: .green)
                            healthBadge(label: "Pending", value: podStatusCount("pending"), color: .orange)
                            healthBadge(label: "Failed", value: podStatusCount("failed"), color: .red)
                            healthBadge(label: "Other", value: max(0, viewModel.state.overviewPods.count - podStatusCount("running") - podStatusCount("pending") - podStatusCount("failed")), color: .gray)
                        }
                    }
                    .runePanelCard()

                    if !simpleMode {
                        OverviewRecentEventsPanelView(
                            events: viewModel.state.overviewEvents,
                            onOpenEventSource: viewModel.openEventSource
                        )
                    }
                }
            }
        }
        .id("overview")
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var workloadsPane: some View {
        workloadsContent
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    @ViewBuilder
    private var workloadsContent: some View {
        switch viewModel.state.selectedWorkloadKind {
        case .pod:
            VStack(alignment: .leading, spacing: 8) {
                if viewModel.selectedPodCount > 0 {
                    podBulkSelectionControls
                }

                resourceListGate(kindTitle: "Pods", visibleCount: viewModel.visiblePods.count) {
                    VStack(alignment: .leading, spacing: 8) {
                        if viewModel.selectedPodCount == 0 {
                            podBulkSelectionControls
                        }

                        AppKitPodTableView(
                            pods: viewModel.visiblePods,
                            selectedPodID: viewModel.state.selectedPod?.id,
                            selectedPodIDs: viewModel.state.selectedPodIDs,
                            sortColumn: viewModel.podSortColumn,
                            sortAscending: viewModel.podSortAscending,
                            nameColumnWidth: podNameColumnWidth,
                            canApplyClusterMutations: viewModel.canApplyClusterMutations,
                            isFavorite: { pod in
                                viewModel.isFavoriteResource(kind: .pod, namespace: pod.namespace, name: pod.name)
                            },
                            onSelectPod: viewModel.selectPod,
                            onToggleBulkSelection: viewModel.togglePodBulkSelection,
                            onToggleSort: viewModel.togglePodSort,
                            onNameColumnWidthChanged: commitPodNameColumnWidth,
                            onToggleFavorite: { pod in
                                viewModel.toggleFavoriteResource(kind: .pod, namespace: pod.namespace, name: pod.name)
                            },
                            onOpenLogs: { pod in
                                viewModel.selectPod(pod)
                                podInspectorTab = .logs
                                viewModel.reloadLogsForSelection()
                            },
                            onOpenExec: { pod in
                                viewModel.selectPod(pod)
                                podInspectorTab = .exec
                            },
                            onOpenDescribe: { pod in
                                viewModel.selectPod(pod)
                                podInspectorTab = .describe
                            },
                            onOpenYAML: { pod in
                                viewModel.selectPod(pod)
                                podInspectorTab = .yaml
                            },
                            onDelete: { pod in
                                viewModel.requestDeleteResource(kind: .pod, name: pod.name)
                            }
                        )
                        .transaction { transaction in
                            transaction.animation = nil
                        }
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .id("workloads:pods:\(podListIdentity(viewModel.visiblePods))")

        case .deployment:
            resourceListGate(kindTitle: "Deployments", visibleCount: viewModel.visibleDeployments.count) {
                AppKitDeploymentListView(
                    deployments: viewModel.visibleDeployments,
                    selectedDeploymentID: viewModel.state.selectedDeployment?.id,
                    sortColumn: viewModel.deploymentSortColumn,
                    sortAscending: viewModel.deploymentSortAscending,
                    canApplyClusterMutations: viewModel.canApplyClusterMutations,
                    isFavorite: { deployment in
                        viewModel.isFavoriteResource(kind: .deployment, namespace: deployment.namespace, name: deployment.name)
                    },
                    onSelectDeployment: viewModel.selectDeployment,
                    onToggleSort: viewModel.toggleDeploymentSort,
                    onToggleFavorite: { deployment in
                        viewModel.toggleFavoriteResource(kind: .deployment, namespace: deployment.namespace, name: deployment.name)
                    },
                    onOpenUnifiedLogs: { deployment in
                        viewModel.selectDeployment(deployment)
                        deploymentInspectorTab = .unifiedLogs
                        viewModel.reloadLogsForSelection()
                    },
                    onOpenRollout: { deployment in
                        viewModel.selectDeployment(deployment)
                        deploymentInspectorTab = .rollout
                    },
                    onOpenDescribe: { deployment in
                        viewModel.selectDeployment(deployment)
                        deploymentInspectorTab = .describe
                    },
                    onOpenYAML: { deployment in
                        viewModel.selectDeployment(deployment)
                        deploymentInspectorTab = .yaml
                    },
                    onDelete: { deployment in
                        viewModel.requestDeleteResource(kind: .deployment, name: deployment.name)
                    }
                )
            }

        case .statefulSet:
            genericResourceList(viewModel.visibleStatefulSets, selection: viewModel.state.selectedStatefulSet, action: viewModel.selectStatefulSet)

        case .daemonSet:
            genericResourceList(viewModel.visibleDaemonSets, selection: viewModel.state.selectedDaemonSet, action: viewModel.selectDaemonSet)

        case .job:
            genericResourceList(viewModel.visibleJobs, selection: viewModel.state.selectedJob, action: viewModel.selectJob)

        case .cronJob:
            genericResourceList(viewModel.visibleCronJobs, selection: viewModel.state.selectedCronJob, action: viewModel.selectCronJob)

        case .replicaSet:
            genericResourceList(viewModel.visibleReplicaSets, selection: viewModel.state.selectedReplicaSet, action: viewModel.selectReplicaSet)

        case .horizontalPodAutoscaler:
            genericResourceList(
                viewModel.visibleHorizontalPodAutoscalers,
                selection: viewModel.state.selectedHorizontalPodAutoscaler,
                action: viewModel.selectHorizontalPodAutoscaler
            )

        case .service, .endpoint, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            EmptyView()

        case .event:
            EmptyView()
        }
    }

    private var networkingPane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                resourceListGate(kindTitle: "Services", visibleCount: viewModel.visibleServices.count) {
                    AppKitServiceListView(
                        services: viewModel.visibleServices,
                        selectedServiceID: viewModel.state.selectedService?.id,
                        sortColumn: viewModel.serviceSortColumn,
                        sortAscending: viewModel.serviceSortAscending,
                        canApplyClusterMutations: viewModel.canApplyClusterMutations,
                        isFavorite: { service in
                            viewModel.isFavoriteResource(kind: .service, namespace: service.namespace, name: service.name)
                        },
                        onSelectService: viewModel.selectService,
                        onToggleSort: viewModel.toggleServiceSort,
                        onToggleFavorite: { service in
                            viewModel.toggleFavoriteResource(kind: .service, namespace: service.namespace, name: service.name)
                        },
                        onOpenUnifiedLogs: { service in
                            viewModel.selectService(service)
                            serviceInspectorTab = .unifiedLogs
                            viewModel.reloadLogsForSelection()
                        },
                        onOpenPortForward: { service in
                            viewModel.selectService(service)
                            serviceInspectorTab = .portForward
                        },
                        onOpenDescribe: { service in
                            viewModel.selectService(service)
                            serviceInspectorTab = .describe
                        },
                        onOpenYAML: { service in
                            viewModel.selectService(service)
                            serviceInspectorTab = .yaml
                        },
                        onDelete: { service in
                            viewModel.requestDeleteResource(kind: .service, name: service.name)
                        }
                    )
                    .id("networking:service")
                }

            case .ingress:
                genericResourceList(viewModel.visibleIngresses, selection: viewModel.state.selectedIngress, action: viewModel.selectIngress)

            case .endpoint:
                genericResourceList(viewModel.visibleEndpoints, selection: viewModel.state.selectedEndpoint, action: viewModel.selectEndpoint)

            case .networkPolicy:
                genericResourceList(
                    viewModel.visibleNetworkPolicies,
                    selection: viewModel.state.selectedNetworkPolicy,
                    action: viewModel.selectNetworkPolicy
                )

            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var configPane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .configMap:
                genericResourceList(viewModel.visibleConfigMaps, selection: viewModel.state.selectedConfigMap, action: viewModel.selectConfigMap)

            case .secret:
                genericResourceList(viewModel.visibleSecrets, selection: viewModel.state.selectedSecret, action: viewModel.selectSecret)

            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var storagePane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolumeClaim:
                genericResourceList(
                    viewModel.visiblePersistentVolumeClaims,
                    selection: viewModel.state.selectedPersistentVolumeClaim,
                    action: viewModel.selectPersistentVolumeClaim
                )

            case .persistentVolume:
                genericResourceList(
                    viewModel.visiblePersistentVolumes,
                    selection: viewModel.state.selectedPersistentVolume,
                    action: viewModel.selectPersistentVolume
                )

            case .storageClass:
                genericResourceList(
                    viewModel.visibleStorageClasses,
                    selection: viewModel.state.selectedStorageClass,
                    action: viewModel.selectStorageClass
                )

            case .node:
                genericResourceList(viewModel.visibleNodes, selection: viewModel.state.selectedNode, action: viewModel.selectNode)

            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var rbacPane: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                genericResourceList(
                    viewModel.visibleRBACResources,
                    selection: viewModel.state.selectedRBACResource,
                    action: viewModel.selectRBACResource
                )
            default:
                EmptyView()
            }
        }
        .scrollContentBackground(.hidden)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var helmPane: some View {
        VStack(alignment: .leading, spacing: 8) {
            RuneSegmentedPickerInScroll(
                "Helm browser",
                selection: Binding(
                    get: { helmBrowserTab },
                    set: { tab in
                        helmBrowserTab = tab
                        viewModel.setHelmBrowserResourceFamily(tab.resourceListFamily)
                    }
                ),
                labelsHidden: true
            ) {
                ForEach(HelmBrowserTab.allCases) { tab in
                    Text(tab.localizedTitle(appString)).tag(tab)
                }
            }
            .accessibilityLabel("Helm browser")

            switch helmBrowserTab {
            case .releases:
                helmReleaseBrowser
            case .operatorResources:
                operatorResourceViews
            }
        }
        .padding(.horizontal, 2)
        .padding(.vertical, 2)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var helmReleaseBrowser: some View {
        resourceListGate(
            kindTitle: "Helm releases",
            visibleCount: viewModel.visibleHelmReleases.count,
            scopeDescription: viewModel.state.isHelmAllNamespaces ? "all namespaces" : resourceListScopeDescription
        ) {
            AppKitHelmReleaseListView(
                releases: viewModel.visibleHelmReleases,
                selectedReleaseID: viewModel.state.selectedHelmRelease?.id,
                sortColumn: viewModel.helmReleaseSortColumn,
                sortAscending: viewModel.helmReleaseSortAscending,
                onSelectRelease: { release in
                    helmBrowserTab = .releases
                    viewModel.selectHelmRelease(release)
                },
                onToggleSort: viewModel.toggleHelmReleaseSort
            )
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var operatorResourceViews: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 8) {
                RuneSegmentedPickerInScroll(
                    "Operator resource focus",
                    selection: Binding(
                        get: { viewModel.operatorResourceFocus },
                        set: { viewModel.setOperatorResourceFocus($0) }
                    )
                ) {
                    ForEach(OperatorResourceFocus.allCases) { focus in
                        Text(focus.title).tag(focus)
                    }
                }
                .frame(maxWidth: 220)

                Text(viewModel.operatorResourceFocusSummary)
                    .font(.caption.weight(.medium))
                    .foregroundStyle(.secondary)

                Spacer()
            }

            resourceListGate(
                kindTitle: viewModel.operatorResourceFocus == .all
                    ? "Operator resources"
                    : "\(viewModel.operatorResourceFocus.title) operator resources",
                visibleCount: viewModel.visibleOperatorResources.count,
                scopeDescription: "the selected operator focus"
            ) {
                HStack(spacing: 8) {
                    Text(viewModel.operatorResourcePageSummary)
                        .font(.caption.weight(.medium))
                        .foregroundStyle(.secondary)
                    Spacer()
                    RuneIconButton(
                        "Previous operator resource page",
                        systemImage: "chevron.left",
                        isDisabled: !viewModel.canPageOperatorResourcesBackward
                    ) {
                        viewModel.pageOperatorResourcesBackward()
                    }
                    RuneIconButton(
                        "Next operator resource page",
                        systemImage: "chevron.right",
                        isDisabled: !viewModel.canPageOperatorResourcesForward
                    ) {
                        viewModel.pageOperatorResourcesForward()
                    }

                    if let family = viewModel.operatorPrinterColumnFamilyForCustomization {
                        Menu {
                            Button(viewModel.showsOperatorPrinterColumnsForCurrentFamily ? "Hide Columns for \(family)" : "Show Columns for \(family)") {
                                viewModel.toggleOperatorPrinterColumnsForCurrentFamily()
                            }
                        } label: {
                            Label("Columns", systemImage: "tablecolumns")
                                .runeMinimumInteractiveTarget()
                        }
                        .controlSize(.small)
                    }
                }

                AppKitOperatorResourceListView(
                    resources: viewModel.pagedOperatorResources,
                    selectedResourceID: viewModel.state.selectedOperatorResource?.id,
                    sortColumn: viewModel.operatorResourceSortColumn,
                    sortAscending: viewModel.operatorResourceSortAscending,
                    showsPrinterColumns: viewModel.showsOperatorPrinterColumnsForCurrentFamily,
                    isFavorite: viewModel.isFavoriteOperatorResource,
                    onSelectResource: { resource in
                        helmBrowserTab = .operatorResources
                        viewModel.selectOperatorResource(resource)
                        genericResourceManifestTab = .overview
                        yamlManifestIsEditing = false
                    },
                    onToggleSort: viewModel.toggleOperatorResourceSort,
                    onToggleFavorite: viewModel.toggleFavoriteOperatorResource,
                    onOpenDescribe: { resource in
                        helmBrowserTab = .operatorResources
                        viewModel.selectOperatorResource(resource)
                        genericResourceManifestTab = .describe
                        yamlManifestIsEditing = false
                    },
                    onOpenYAML: { resource in
                        helmBrowserTab = .operatorResources
                        viewModel.selectOperatorResource(resource)
                        genericResourceManifestTab = .yaml
                        yamlManifestIsEditing = false
                    }
                )
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var eventsPane: some View {
        resourceListGate(kindTitle: "Events", visibleCount: viewModel.visibleEvents.count) {
            AppKitEventListView(
                events: viewModel.visibleEvents,
                selectedEventID: viewModel.state.selectedEvent?.id,
                sortColumn: viewModel.eventSortColumn,
                sortAscending: viewModel.eventSortAscending,
                onSelectEvent: viewModel.selectEvent,
                onToggleSort: viewModel.toggleEventSort
            )
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .runeHelp(viewModel.state.selectedEvent.map(eventHint(for:)) ?? "", enabled: showHoverTooltips)
        }
        .padding(.horizontal, 2)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var sectionPlaceholder: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(viewModel.state.selectedSection.localizedTitle(appString) + " is being implemented")
                .font(.title3.weight(.bold))
            Text("Flow and shortcuts are already wired so section switching feels instant.")
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .padding(20)
        .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous))
    }

    private var detailPane: some View {
        VStack(alignment: .leading, spacing: 12) {
            ZStack(alignment: .topLeading) {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous)
                    .fill(panelFill)

                Group {
                    switch viewModel.state.selectedSection {
                    case .overview:
                        overviewDetails
                    case .workloads:
                        workloadDetails
                    case .networking:
                        networkingDetails
                    case .config:
                        configDetails
                    case .storage:
                        storageDetails
                    case .events:
                        eventDetails
                    case .helm:
                        helmDetails
                    case .rbac:
                        rbacDetails
                    case .terminal:
                        terminalDetails
                    }
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                .padding(RuneUILayoutMetrics.paneInnerPadding)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .clipShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous))
            .overlay {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.paneShellCornerRadius, style: .continuous)
                    .strokeBorder(Color(nsColor: .separatorColor).opacity(0.16), lineWidth: 1)
            }
        }
        // `minWidth: 0` lets split columns shrink correctly; without it, nested scroll views can force odd horizontal alignment.
        .frame(minWidth: 0, maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .padding(RuneUILayoutMetrics.paneOuterPadding)
        .background {
            ZStack(alignment: .leading) {
                RuneGlassPaneSurface(role: .inspector)
                RuneGlassPaneBorder(role: .inspector)
                RuneRootLayoutProbe(kind: .detail, generation: layoutGeneration)
                paneFocusOutline(isFocused: keyboardPaneFocus == .detail)
            }
        }
    }

    private var overviewDetails: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Overview")
                .font(.title2.weight(.bold))

            VStack(alignment: .leading, spacing: 8) {
                inspectorInfoRow("Context", value: viewModel.state.selectedContext?.name ?? "-", symbol: "network")
                inspectorInfoRow("Namespace", value: viewModel.state.selectedNamespace, symbol: "square.stack.3d.up")
                inspectorInfoRow("Mode", value: viewModel.state.isReadOnlyMode ? "Read-only" : "Read/Write", symbol: "lock.shield")
            }

            inspectorActionButtonRow {
                Button("Open Workloads") {
                    viewModel.setSection(.workloads)
                }

                Button("Open Events") {
                    viewModel.setSection(.events)
                }

                Button("Open Helm") {
                    viewModel.setSection(.helm)
                }

                Button("Reload") {
                    viewModel.refreshCurrentView(debounced: false)
                }

                if !simpleMode {
                    Menu {
                        Button("Save Bundle") {
                            viewModel.saveSupportBundle()
                        }

                        Button("Save Bundle to Export Folder") {
                            viewModel.saveSupportBundleToExportFolder(openAfterSave: false)
                        }

                        Button("Save Bundle and Open") {
                            viewModel.saveSupportBundleToExportFolder(openAfterSave: true)
                        }
                    } label: {
                        Label("Save Bundle", systemImage: "square.and.arrow.down")
                            .runeMinimumInteractiveTarget()
                    }
                }
            }

            Divider()

            VStack(alignment: .leading, spacing: 8) {
                HStack(spacing: 8) {
                    Text("Write Audit")
                        .font(.headline)
                    Spacer(minLength: 0)
                    Menu {
                        Button("Export JSON…") {
                            viewModel.saveVisibleWriteAuditLog()
                        }

                        Button("Save JSON to Export Folder") {
                            viewModel.saveVisibleWriteAuditLogToExportFolder(openAfterSave: false)
                        }

                        Button("Save JSON and Open") {
                            viewModel.saveVisibleWriteAuditLogToExportFolder(openAfterSave: true)
                        }
                    } label: {
                        Label("Export", systemImage: "square.and.arrow.up")
                            .runeMinimumInteractiveTarget()
                    }
                    .buttonStyle(.bordered)
                    .controlSize(.small)
                    .disabled(viewModel.state.writeAuditLog.isEmpty)
                    .help("Export visible write audit entries")
                }

                if viewModel.state.writeAuditLog.isEmpty {
                    Text("No write actions in this session.")
                        .font(.footnote)
                        .foregroundStyle(.secondary)
                } else {
                    TextField("Search audit", text: $viewModel.writeAuditSearchQuery)
                        .textFieldStyle(.roundedBorder)
                        .controlSize(.small)

                    if viewModel.visibleWriteAuditEntries.isEmpty {
                        Text("No audit entries match the current search.")
                            .font(.footnote)
                            .foregroundStyle(.secondary)
                    }

                    ForEach(viewModel.visibleWriteAuditEntries.prefix(8)) { entry in
                        VStack(alignment: .leading, spacing: 3) {
                            HStack {
                                Text(entry.action)
                                    .font(.subheadline.weight(.semibold))
                                Spacer(minLength: 8)
                                Text(entry.status)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(entry.status == "Succeeded" ? Color.green : Color.red)
                            }
                            Text(entry.resource)
                                .font(.caption)
                                .lineLimit(1)
                                .help(entry.resource)
                            Text("\(entry.contextName) · \(entry.namespace) · \(entry.timestamp.formatted(date: .omitted, time: .standard))")
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                        }
                        .padding(.vertical, 4)
                    }
                }
            }

            Spacer(minLength: 0)
        }
    }

    private var workloadDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                podDetails
            case .deployment:
                deploymentDetails
            case .statefulSet:
                statefulSetDetails
            case .daemonSet:
                daemonSetDetails
            case .job:
                jobDetails
            case .cronJob:
                cronJobInspectorContent
            case .replicaSet:
                replicaSetDetails
            case .horizontalPodAutoscaler:
                horizontalPodAutoscalerDetails
            case .service, .endpoint, .ingress, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .networkPolicy, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                EmptyView()
            case .event:
                EmptyView()
            }
        }
    }

    private var replicaSetDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedReplicaSet) {
            let relatedPods = viewModel.selectedReplicaSetRelatedPods
            if !relatedPods.isEmpty {
                RelatedPodsRelationshipSection(pods: relatedPods, open: viewModel.openReplicaSetRelatedPod)
            }
        }
    }

    private var statefulSetDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedStatefulSet) {
            if viewModel.state.selectedStatefulSet != nil,
               let replicas = viewModel.selectedStatefulSetReplicaCounts {
                HStack(spacing: 8) {
                    Circle()
                        .fill(replicas.ready >= replicas.desired ? .green : (replicas.ready > 0 ? .orange : .red))
                        .frame(width: 8, height: 8)
                    Text("\(replicas.ready) of \(replicas.desired) ready")
                        .font(.body.weight(.semibold))
                }

                WorkloadReplicaScaleControlsView(
                    label: "Replicas",
                    isDirty: viewModel.scaleReplicaInput != replicas.desired,
                    canMutate: viewModel.canApplyClusterMutations,
                    replicas: $viewModel.scaleReplicaInput,
                    action: viewModel.requestScaleSelectedStatefulSet
                )

                Button("Restart Rollout") {
                    viewModel.requestRolloutRestartSelectedStatefulSet()
                }
                .buttonStyle(.bordered)
                .disabled(!viewModel.canApplyClusterMutations)

                Divider()
                    .opacity(0.45)
            }

            let relatedPods = viewModel.selectedStatefulSetRelatedPods
            if !relatedPods.isEmpty {
                RelatedPodsRelationshipSection(pods: relatedPods, open: viewModel.openStatefulSetRelatedPod)
            }
        }
    }

    private var daemonSetDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedDaemonSet) {
            let relatedPods = viewModel.selectedDaemonSetRelatedPods
            if !relatedPods.isEmpty {
                RelatedPodsRelationshipSection(pods: relatedPods, open: viewModel.openDaemonSetRelatedPod)
            }
        }
    }

    private var cronJobInspectorContent: some View {
        genericResourceDetails(resource: viewModel.state.selectedCronJob) {
            if viewModel.state.selectedCronJob != nil {
                HStack(spacing: 10) {
                    if viewModel.state.selectedCronJob?.secondaryText == "Suspended" {
                        Button("Resume") {
                            viewModel.setSelectedCronJobSuspended(false)
                        }
                        .disabled(!viewModel.canApplyClusterMutations)
                    } else {
                        Button("Suspend") {
                            viewModel.setSelectedCronJobSuspended(true)
                        }
                        .disabled(!viewModel.canApplyClusterMutations)
                    }
                    Button("Create job now") {
                        viewModel.createManualJobFromSelectedCronJob()
                    }
                    .disabled(!viewModel.canApplyClusterMutations)
                }
            }
            let relatedJobs = viewModel.selectedCronJobRelatedJobs
            if !relatedJobs.isEmpty {
                ResourceRelationshipSection(title: "Related Jobs", rowCount: relatedJobs.count) {
                    ForEach(relatedJobs) { job in
                        ResourceRelationshipLinkButton(
                            title: job.name,
                            subtitle: "\(job.namespace ?? viewModel.state.selectedNamespace) · \(job.primaryText)",
                            symbol: "checklist"
                        ) {
                            viewModel.openCronJobRelatedJob(job)
                        }
                    }
                }
            }
        }
    }

    private var jobDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedJob) {
            let relatedPods = viewModel.selectedJobRelatedPods
            if !relatedPods.isEmpty {
                RelatedPodsRelationshipSection(pods: relatedPods, open: viewModel.openJobRelatedPod)
            }
        }
    }

    private var horizontalPodAutoscalerDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedHorizontalPodAutoscaler) {
            if let target = viewModel.selectedHorizontalPodAutoscalerScaleTarget {
                ResourceRelationshipSection(title: "Scale Target") {
                    ResourceRelationshipLinkButton(
                        title: target.name,
                        subtitle: target.subtitle,
                        symbol: target.symbol
                    ) {
                        viewModel.openHorizontalPodAutoscalerScaleTarget(target)
                    }
                }
            }
        }
    }

    private var networkingDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                serviceDetails
            case .ingress:
                ingressDetails
            case .endpoint:
                genericResourceDetails(resource: viewModel.state.selectedEndpoint)
            case .networkPolicy:
                genericResourceDetails(resource: viewModel.state.selectedNetworkPolicy)
            case .pod, .deployment, .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .configMap, .secret, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .event, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                EmptyView()
            }
        }
    }

    private var ingressDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedIngress) {
            let relatedServices = viewModel.selectedIngressRelatedServices
            if !relatedServices.isEmpty {
                ResourceRelationshipSection(title: "Related Services", rowCount: relatedServices.count) {
                    ForEach(relatedServices) { service in
                        ResourceRelationshipLinkButton(
                            title: service.name,
                            subtitle: "\(service.namespace) · \(service.type) · \(service.clusterIP)",
                            symbol: "point.3.connected.trianglepath.dotted"
                        ) {
                            viewModel.openIngressRelatedService(service)
                        }
                    }
                }
            }
        }
    }

    private var configDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .configMap:
                genericResourceDetails(resource: viewModel.state.selectedConfigMap)
            case .secret:
                genericResourceDetails(resource: viewModel.state.selectedSecret)
            case .pod, .deployment, .statefulSet, .daemonSet, .job, .cronJob, .replicaSet, .horizontalPodAutoscaler, .service, .endpoint, .ingress, .networkPolicy, .node, .persistentVolumeClaim, .persistentVolume, .storageClass, .event, .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
                EmptyView()
            }
        }
    }

    private var storageDetails: some View {
        Group {
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolumeClaim:
                persistentVolumeClaimDetails
            case .persistentVolume:
                persistentVolumeDetails
            case .storageClass:
                genericResourceDetails(resource: viewModel.state.selectedStorageClass)
            case .node:
                nodeDetails
            default:
                EmptyView()
            }
        }
    }

    private var persistentVolumeClaimDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedPersistentVolumeClaim) {
            if let persistentVolume = viewModel.selectedPersistentVolumeClaimRelatedPersistentVolume {
                ResourceRelationshipSection(title: "Related PersistentVolume") {
                    ResourceRelationshipLinkButton(
                        title: persistentVolume.name,
                        subtitle: "\(persistentVolume.primaryText) · \(persistentVolume.secondaryText)",
                        symbol: "externaldrive.badge.checkmark"
                    ) {
                        viewModel.openPersistentVolumeClaimRelatedPersistentVolume(persistentVolume)
                    }
                }
            }
        }
    }

    private var persistentVolumeDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedPersistentVolume) {
            let relatedClaims = viewModel.selectedPersistentVolumeRelatedPersistentVolumeClaims
            if !relatedClaims.isEmpty {
                ResourceRelationshipSection(title: "Related PVCs", rowCount: relatedClaims.count) {
                    ForEach(relatedClaims) { pvc in
                        ResourceRelationshipLinkButton(
                            title: pvc.name,
                            subtitle: "\(pvc.namespace ?? viewModel.state.selectedNamespace) · \(pvc.primaryText)",
                            symbol: "externaldrive.badge.person.crop"
                        ) {
                            viewModel.openPersistentVolumeRelatedPersistentVolumeClaim(pvc)
                        }
                    }
                }
            }
        }
    }

    private var nodeDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedNode) {
            let relatedPods = viewModel.selectedNodeRelatedPods
            if !relatedPods.isEmpty {
                RelatedPodsRelationshipSection(pods: relatedPods, open: viewModel.openNodeRelatedPod)
            }
        }
    }

    private var rbacDetails: some View {
        genericResourceDetails(resource: viewModel.state.selectedRBACResource) {
            if let role = viewModel.selectedRBACBindingReferencedRole {
                ResourceRelationshipSection(title: "Referenced Role") {
                    ResourceRelationshipLinkButton(
                        title: role.name,
                        subtitle: "\(role.namespace ?? "Cluster") · \(role.primaryText)",
                        symbol: role.kind == .clusterRole ? "lock.shield" : "person.badge.key"
                    ) {
                        viewModel.openRBACBindingReferencedRole(role)
                    }
                }
            }

            let relatedBindings = viewModel.selectedRBACRoleRelatedBindings
            if !relatedBindings.isEmpty {
                ResourceRelationshipSection(title: "Related Bindings", rowCount: relatedBindings.count) {
                    ForEach(relatedBindings) { binding in
                        ResourceRelationshipLinkButton(
                            title: binding.name,
                            subtitle: "\(binding.namespace ?? "Cluster") · \(binding.secondaryText)",
                            symbol: binding.kind == .clusterRoleBinding ? "person.2.badge.key" : "person.badge.key"
                        ) {
                            viewModel.openRBACRoleRelatedBinding(binding)
                        }
                    }
                }
            }

            if !simpleMode {
                RBACCanISimulatorPanel(viewModel: viewModel)
            }

            Divider()
        }
    }

    private var helmDetails: some View {
        Group {
            if let resource = viewModel.state.selectedOperatorResource {
                operatorResourceDetails(resource: resource)
            } else if let release = viewModel.state.selectedHelmRelease {
                RuneInspectorScaffold(
                    title: release.name,
                    copyAccessibilityLabel: "Copy Helm release name",
                    bodyScrollBehavior: helmInspectorTab == .overview || helmInspectorTab == .history
                        ? .vertical
                        : .selfManaged,
                    showsActions: helmInspectorTab != .overview,
                    onCopy: { copyToClipboard(release.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        helmReleaseCoreInfo(release)
                    },
                    tabs: {
                        RuneSegmentedPickerInScroll(
                            "",
                            selection: $helmInspectorTab,
                            labelsHidden: true
                        ) {
                            ForEach(HelmInspectorTab.allCases) { tab in
                                Text(tab.localizedTitle(appString)).tag(tab)
                            }
                        }
                        .accessibilityLabel("Helm inspector")
                    },
                    actions: {
                        helmInspectorActions
                    },
                    content: {
                        switch helmInspectorTab {
                        case .overview:
                            inspectorInfoRow("Updated", value: release.updated, symbol: "calendar.badge.clock")
                        case .values:
                            exportableTextPane(
                                text: viewModel.state.helmValues,
                                emptyText: "No values loaded"
                            )
                        case .manifest:
                            exportableTextPane(
                                text: viewModel.state.helmManifest,
                                emptyText: "No manifest loaded"
                            )
                        case .history:
                            helmHistoryPane
                        }
                    }
                )
            } else {
                inspectorEmptyState(
                    .unselected(
                        title: "Select a Helm release",
                        message: "Select an item in the center list to inspect details and actions here."
                    ),
                    symbol: "ferry"
                )
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private func helmReleaseCoreInfo(_ release: HelmReleaseSummary) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            if shouldShowResourceNamespaceLabel(release.namespace) {
                inspectorInfoRow("Namespace", value: release.namespace, symbol: "square.stack.3d.up")
            }
            inspectorInfoRow("Status", value: release.status.capitalized, symbol: "checkmark.seal")
            inspectorInfoRow("Chart", value: release.chart, symbol: "shippingbox")
            inspectorInfoRow("App Version", value: release.appVersion, symbol: "tag")
            inspectorInfoRow("Revision", value: String(release.revision), symbol: "clock.arrow.trianglehead.counterclockwise.rotate.90")
        }
    }

    @ViewBuilder
    private var helmInspectorActions: some View {
        switch helmInspectorTab {
        case .values:
            Button("Save Values…", action: viewModel.saveCurrentHelmValues)
                .buttonStyle(.bordered)
        case .manifest:
            Button("Save Manifest…", action: viewModel.saveCurrentHelmManifest)
                .buttonStyle(.bordered)
        case .history:
            Button("Save History…", action: viewModel.saveCurrentHelmHistory)
                .buttonStyle(.bordered)
        case .overview:
            EmptyView()
        }
    }

    @ViewBuilder
    private var helmHistoryPane: some View {
        if viewModel.state.helmHistory.isEmpty {
            inspectorEmptyState(
                .empty(
                    title: "No history loaded",
                    message: "Revision history will appear after it is returned by the cluster."
                ),
                symbol: "clock.arrow.circlepath"
            )
        } else {
            VStack(alignment: .leading, spacing: 10) {
                HelmRollbackOptionsView(
                    wait: $viewModel.helmRollbackWait,
                    timeout: $viewModel.helmRollbackTimeoutInput,
                    cleanupOnFail: $viewModel.helmRollbackCleanupOnFail
                )
                .padding(10)
                .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))

                LazyVStack(alignment: .leading, spacing: 10) {
                    ForEach(viewModel.state.helmHistory) { entry in
                        VStack(alignment: .leading, spacing: 4) {
                            HStack {
                                Text("Revision \(entry.revision)")
                                    .font(.subheadline.weight(.semibold))
                                Spacer()
                                Button("Rollback") {
                                    viewModel.requestHelmRollback(revision: entry.revision)
                                }
                                .disabled(!viewModel.canApplyClusterMutations)
                                Text(entry.status.capitalized)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(statusColor(for: entry.status))
                            }
                            Text(entry.chart + " • " + entry.appVersion)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            Text(entry.description)
                                .font(.footnote)
                            Text(entry.updated)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        .padding(10)
                        .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
                    }
                }
            }
        }
    }

    private func operatorResourceDetails(resource: OperatorResourceSummary) -> some View {
        RuneInspectorScaffold(
            title: resource.name,
            copyAccessibilityLabel: "Copy \(resource.kind) name",
            bodyScrollBehavior: genericResourceManifestTab == .overview ? .vertical : .selfManaged,
            showsActions: false,
            onCopy: { copyToClipboard(resource.name) },
            onRefresh: refreshDetailPane,
            info: {
                VStack(alignment: .leading, spacing: 6) {
                    inspectorInfoRow("Family", value: resource.family, symbol: "shippingbox")
                    if let namespace = resource.namespace, shouldShowResourceNamespaceLabel(namespace) {
                        inspectorInfoRow("Namespace", value: namespace, symbol: "square.stack.3d.up")
                    } else if resource.namespace == nil {
                        inspectorInfoRow("Scope", value: "Cluster", symbol: "square.stack.3d.up")
                    }
                    inspectorInfoRow("Status", value: resource.status, symbol: "checkmark.seal")
                    inspectorInfoRow("API Path", value: resource.apiPath, symbol: "curlybraces")
                }
            },
            tabs: {
                RuneSegmentedPickerInScroll(appString(.manifest), selection: $genericResourceManifestTab) {
                    ForEach(GenericResourceManifestTab.allCases) { tab in
                        Text(tab.localizedTitle(appString)).tag(tab)
                    }
                }
            },
            actions: {
                EmptyView()
            },
            content: {
                switch genericResourceManifestTab {
                case .overview:
                    operatorResourceOverview(resource)
                case .describe, .yaml:
                    manifestInspectorPane(activeTab: genericResourceManifestTab)
                }
            }
        )
    }

    private func operatorResourceOverview(_ resource: OperatorResourceSummary) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            if !resource.message.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                Text(resource.message)
                    .font(.callout)
                    .textSelection(.enabled)
                    .fixedSize(horizontal: false, vertical: true)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(10)
                    .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
            }

            ForEach(Array(resource.printerColumns.enumerated()), id: \.offset) { _, column in
                inspectorInfoRow(column.title, value: column.value, symbol: "list.bullet.rectangle")
            }
        }
    }

    private var podDetails: some View {
        Group {
            if let pod = viewModel.state.selectedPod {
                RuneInspectorScaffold(
                    title: pod.name,
                    copyAccessibilityLabel: "Copy Pod name",
                    bodyScrollBehavior: podInspectorTab == .overview ? .vertical : .selfManaged,
                    showsActions: podInspectorTab == .overview,
                    onCopy: { copyToClipboard(pod.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        podInspectorCoreInfo(pod)
                    },
                    tabs: {
                    RuneSegmentedPickerInScroll(
                        "",
                        selection: $podInspectorTab,
                        labelsHidden: true
                    ) {
                        ForEach(PodInspectorTab.allCases) { tab in
                            Text(tab.localizedTitle(appString)).tag(tab)
                        }
                    }
                    .accessibilityLabel("Inspector")
                    },
                    actions: {
                        Button("Delete", role: .destructive) {
                            viewModel.requestDeleteSelectedResource()
                        }
                        .disabled(!viewModel.canApplyClusterMutations)
                    },
                    content: {
                    Group {
                        switch podInspectorTab {
                        case .overview:
                            podOverviewSection(pod: pod)

                        case .logs:
                            PodLogsInspectorPane(
                                selectedLogPreset: $viewModel.selectedLogPreset,
                                includePreviousLogs: $viewModel.includePreviousLogs,
                                selectedContainer: $viewModel.selectedLogContainer,
                                isTailModeEnabled: $viewModel.isLogTailModeEnabled,
                                isStreamPaused: $viewModel.isLogStreamPaused,
                                isLoadingLogs: viewModel.state.isLoadingLogs,
                                isLoadingResources: viewModel.state.isLoading,
                                errorMessage: viewModel.state.lastLogFetchError,
                                statusText: logStatusText,
                                containerOptions: viewModel.podLogContainerOptions,
                                logText: viewModel.state.podLogs,
                                readOnlyResetID: "podlogs:\(viewModel.state.selectedPod?.name ?? ""):\(viewModel.selectedLogPreset.id):\(viewModel.includePreviousLogs):\(viewModel.selectedLogContainer)",
                                onReload: { viewModel.reloadLogsForSelection() },
                                onSave: { viewModel.saveCurrentLogs() },
                                onSaveToExportFolder: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: false) },
                                onSaveAndOpen: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: true) },
                                onSaveVisibleZip: { viewModel.saveVisibleLogsZip(visibleText: $0) },
                                onSaveVisibleZipToExportFolder: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: false) },
                                onSaveVisibleZipAndOpen: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: true) },
                                onSaveFullZip: { viewModel.saveCurrentLogsZip() },
                                onSaveFullZipToExportFolder: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: false) },
                                onSaveFullZipAndOpen: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: true) },
                                onSaveAllPodsZip: { viewModel.saveAllPodsLogsZip() },
                                onSaveAllPodsZipToExportFolder: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: false) },
                                onSaveAllPodsZipAndOpen: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: true) },
                                onCopySelection: { copySelectedTextFromFocusedTextView() },
                                onCopyAll: { viewModel.copyCurrentLogsToClipboard() },
                                onToggleStreamPause: { viewModel.toggleLogStreamPause() }
                            )

                        case .exec:
                            execPane(for: pod)

                        case .portForward:
                            portForwardPane(targetKind: .pod, targetName: pod.name)

                        case .describe:
                            manifestInspectorPane(activeTab: .describe)

                        case .yaml:
                            manifestInspectorPane(activeTab: .yaml)
                        }
                    }
                    .transaction { transaction in
                        transaction.animation = nil
                    }
                    }
                )
            } else {
                inspectorEmptyState(
                    .unselected(
                        title: "Select a pod",
                        message: "Select an item in the center list to inspect details and actions here."
                    ),
                    symbol: "cube.box"
                )
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var deploymentDetails: some View {
        Group {
            if let deployment = viewModel.state.selectedDeployment {
                RuneInspectorScaffold(
                    title: deployment.name,
                    copyAccessibilityLabel: "Copy Deployment name",
                    bodyScrollBehavior: deploymentInspectorTab == .overview ? .vertical : .selfManaged,
                    showsActions: deploymentInspectorTab == .overview,
                    onCopy: { copyToClipboard(deployment.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        deploymentInspectorCoreInfo(deployment)
                    },
                    tabs: {
                    RuneSegmentedPickerInScroll(
                        "",
                        selection: $deploymentInspectorTab,
                        labelsHidden: true
                    ) {
                        ForEach(DeploymentInspectorTab.allCases) { tab in
                            Text(tab.localizedTitle(appString)).tag(tab)
                        }
                    }
                    .accessibilityLabel("Inspector")
                    },
                    actions: {
                        deploymentInspectorActions
                    },
                    content: {
                    Group {
                        switch deploymentInspectorTab {
                        case .overview:
                            deploymentOverviewSection(deployment: deployment)

                        case .unifiedLogs:
                            UnifiedResourceLogsInspectorPane(
                                selectedLogPreset: $viewModel.selectedLogPreset,
                                includePreviousLogs: $viewModel.includePreviousLogs,
                                isTailModeEnabled: $viewModel.isLogTailModeEnabled,
                                isStreamPaused: $viewModel.isLogStreamPaused,
                                isLoadingLogs: viewModel.state.isLoadingLogs,
                                isLoadingResources: viewModel.state.isLoading,
                                errorMessage: viewModel.state.lastLogFetchError,
                                statusText: logStatusText,
                                podNames: viewModel.state.unifiedServiceLogPods,
                                logText: viewModel.state.unifiedServiceLogs,
                                readOnlyResetID: "unifiedlogs:\(viewModel.state.selectedDeployment?.name ?? ""):\(viewModel.selectedLogPreset.id):\(viewModel.includePreviousLogs)",
                                onReload: { viewModel.reloadLogsForSelection() },
                                onSave: { viewModel.saveCurrentLogs() },
                                onSaveToExportFolder: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: false) },
                                onSaveAndOpen: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: true) },
                                onSaveVisibleZip: { viewModel.saveVisibleLogsZip(visibleText: $0) },
                                onSaveVisibleZipToExportFolder: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: false) },
                                onSaveVisibleZipAndOpen: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: true) },
                                onSaveFullZip: { viewModel.saveCurrentLogsZip() },
                                onSaveFullZipToExportFolder: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: false) },
                                onSaveFullZipAndOpen: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: true) },
                                onSaveAllPodsZip: { viewModel.saveAllPodsLogsZip() },
                                onSaveAllPodsZipToExportFolder: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: false) },
                                onSaveAllPodsZipAndOpen: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: true) },
                                onCopySelection: { copySelectedTextFromFocusedTextView() },
                                onCopyAll: { viewModel.copyCurrentLogsToClipboard() },
                                onToggleStreamPause: { viewModel.toggleLogStreamPause() }
                            )

                        case .rollout:
                            VStack(alignment: .leading, spacing: 10) {
                                RuneAdaptiveToolbar("Deployment rollout actions") {
                                    TextField("Revision (optional)", text: $viewModel.rolloutRevisionInput)
                                        .textFieldStyle(.roundedBorder)
                                        .frame(maxWidth: 160)
                                } secondary: {
                                    HStack(spacing: 8) {
                                        Button("Rollback") {
                                            viewModel.requestRolloutUndoSelectedDeployment()
                                        }
                                        .disabled(!viewModel.canApplyClusterMutations)

                                        Button("Save History") {
                                            viewModel.saveCurrentRolloutHistory()
                                        }
                                    }
                                }

                                DeploymentRolloutHistoryView(history: viewModel.state.deploymentRolloutHistory)
                            }

                        case .describe:
                            manifestInspectorPane(activeTab: .describe)

                        case .yaml:
                            manifestInspectorPane(activeTab: .yaml)
                        }
                    }
                    .transaction { transaction in
                        transaction.animation = nil
                    }
                    }
                )
            } else {
                inspectorEmptyState(
                    .unselected(
                        title: "Select a deployment",
                        message: "Select an item in the center list to inspect details and actions here."
                    ),
                    symbol: "shippingbox"
                )
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var serviceDetails: some View {
        Group {
            if let service = viewModel.state.selectedService {
                RuneInspectorScaffold(
                    title: service.name,
                    copyAccessibilityLabel: "Copy Service name",
                    bodyScrollBehavior: serviceInspectorTab == .overview ? .vertical : .selfManaged,
                    showsActions: serviceInspectorTab == .overview,
                    onCopy: { copyToClipboard(service.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        serviceInspectorCoreInfo(service)
                    },
                    tabs: {
                    RuneSegmentedPickerInScroll(
                        "",
                        selection: $serviceInspectorTab,
                        labelsHidden: true
                    ) {
                        ForEach(ServiceInspectorTab.allCases) { tab in
                            Text(tab.localizedTitle(appString)).tag(tab)
                        }
                    }
                    .accessibilityLabel("Inspector")
                    },
                    actions: {
                        serviceInspectorActions
                    },
                    content: {
                    Group {
                        switch serviceInspectorTab {
                        case .overview:
                            VStack(alignment: .leading, spacing: 12) {
                                let relatedPods = viewModel.selectedServiceRelatedPods
                                if !relatedPods.isEmpty {
                                    RelatedPodsRelationshipSection(pods: relatedPods, open: viewModel.openServiceRelatedPod)
                                }
                                let relatedIngresses = viewModel.selectedServiceRelatedIngresses
                                if !relatedIngresses.isEmpty {
                                    ResourceRelationshipSection(title: "Related Ingresses", rowCount: relatedIngresses.count) {
                                        ForEach(relatedIngresses) { ingress in
                                            ResourceRelationshipLinkButton(
                                                title: ingress.name,
                                                subtitle: "\(ingress.namespace ?? service.namespace) · \(ingress.primaryText)",
                                                symbol: "point.3.filled.connected.trianglepath.dotted"
                                            ) {
                                                viewModel.openServiceRelatedIngress(ingress)
                                            }
                                        }
                                    }
                                }
                                let relatedEvents = viewModel.selectedServiceRelatedEvents
                                if !relatedEvents.isEmpty {
                                    RelatedEventsRelationshipSection(events: relatedEvents, open: viewModel.openRelatedEvent)
                                }
                            }

                        case .unifiedLogs:
                            UnifiedResourceLogsInspectorPane(
                                selectedLogPreset: $viewModel.selectedLogPreset,
                                includePreviousLogs: $viewModel.includePreviousLogs,
                                isTailModeEnabled: $viewModel.isLogTailModeEnabled,
                                isStreamPaused: $viewModel.isLogStreamPaused,
                                isLoadingLogs: viewModel.state.isLoadingLogs,
                                isLoadingResources: viewModel.state.isLoading,
                                errorMessage: viewModel.state.lastLogFetchError,
                                statusText: logStatusText,
                                podNames: viewModel.state.unifiedServiceLogPods,
                                logText: viewModel.state.unifiedServiceLogs,
                                readOnlyResetID: "unifiedlogs:\(viewModel.state.selectedService?.name ?? ""):\(viewModel.selectedLogPreset.id):\(viewModel.includePreviousLogs)",
                                onReload: { viewModel.reloadLogsForSelection() },
                                onSave: { viewModel.saveCurrentLogs() },
                                onSaveToExportFolder: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: false) },
                                onSaveAndOpen: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: true) },
                                onSaveVisibleZip: { viewModel.saveVisibleLogsZip(visibleText: $0) },
                                onSaveVisibleZipToExportFolder: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: false) },
                                onSaveVisibleZipAndOpen: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: true) },
                                onSaveFullZip: { viewModel.saveCurrentLogsZip() },
                                onSaveFullZipToExportFolder: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: false) },
                                onSaveFullZipAndOpen: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: true) },
                                onSaveAllPodsZip: { viewModel.saveAllPodsLogsZip() },
                                onSaveAllPodsZipToExportFolder: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: false) },
                                onSaveAllPodsZipAndOpen: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: true) },
                                onCopySelection: { copySelectedTextFromFocusedTextView() },
                                onCopyAll: { viewModel.copyCurrentLogsToClipboard() },
                                onToggleStreamPause: { viewModel.toggleLogStreamPause() }
                            )

                        case .portForward:
                            portForwardPane(targetKind: .service, targetName: service.name)

                        case .describe:
                            manifestInspectorPane(activeTab: .describe)

                        case .yaml:
                            manifestInspectorPane(activeTab: .yaml)
                        }
                    }
                    .transaction { transaction in
                        transaction.animation = nil
                    }
                    }
                )
            } else {
                inspectorEmptyState(
                    .unselected(
                        title: "Select a service",
                        message: "Select an item in the center list to inspect details and actions here."
                    ),
                    symbol: "point.3.connected.trianglepath.dotted"
                )
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private func serviceInspectorCoreInfo(_ service: ServiceSummary) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            if shouldShowResourceNamespaceLabel(service.namespace) {
                inspectorInfoRow("Namespace", value: service.namespace, symbol: "square.stack.3d.up")
            }
            inspectorInfoRow("Type", value: service.type, symbol: "point.3.connected.trianglepath.dotted")
            inspectorInfoRow("Cluster IP", value: service.clusterIP, symbol: "network")
        }
    }

    @ViewBuilder
    private var serviceInspectorActions: some View {
        Button(appString(.applyYAML)) {
            viewModel.requestApplySelectedResourceYAML()
        }
        .buttonStyle(.bordered)
        .disabled(!viewModel.canApplyClusterMutations)

        Button("Export…") {
            viewModel.saveCurrentResourceYAML()
        }
        .buttonStyle(.bordered)

        Button("Delete", role: .destructive) {
            viewModel.requestDeleteSelectedResource()
        }
        .disabled(!viewModel.canApplyClusterMutations)
    }

    private var eventDetails: some View {
        Group {
            if let event = viewModel.state.selectedEvent {
                RuneInspectorScaffold(
                    title: event.reason,
                    copyAccessibilityLabel: "Copy event reason",
                    bodyScrollBehavior: .vertical,
                    showsTabs: false,
                    showsActions: !event.objectName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty,
                    onCopy: { copyToClipboard(event.reason) },
                    onRefresh: refreshDetailPane,
                    info: {
                        eventInspectorCoreInfo(event)
                    },
                    tabs: {
                        EmptyView()
                    },
                    actions: {
                        Button(eventGoToResourceButtonTitle(for: event)) {
                            viewModel.openEventSource(event)
                        }
                        .buttonStyle(.borderedProminent)
                        .help("Switches section and selects the involved object when it appears in the list.")
                    },
                    content: {
                        Text(event.message.isEmpty ? "No event message" : event.message)
                            .font(.system(size: 12, weight: .regular, design: .monospaced))
                            .textSelection(.enabled)
                            .fixedSize(horizontal: false, vertical: true)
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .padding(10)
                            .background(editorFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
                    }
                )
            } else {
                inspectorEmptyState(
                    .unselected(
                        title: "Select an event",
                        message: "Select an item in the center list to inspect details and actions here."
                    ),
                    symbol: "bolt.badge.clock"
                )
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private func eventInspectorCoreInfo(_ event: EventSummary) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            if let timestamp = event.lastTimestamp?.trimmingCharacters(in: .whitespacesAndNewlines), !timestamp.isEmpty {
                inspectorInfoRow("Time", value: timestamp, symbol: "clock")
            }
            inspectorInfoRow("Type", value: event.type, symbol: "exclamationmark.bubble")

            let kind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
            let object = kind.isEmpty ? event.objectName : "\(kind) / \(event.objectName)"
            inspectorInfoRow("Object", value: object, symbol: "cube")

            if let namespace = event.involvedNamespace?.trimmingCharacters(in: .whitespacesAndNewlines), !namespace.isEmpty {
                inspectorInfoRow("Namespace", value: namespace, symbol: "square.stack.3d.up")
            }
        }
    }

    private func eventGoToResourceButtonTitle(for event: EventSummary) -> String {
        let kind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() ?? ""
        switch kind {
        case "pod": return "Go to Pod"
        case "deployment": return "Go to Deployment"
        case "statefulset": return "Go to StatefulSet"
        case "daemonset": return "Go to DaemonSet"
        case "service": return "Go to Service"
        case "ingress": return "Go to Ingress"
        case "configmap": return "Go to ConfigMap"
        case "secret": return "Go to Secret"
        case "node": return "Go to Node"
        case "": return "Go to resource"
        default: return "Go to resource"
        }
    }

    private func genericResourceDetails(resource: ClusterResourceSummary?) -> some View {
        genericResourceDetails(resource: resource) {
            EmptyView()
        }
    }

    private func genericResourceDetails<Supplementary: View>(
        resource: ClusterResourceSummary?,
        @ViewBuilder supplementary: () -> Supplementary
    ) -> some View {
        Group {
            if let resource {
                RuneInspectorScaffold(
                    title: resource.name,
                    copyAccessibilityLabel: "Copy \(resource.kind.localizedTitle(appString)) name",
                    bodyScrollBehavior: genericResourceManifestTab == .overview ? .vertical : .selfManaged,
                    showsActions: genericResourceManifestTab == .overview && resource.kind == .configMap,
                    onCopy: { copyToClipboard(resource.name) },
                    onRefresh: refreshDetailPane,
                    info: {
                        VStack(alignment: .leading, spacing: 6) {
                            if let namespace = resource.namespace, shouldShowResourceNamespaceLabel(namespace) {
                                inspectorInfoRow("Namespace", value: namespace, symbol: "square.stack.3d.up")
                            }
                            inspectorInfoRow("Primary", value: resource.primaryText, symbol: "info.circle")
                            inspectorInfoRow("Status", value: resource.secondaryText, symbol: "text.alignleft")
                        }
                    },
                    tabs: {
                        RuneSegmentedPickerInScroll(appString(.manifest), selection: $genericResourceManifestTab) {
                            ForEach(GenericResourceManifestTab.allCases) { tab in
                                Text(tab.localizedTitle(appString)).tag(tab)
                            }
                        }
                    },
                    actions: {
                        if resource.kind == .configMap {
                            Button("Edit ConfigMap YAML") {
                                genericResourceManifestTab = .yaml
                                yamlManifestIsEditing = resolvedManifestInlineEditorImplementation.supportsInlineEditing
                            }
                            .buttonStyle(.borderedProminent)
                            .disabled(!viewModel.canApplyClusterMutations)
                            .help("Switches to YAML and enables inline editing for this ConfigMap.")
                        }
                    },
                    content: {
                        switch genericResourceManifestTab {
                        case .overview:
                            genericResourceOverview(resource: resource, supplementary: supplementary)
                        case .describe, .yaml:
                            manifestInspectorPane(activeTab: genericResourceManifestTab)
                        }
                    }
                )
            } else {
                genericResourceEmptyState
            }
        }
    }

    private func genericResourceOverview<Supplementary: View>(
        resource: ClusterResourceSummary,
        @ViewBuilder supplementary: () -> Supplementary
    ) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            supplementary()

            let relatedEvents = viewModel.relatedEvents(for: resource)
            if !relatedEvents.isEmpty {
                RelatedEventsRelationshipSection(events: relatedEvents, open: viewModel.openRelatedEvent)
            }

            if resource.kind == .statefulSet || resource.kind == .daemonSet {
                HStack(spacing: 10) {
                    TextField("Revision (optional)", text: $viewModel.rolloutRevisionInput)
                        .textFieldStyle(.roundedBorder)
                        .frame(maxWidth: 160)

                    Button("Rollback") {
                        viewModel.requestRolloutUndoSelectedController()
                    }
                    .disabled(!viewModel.canApplyClusterMutations)

                    Spacer(minLength: 0)
                }
            }
        }
    }

    private var yamlManifestDocumentState: ManifestDocumentState {
        ManifestDocumentState.resolved(
            content: viewModel.state.resourceYAML,
            isLoading: viewModel.state.isLoadingResourceDetails,
            error: viewModel.state.lastResourceYAMLError,
            loadingTitle: "Loading YAML",
            loadingMessage: "Fetching \(manifestResourceReference) from the cluster.",
            failureTitle: "YAML could not be refreshed",
            emptyTitle: "No YAML available",
            emptyMessage: "The cluster returned no manifest for \(manifestResourceReference)."
        )
    }

    private var yamlFooterText: String {
        if viewModel.state.isLoadingResourceDetails {
            return "Loading resource YAML from the cluster."
        }
        if viewModel.state.lastResourceYAMLError != nil {
            return "YAML could not be loaded for the current selection. Check context, namespace, and cluster access in Settings."
        }
        return "No YAML was returned for the current selection yet. You can also use Import… to paste YAML from a file."
    }

    private var describeManifestDocumentState: ManifestDocumentState {
        ManifestDocumentState.resolved(
            content: viewModel.state.resourceDescribe,
            isLoading: viewModel.state.isLoadingResourceDetails,
            error: viewModel.state.lastResourceDescribeError,
            loadingTitle: "Loading describe output",
            loadingMessage: "Fetching \(manifestResourceReference) from the cluster.",
            failureTitle: "Describe could not be refreshed",
            emptyTitle: "No describe output",
            emptyMessage: "The cluster returned no describe content for \(manifestResourceReference)."
        )
    }

    private var logStatusText: String {
        if viewModel.state.isLoadingLogs {
            return "Refreshing logs"
        }
        if let error = viewModel.state.lastLogFetchError?.trimmingCharacters(in: .whitespacesAndNewlines), !error.isEmpty {
            return "Reconnect failed"
        }
        if let date = viewModel.state.lastLogUpdatedAt {
            if viewModel.isLogTailModeEnabled {
                if viewModel.isLogStreamPaused {
                    return "Paused - last updated \(date.formatted(date: .omitted, time: .standard))"
                }
                return "Live tailing - updated \(date.formatted(date: .omitted, time: .standard))"
            }
            return "Tail off - last updated \(date.formatted(date: .omitted, time: .standard))"
        }
        if viewModel.isLogTailModeEnabled {
            if viewModel.isLogStreamPaused {
                return "Paused"
            }
            return "Live tailing"
        }
        if viewModel.includePreviousLogs {
            return "Previous logs requested"
        }
        return "No log read yet"
    }

    private var manifestStatusText: String {
        if viewModel.state.isLoadingResourceDetails {
            return "Refreshing details"
        }
        if viewModel.state.lastResourceYAMLError != nil || viewModel.state.lastResourceDescribeError != nil {
            return "Stale details"
        }
        if let date = viewModel.state.lastResourceDetailsUpdatedAt {
            return "Last updated \(date.formatted(date: .omitted, time: .standard))"
        }
        return "No detail read yet"
    }

    private var yamlDraftBinding: Binding<String> {
        Binding(
            get: { viewModel.state.resourceYAML },
            set: { viewModel.state.updateResourceYAMLDraft($0) }
        )
    }

    private var manifestResourceReference: String {
        if viewModel.state.selectedSection == .helm,
           let resource = viewModel.state.selectedOperatorResource {
            return "\(resource.kind) \(resource.name)"
        }

        switch viewModel.state.selectedWorkloadKind {
        case .pod:
            return viewModel.state.selectedPod.map { "pod \($0.name)" } ?? "the selected pod"
        case .deployment:
            return viewModel.state.selectedDeployment.map { "deployment \($0.name)" } ?? "the selected deployment"
        case .service:
            return viewModel.state.selectedService.map { "service \($0.name)" } ?? "the selected service"
        case .statefulSet:
            return viewModel.state.selectedStatefulSet.map { "statefulset \($0.name)" } ?? "the selected statefulset"
        case .daemonSet:
            return viewModel.state.selectedDaemonSet.map { "daemonset \($0.name)" } ?? "the selected daemonset"
        case .job:
            return viewModel.state.selectedJob.map { "job \($0.name)" } ?? "the selected job"
        case .cronJob:
            return viewModel.state.selectedCronJob.map { "cronjob \($0.name)" } ?? "the selected cronjob"
        case .replicaSet:
            return viewModel.state.selectedReplicaSet.map { "replicaset \($0.name)" } ?? "the selected replicaset"
        case .ingress:
            return viewModel.state.selectedIngress.map { "ingress \($0.name)" } ?? "the selected ingress"
        case .endpoint:
            return viewModel.state.selectedEndpoint.map { "endpoints \($0.name)" } ?? "the selected endpoints"
        case .configMap:
            return viewModel.state.selectedConfigMap.map { "configmap \($0.name)" } ?? "the selected configmap"
        case .secret:
            return viewModel.state.selectedSecret.map { "secret \($0.name)" } ?? "the selected secret"
        case .node:
            return viewModel.state.selectedNode.map { "node \($0.name)" } ?? "the selected node"
        case .persistentVolumeClaim:
            return viewModel.state.selectedPersistentVolumeClaim.map { "pvc \($0.name)" } ?? "the selected PVC"
        case .persistentVolume:
            return viewModel.state.selectedPersistentVolume.map { "pv \($0.name)" } ?? "the selected PV"
        case .storageClass:
            return viewModel.state.selectedStorageClass.map { "storageclass \($0.name)" } ?? "the selected StorageClass"
        case .horizontalPodAutoscaler:
            return viewModel.state.selectedHorizontalPodAutoscaler.map { "hpa \($0.name)" } ?? "the selected HPA"
        case .networkPolicy:
            return viewModel.state.selectedNetworkPolicy.map { "networkpolicy \($0.name)" } ?? "the selected NetworkPolicy"
        case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding:
            return viewModel.state.selectedRBACResource.map { "\($0.kind.kubernetesResourceName) \($0.name)" } ?? "the selected RBAC resource"
        case .event:
            return "the selected event"
        }
    }

    private var genericResourceEmptyState: some View {
        if viewModel.state.isLoading || viewModel.state.isLoadingResourceDetails {
            return AnyView(
                inspectorEmptyState(
                    .loading(
                        title: "Loading \(viewModel.state.selectedWorkloadKind.title)",
                        message: "Loading resources and manifest details for the active namespace."
                    ),
                    symbol: "hourglass"
                )
            )
        }

        let availableCount: Int = {
            switch viewModel.state.selectedWorkloadKind {
            case .statefulSet: return viewModel.visibleStatefulSets.count
            case .daemonSet: return viewModel.visibleDaemonSets.count
            case .job: return viewModel.visibleJobs.count
            case .cronJob: return viewModel.visibleCronJobs.count
            case .replicaSet: return viewModel.visibleReplicaSets.count
            case .horizontalPodAutoscaler: return viewModel.visibleHorizontalPodAutoscalers.count
            case .ingress: return viewModel.visibleIngresses.count
            case .endpoint: return viewModel.visibleEndpoints.count
            case .networkPolicy: return viewModel.visibleNetworkPolicies.count
            case .persistentVolumeClaim: return viewModel.visiblePersistentVolumeClaims.count
            case .persistentVolume: return viewModel.visiblePersistentVolumes.count
            case .storageClass: return viewModel.visibleStorageClasses.count
            case .configMap: return viewModel.visibleConfigMaps.count
            case .secret: return viewModel.visibleSecrets.count
            case .node: return viewModel.visibleNodes.count
            case .serviceAccount, .role, .roleBinding, .clusterRole, .clusterRoleBinding: return viewModel.visibleRBACResources.count
            default: return 0
            }
        }()

        if availableCount == 0 {
            return AnyView(
                inspectorEmptyState(
                    .empty(
                        title: "No \(viewModel.state.selectedWorkloadKind.title.lowercased()) found",
                        message: "Nothing is available in the current namespace yet. YAML and describe will appear here when a resource is selected."
                    ),
                    symbol: "tray"
                )
            )
        }

        return AnyView(
            inspectorEmptyState(
                .unselected(
                    title: "Select a resource",
                    message: "Select an item in the center list to inspect details and actions here."
                ),
                symbol: "list.bullet.rectangle"
            )
        )
    }

    private func openYAMLEditorSheet() {
        yamlManifestIsEditing = false
        isYAMLEditorSheetPresented = true
    }

    private func yamlManifestEditorSheet() -> some View {
        ResourceYAMLEditorSheetView(
            resourceReference: manifestResourceReference,
            yamlText: yamlDraftBinding,
            yamlFooterText: yamlFooterText,
            canApplyMutations: viewModel.canApplyClusterMutations && viewModel.state.selectedOperatorResource == nil,
            hasUnsavedEdits: viewModel.state.resourceYAMLHasUnsavedEdits,
            validationIssues: viewModel.state.resourceYAMLValidationIssues,
            isValidating: viewModel.state.isValidatingResourceYAML,
            canUndoEdit: viewModel.state.canUndoResourceYAMLEdit,
            canReapplySnapshot: viewModel.canReapplyResourceYAMLBaseline,
            onApply: { viewModel.requestApplySelectedResourceYAML() },
            onReapplySnapshot: { viewModel.requestReapplyResourceYAMLBaseline() },
            onUndoEdit: { viewModel.undoResourceYAMLEdit() },
            onRevert: { viewModel.revertResourceYAMLDraft() },
            onImport: { viewModel.importResourceYAMLFromFile() },
            onExport: { viewModel.saveCurrentResourceYAML() },
            onExportToExportFolder: { viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: false) },
            onExportAndOpen: { viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: true) },
            onClose: { isYAMLEditorSheetPresented = false },
            documentState: yamlManifestDocumentState
        )
    }

    private var yamlBlock: some View {
        ResourceYAMLInspectorPane(
            resourceReference: manifestResourceReference,
            yamlText: yamlDraftBinding,
            yamlDisplayText: viewModel.state.resourceYAML,
            yamlFooterText: yamlFooterText,
            baseline: viewModel.state.resourceYAMLBaseline,
            hasUnsavedEdits: viewModel.state.resourceYAMLHasUnsavedEdits,
            canApplyMutations: viewModel.canApplyClusterMutations && viewModel.state.selectedOperatorResource == nil,
            validationIssues: viewModel.state.resourceYAMLValidationIssues,
            isValidating: viewModel.state.isValidatingResourceYAML,
            statusText: manifestStatusText,
            canUndoEdit: viewModel.state.canUndoResourceYAMLEdit,
            canReapplySnapshot: viewModel.canReapplyResourceYAMLBaseline,
            isInlineEditing: $yamlManifestIsEditing,
            inlineEditorImplementation: resolvedManifestInlineEditorImplementation,
            onApply: { viewModel.requestApplySelectedResourceYAML() },
            onReapplySnapshot: { viewModel.requestReapplyResourceYAMLBaseline() },
            onOpenEditor: { openYAMLEditorSheet() },
            onUndoEdit: { viewModel.undoResourceYAMLEdit() },
            onRevert: { viewModel.revertResourceYAMLDraft() },
            onImport: { viewModel.importResourceYAMLFromFile() },
            onExport: { viewModel.saveCurrentResourceYAML() },
            onExportToExportFolder: { viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: false) },
            onExportAndOpen: { viewModel.saveCurrentResourceYAMLToExportFolder(openAfterSave: true) },
            readOnlyResetID: "yaml:\(manifestResourceReference):\(viewModel.state.selectedSection.rawValue):\(viewModel.state.selectedWorkloadKind.kubernetesResourceName)",
            documentState: yamlManifestDocumentState
        )
    }

    /// One pane at a time — avoids `ZStack` + opacity (both branches still participated in layout, causing width drift and editor jumping when switching YAML/Describe).
    @ViewBuilder
    private func manifestInspectorPane(activeTab: GenericResourceManifestTab) -> some View {
        switch activeTab {
        case .overview:
            EmptyView()
        case .yaml:
            yamlBlock
        case .describe:
            describeBlock
        }
    }

    /// Describe tab: read-only describe output; cluster updates use the YAML manifest (same buffer as the YAML tab) and Apply.
    private var describeBlock: some View {
        ResourceDescribeInspectorPane(
            describeText: viewModel.state.resourceDescribe,
            resourceReference: manifestResourceReference,
            canApplyMutations: viewModel.canApplyClusterMutations && viewModel.state.selectedOperatorResource == nil,
            yamlText: viewModel.state.resourceYAML,
            hasUnsavedEdits: viewModel.state.resourceYAMLHasUnsavedEdits,
            validationIssues: viewModel.state.resourceYAMLValidationIssues,
            statusText: manifestStatusText,
            onApply: { viewModel.requestApplySelectedResourceYAML() },
            onOpenYAMLEditor: { openYAMLEditorSheet() },
            onExport: { viewModel.saveCurrentResourceDescribe() },
            onExportToExportFolder: { viewModel.saveCurrentResourceDescribeToExportFolder(openAfterSave: false) },
            onExportAndOpen: { viewModel.saveCurrentResourceDescribeToExportFolder(openAfterSave: true) },
            readOnlyResetID: "describe:\(manifestResourceReference):\(viewModel.state.selectedSection.rawValue):\(viewModel.state.selectedWorkloadKind.kubernetesResourceName)",
            documentState: describeManifestDocumentState
        )
    }

    private func exportableTextPane(text: String, emptyText: String) -> some View {
        InspectorTextSurface(minHeight: 220) {
            InspectorReadOnlyTextView(
                text: text.isEmpty ? emptyText : text,
                resetID: "export:\(emptyText):\((text.isEmpty ? emptyText : text).count)"
            )
        }
    }

    private func execPane(for pod: PodSummary) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            TextField("Command", text: $viewModel.execCommandInput)
                .textFieldStyle(.roundedBorder)

            HStack(spacing: 10) {
                Button("Run in Pod") {
                    viewModel.requestExecInSelectedPod()
                }
                .disabled(viewModel.state.isExecutingCommand || !viewModel.canApplyClusterMutations)

                Button("Open Terminal") {
                    viewModel.startTerminalSession(for: pod)
                }
                .disabled(!viewModel.canApplyClusterMutations)

                if viewModel.state.isExecutingCommand {
                    ProgressView()
                        .controlSize(.small)
                }

                Spacer()

                if let result = viewModel.state.lastExecResult, result.podName == pod.name {
                    Text("Exit code: \(result.exitCode)")
                        .font(.footnote.weight(.semibold))
                        .foregroundStyle(result.exitCode == 0 ? Color.secondary : Color.red)
                }
            }

            if let result = viewModel.state.lastExecResult, result.podName == pod.name {
                VStack(alignment: .leading, spacing: 8) {
                    Text(result.command.joined(separator: " "))
                        .font(.footnote.weight(.semibold))
                        .foregroundStyle(.secondary)

                    InspectorReadOnlyTextSurface(
                        text: execOutputText(for: result),
                        minHeight: 220,
                        resetID: "exec:\(result.podName):\(result.command.joined(separator: " ")):\(result.exitCode)"
                    )
                }
            } else {
                Text("Run a command to see stdout/stderr here.")
                    .foregroundStyle(.secondary)
            }
        }
    }

    private func portForwardPane(targetKind: PortForwardTargetKind, targetName: String) -> some View {
        let matchingNamespace = viewModel.state.selectedNamespace
        let matchingSessions = viewModel.state.portForwardSessions.filter {
            $0.targetKind == targetKind
                && $0.targetName == targetName
                && $0.namespace == matchingNamespace
        }
        let activeSession = matchingSessions.first {
            $0.contextName == viewModel.state.selectedContext?.name && $0.isActiveOrStarting
        }

        return VStack(alignment: .leading, spacing: 12) {
            PortForwardEndpointFields(
                localPort: $viewModel.portForwardLocalPortInput,
                remotePort: $viewModel.portForwardRemotePortInput,
                address: $viewModel.portForwardAddressInput
            )

            PortForwardPrimaryActionLayout {
                PortForwardPrimaryActionButton(
                    activeSession: activeSession,
                    startTitle: "Start Port Forward",
                    isStartDisabled: viewModel.state.isStartingPortForward,
                    onStart: viewModel.startPortForwardForSelection,
                    onStop: viewModel.stopPortForward
                )
            } utilities: {
                HStack(spacing: 8) {
                    if viewModel.state.isStartingPortForward {
                        ProgressView()
                            .controlSize(.small)
                    }

                    Button("Open Terminal") {
                        viewModel.setSection(.terminal)
                    }
                }
            }

            if matchingSessions.isEmpty {
                Text("No port-forward sessions for this \(targetKind.title.lowercased()).")
                    .foregroundStyle(.secondary)
            } else {
                HStack {
                    Text(matchingSessions.contains(where: \.isActiveOrStarting) ? "Port forwards for this \(targetKind.title.lowercased())" : "Recent port forwards for this \(targetKind.title.lowercased())")
                        .font(.subheadline.weight(.semibold))
                    Spacer(minLength: 0)
                    if matchingSessions.contains(where: \.isInactive) {
                        Button("Clear Inactive") {
                            viewModel.clearInactivePortForwardSessions(
                                targetKind: targetKind,
                                targetName: targetName,
                                namespace: matchingNamespace
                            )
                        }
                        .controlSize(.small)
                        .help("Remove stopped and failed port-forward rows")
                    }
                }
                ForEach(matchingSessions) { session in
                    portForwardSessionRow(session)
                }
            }
        }
    }

    private var terminalPane: some View {
        ResourceTerminalWorkspaceView(
            session: viewModel.state.terminalSession,
            sessions: viewModel.state.terminalSessions,
            activeSessionID: viewModel.state.activeTerminalSessionID,
            contextName: viewModel.state.selectedContext?.name,
            namespace: viewModel.state.selectedNamespace,
            selectedPod: viewModel.state.selectedPod,
            availablePods: viewModel.state.pods,
            portForwardSessions: viewModel.state.portForwardSessions,
            canApplyMutations: viewModel.canApplyClusterMutations,
            selectedShellPodID: $terminalShellPodID,
            selectedPortForwardPodID: $terminalPortForwardPodID,
            terminalInput: $viewModel.terminalSessionInput,
            portForwardLocalPort: $viewModel.portForwardLocalPortInput,
            portForwardRemotePort: $viewModel.portForwardRemotePortInput,
            portForwardAddress: $viewModel.portForwardAddressInput,
            onStartSession: { pod, containerName in viewModel.startTerminalSession(for: pod, container: containerName) },
            onReconnectSession: { session, pod, containerName in
                viewModel.startTerminalSession(for: pod, container: containerName, replacingSessionID: session.id)
            },
            onStartPortForward: { pod in
                viewModel.startPortForward(targetKind: .pod, targetName: pod.name)
            },
            onStopPortForward: { session in
                viewModel.stopPortForward(session)
            },
            onOpenPortForwardInBrowser: { session in
                viewModel.openPortForwardInBrowser(session)
            },
            onRetryPortForward: { session in
                viewModel.retryPortForward(session)
            },
            onClearPortForward: { session in
                viewModel.clearPortForwardSession(session)
            },
            onClearInactivePortForwards: {
                viewModel.clearInactivePortForwardSessions()
            },
            onSend: { viewModel.sendTerminalSessionInput() },
            onSendControlSequence: { text in viewModel.sendTerminalControlSequence(text) },
            onResizeSession: { id, columns, rows in viewModel.resizeTerminalSession(id: id, columns: columns, rows: rows) },
            onDisconnect: { viewModel.stopTerminalSession() },
            onSelectSession: { id in viewModel.selectTerminalSession(id: id) },
            onCloseSession: { id in viewModel.closeTerminalSession(id: id) },
            onClearTranscript: { viewModel.clearTerminalSessionTranscript() },
            onSaveActiveTerminalTranscript: { viewModel.saveActiveTerminalTranscript() },
            onSaveAllTerminalTranscripts: { viewModel.saveAllTerminalTranscriptsZip() },
            onSaveActiveTerminalTranscriptToExportFolder: { viewModel.saveActiveTerminalTranscriptToExportFolder(openAfterSave: false) },
            onSaveActiveTerminalTranscriptAndOpen: { viewModel.saveActiveTerminalTranscriptToExportFolder(openAfterSave: true) },
            onSaveAllTerminalTranscriptsToExportFolder: { viewModel.saveAllTerminalTranscriptsZipToExportFolder(openAfterSave: false) },
            onSaveAllTerminalTranscriptsAndOpen: { viewModel.saveAllTerminalTranscriptsZipToExportFolder(openAfterSave: true) },
            isFavoritePod: isFavoritePod,
            onToggleFavoritePod: toggleFavoritePod
        )
        .id("terminal")
        .onAppear(perform: reconcileTerminalPodSelections)
        .onChange(of: viewModel.state.pods.map(\.id)) { _, _ in
            reconcileTerminalPodSelections()
        }
        .onChange(of: viewModel.state.selectedPod?.id) { _, _ in
            reconcileTerminalPodSelections()
        }
    }

    private func reconcileTerminalPodSelections() {
        let availableIDs = Set(viewModel.state.pods.map(\.id))
        let fallbackID = viewModel.state.selectedPod?.id ?? viewModel.state.pods.first?.id ?? ""

        if terminalShellPodID.isEmpty || !availableIDs.contains(terminalShellPodID) {
            terminalShellPodID = fallbackID
        }
        if terminalPortForwardPodID.isEmpty || !availableIDs.contains(terminalPortForwardPodID) {
            terminalPortForwardPodID = fallbackID
        }
        if terminalLogTabState.selectedPodID.isEmpty || !availableIDs.contains(terminalLogTabState.selectedPodID) {
            terminalLogTabState.selectedPodID = fallbackID
        }
        reconcileTerminalLogTabs()
    }

    private var terminalDetails: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(appString(.terminal))
                .font(.title2.weight(.bold))

            RuneSegmentedPickerInScroll(
                "",
                selection: $terminalInspectorTab,
                labelsHidden: true
            ) {
                ForEach(TerminalInspectorTab.allCases) { tab in
                    Text(tab.localizedTitle(appString)).tag(tab)
                }
            }
            .accessibilityLabel("Terminal Inspector")

            Group {
                switch terminalInspectorTab {
                case .commands:
                    ResourceTerminalDetailsView(
                        session: viewModel.state.terminalSession,
                        selectedPod: terminalInspectorPod,
                        portForwardSessions: viewModel.state.portForwardSessions,
                        onFillCommand: { command in
                            viewModel.applySuggestedTerminalCommand(command, sendImmediately: false)
                        }
                    )

                case .logs:
                    terminalPodLogsDetails

                case .yaml:
                    terminalPodYAMLDetails
                }
            }
            .transaction { transaction in
                transaction.animation = nil
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .onAppear(perform: refreshTerminalInspectorSelectionIfNeeded)
        .onChange(of: terminalInspectorTab) { _, _ in
            refreshTerminalInspectorSelectionIfNeeded()
        }
        .onChange(of: terminalShellPodID) { _, _ in
            refreshTerminalInspectorForShellPodChangeIfNeeded()
        }
    }

    private var terminalInspectorPod: PodSummary? {
        viewModel.state.pods.first { $0.id == terminalShellPodID }
            ?? viewModel.state.selectedPod
            ?? viewModel.state.pods.first
    }

    private var terminalLogActivePod: PodSummary? {
        terminalLogTabState.activePod(in: viewModel.state.pods, fallback: terminalInitialLogPod)
    }

    private var terminalInitialLogPod: PodSummary? {
        viewModel.state.pods.first { $0.id == terminalLogTabState.selectedPodID }
            ?? viewModel.state.selectedPod
            ?? viewModel.state.pods.first
    }

    @ViewBuilder
    private var terminalPodLogsDetails: some View {
        if let pod = terminalLogActivePod {
            let podOptions = terminalLogPodOptions()
            VStack(alignment: .leading, spacing: 12) {
                TerminalLogTabBar(
                    tabs: terminalLogTabPresentations,
                    activeTabID: terminalLogTabState.activeTabID,
                    canAddTab: !viewModel.state.pods.isEmpty,
                    onSelectTab: selectTerminalLogTab,
                    onCloseTab: closeTerminalLogTab,
                    onToggleFavoriteTab: toggleFavoriteTerminalLogTab,
                    onAddTab: addTerminalLogTab
                )

                PodLogsInspectorPane(
                    selectedLogPreset: $viewModel.selectedLogPreset,
                    includePreviousLogs: $viewModel.includePreviousLogs,
                    selectedContainer: $viewModel.selectedLogContainer,
                    isTailModeEnabled: $viewModel.isLogTailModeEnabled,
                    isStreamPaused: $viewModel.isLogStreamPaused,
                    isLoadingLogs: viewModel.state.isLoadingLogs,
                    isLoadingResources: viewModel.state.isLoading,
                    errorMessage: viewModel.state.lastLogFetchError,
                    statusText: logStatusText,
                    podOptions: podOptions,
                    selectedPodID: terminalLogPodSelectionBinding(currentPod: pod, podOptions: podOptions),
                    isFavoritePod: isFavoritePod,
                    onToggleFavoritePod: toggleFavoritePod,
                    presentationStyle: .terminalCompact,
                    showsContainerPicker: false,
                    containerOptions: viewModel.podLogContainerOptions,
                    logText: viewModel.state.podLogs,
                    readOnlyResetID: "terminal-podlogs:\(pod.name):\(viewModel.selectedLogPreset.id):\(viewModel.includePreviousLogs):\(viewModel.selectedLogContainer)",
                    onReload: { reloadActiveTerminalLogPod() },
                    onSave: { viewModel.saveCurrentLogs() },
                    onSaveToExportFolder: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: false) },
                    onSaveAndOpen: { viewModel.saveCurrentLogsToExportFolder(openAfterSave: true) },
                    onSaveVisibleZip: { viewModel.saveVisibleLogsZip(visibleText: $0) },
                    onSaveVisibleZipToExportFolder: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: false) },
                    onSaveVisibleZipAndOpen: { viewModel.saveVisibleLogsZipToExportFolder(visibleText: $0, openAfterSave: true) },
                    onSaveFullZip: { viewModel.saveCurrentLogsZip() },
                    onSaveFullZipToExportFolder: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: false) },
                    onSaveFullZipAndOpen: { viewModel.saveCurrentLogsZipToExportFolder(openAfterSave: true) },
                    onSaveAllPodsZip: { viewModel.saveAllPodsLogsZip() },
                    onSaveAllPodsZipToExportFolder: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: false) },
                    onSaveAllPodsZipAndOpen: { viewModel.saveAllPodsLogsZipToExportFolder(openAfterSave: true) },
                    onCopySelection: { copySelectedTextFromFocusedTextView() },
                    onCopyAll: { viewModel.copyCurrentLogsToClipboard() },
                    onToggleStreamPause: { viewModel.toggleLogStreamPause() }
                )
                .onAppear {
                    ensureTerminalLogTabs(for: pod)
                    reloadActiveTerminalLogPod()
                }
            }
        } else {
            VStack(alignment: .leading, spacing: 12) {
                TerminalLogTabBar(
                    tabs: terminalLogTabPresentations,
                    activeTabID: terminalLogTabState.activeTabID,
                    canAddTab: !viewModel.state.pods.isEmpty,
                    onSelectTab: selectTerminalLogTab,
                    onCloseTab: closeTerminalLogTab,
                    onToggleFavoriteTab: toggleFavoriteTerminalLogTab,
                    onAddTab: addTerminalLogTab
                )
                inspectorEmptyState(
                    .unselected(
                        title: "Select a pod for logs",
                        message: "Choose a pod from the center list before opening its logs."
                    ),
                    symbol: "doc.text.magnifyingglass"
                )
            }
        }
    }

    private func terminalLogPodOptions() -> [PodSummary] {
        viewModel.state.pods
            .sorted { lhs, rhs in
                let lhsFavorite = isFavoritePod(lhs)
                let rhsFavorite = isFavoritePod(rhs)
                if lhsFavorite != rhsFavorite {
                    return lhsFavorite && !rhsFavorite
                }
                let namespaceOrder = lhs.namespace.localizedCaseInsensitiveCompare(rhs.namespace)
                if namespaceOrder != .orderedSame {
                    return namespaceOrder == .orderedAscending
                }
                return lhs.name.localizedCaseInsensitiveCompare(rhs.name) == .orderedAscending
            }
    }

    private func terminalLogPodSelectionBinding(currentPod: PodSummary, podOptions: [PodSummary]) -> Binding<String> {
        Binding(
            get: {
                let availableIDs = Set(podOptions.map(\.id))
                if availableIDs.contains(terminalLogTabState.selectedPodID) {
                    return terminalLogTabState.selectedPodID
                }
                return currentPod.id
            },
            set: { podID in
                guard let pod = podOptions.first(where: { $0.id == podID }) else { return }
                terminalLogTabState.selectedPodID = pod.id
                updateActiveTerminalLogTab(to: pod)
                viewModel.focusTerminalPodInspector(pod, reloadLogs: true)
            }
        )
    }

    private var terminalLogTabPresentations: [TerminalLogTabPresentation] {
        terminalLogTabState.presentations(pods: viewModel.state.pods, isFavorite: isFavoritePod)
    }

    private func ensureTerminalLogTabs(for pod: PodSummary) {
        terminalLogTabState.ensureTab(for: pod)
    }

    private func reconcileTerminalLogTabs() {
        terminalLogTabState.reconcile(availablePods: viewModel.state.pods, fallbackPod: terminalInitialLogPod)
    }

    private func addTerminalLogTab() {
        guard let pod = preferredPodForNewTerminalLogTab() else { return }
        terminalLogTabState.add(preferredPod: pod)
        viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
    }

    private func selectTerminalLogTab(_ id: String) {
        guard let pod = terminalLogTabState.select(id: id, availablePods: viewModel.state.pods) else { return }
        viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
    }

    private func closeTerminalLogTab(_ id: String) {
        if let pod = terminalLogTabState.close(id: id, availablePods: viewModel.state.pods, fallbackPod: terminalInitialLogPod) {
            viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
        }
    }

    private func toggleFavoriteTerminalLogTab(_ id: String) {
        guard let tab = terminalLogTabState.tabs.first(where: { $0.id == id }) else { return }
        if let pod = viewModel.state.pods.first(where: { $0.id == tab.podID }) {
            toggleFavoritePod(pod)
        } else {
            viewModel.toggleFavoriteResource(kind: .pod, namespace: tab.namespace, name: tab.podName)
        }
    }

    private func updateActiveTerminalLogTab(to pod: PodSummary) {
        terminalLogTabState.updateActive(to: pod)
    }

    private func reloadActiveTerminalLogPod() {
        guard let pod = terminalLogActivePod else { return }
        viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
    }

    private func preferredPodForNewTerminalLogTab() -> PodSummary? {
        terminalLogTabState.preferredPodForNewTab(
            pods: viewModel.state.pods,
            fallbackPod: terminalInitialLogPod,
            isFavorite: isFavoritePod
        )
    }

    private func isFavoritePod(_ pod: PodSummary) -> Bool {
        viewModel.isFavoriteResource(kind: .pod, namespace: pod.namespace, name: pod.name)
    }

    private func toggleFavoritePod(_ pod: PodSummary) {
        viewModel.toggleFavoriteResource(kind: .pod, namespace: pod.namespace, name: pod.name)
    }

    @ViewBuilder
    private var terminalPodYAMLDetails: some View {
        if let pod = terminalInspectorPod {
            manifestInspectorPane(activeTab: .yaml)
                .onAppear {
                    viewModel.focusTerminalPodInspector(pod, loadDetails: shouldReloadTerminalPodDetails(for: pod))
                }
        } else {
            inspectorEmptyState(
                .unselected(
                    title: "Select a pod for YAML",
                    message: "Choose a pod from the center list before opening its manifest."
                ),
                symbol: "curlybraces"
            )
        }
    }

    private func refreshTerminalInspectorSelectionIfNeeded() {
        switch terminalInspectorTab {
        case .commands:
            break
        case .logs:
            guard let pod = terminalLogActivePod else { return }
            ensureTerminalLogTabs(for: pod)
            viewModel.focusTerminalPodInspector(pod, reloadLogs: shouldReloadTerminalPodLogs(for: pod))
        case .yaml:
            guard let pod = terminalInspectorPod else { return }
            viewModel.focusTerminalPodInspector(pod, loadDetails: shouldReloadTerminalPodDetails(for: pod))
        }
    }

    private func refreshTerminalInspectorForShellPodChangeIfNeeded() {
        guard terminalInspectorTab == .yaml else { return }
        refreshTerminalInspectorSelectionIfNeeded()
    }

    private func shouldReloadTerminalPodLogs(for pod: PodSummary) -> Bool {
        viewModel.state.selectedPod?.id != pod.id || viewModel.state.podLogs.isEmpty
    }

    private func shouldReloadTerminalPodDetails(for pod: PodSummary) -> Bool {
        viewModel.state.selectedPod?.id != pod.id || viewModel.state.resourceYAML.isEmpty
    }

    private func portForwardSessionRow(_ session: PortForwardSession) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                Text("\(session.resourceLabel)  \(session.localPort):\(session.remotePort)")
                    .font(.subheadline.weight(.semibold))
                Spacer()
                Text(session.status.rawValue.capitalized)
                    .font(.caption.weight(.semibold))
                    .padding(.horizontal, 8)
                    .padding(.vertical, 3)
                    .background(portForwardStatusColor(session.status).opacity(0.16), in: Capsule())
                    .foregroundStyle(portForwardStatusColor(session.status))
            }

            Text("\(session.contextName) • \(session.namespace) • \(session.address)")
                .font(.caption)
                .foregroundStyle(.secondary)

            if !session.lastMessage.isEmpty {
                Text(session.lastMessage)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
            }

            if (session.status == .active && session.browserURL != nil) || session.isInactive {
                HStack(spacing: 8) {
                    if session.status == .active, session.browserURL != nil {
                        Button {
                            viewModel.openPortForwardInBrowser(session)
                        } label: {
                            Label("Open in Browser", systemImage: "safari")
                        }
                        .help(session.browserURL.map { "Open \($0.absoluteString)" } ?? "Open local port-forward URL")
                    }

                    if session.status == .failed {
                        Button {
                            viewModel.retryPortForward(session)
                        } label: {
                            Label("Retry", systemImage: "arrow.clockwise")
                        }
                        .help("Try this port-forward again")
                    }

                    if session.isInactive {
                        Button {
                            viewModel.clearPortForwardSession(session)
                        } label: {
                            Label("Clear", systemImage: "xmark.circle")
                        }
                        .help("Remove this inactive port-forward row")
                    }
                }
            }
        }
        .padding(10)
        .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
    }

    private func genericResourceList(
        _ resources: [ClusterResourceSummary],
        selection: ClusterResourceSummary?,
        action: @escaping (ClusterResourceSummary?) -> Void
    ) -> some View {
        resourceListGate(
            kindTitle: viewModel.state.selectedWorkloadKind.title,
            visibleCount: resources.count
        ) {
            VStack(alignment: .leading, spacing: 8) {
                genericResourceBulkSelectionControls

                AppKitGenericResourceListView(
                    resources: resources,
                    selectedResourceID: selection?.id,
                    selectedResourceIDs: viewModel.state.selectedGenericResourceIDs,
                    sortColumn: viewModel.genericResourceSortColumn,
                    sortAscending: viewModel.genericResourceSortAscending,
                    canApplyClusterMutations: viewModel.canApplyClusterMutations,
                    isFavorite: { resource in
                        viewModel.isFavoriteResource(kind: resource.kind, namespace: resource.namespace, name: resource.name)
                    },
                    onSelectResource: { resource in
                        action(resource)
                    },
                    onToggleBulkSelection: viewModel.toggleGenericResourceBulkSelection,
                    onToggleSort: viewModel.toggleGenericResourceSort,
                    onToggleFavorite: { resource in
                        viewModel.toggleFavoriteResource(kind: resource.kind, namespace: resource.namespace, name: resource.name)
                    },
                    onOpenDescribe: { resource in
                        action(resource)
                        genericResourceManifestTab = .describe
                    },
                    onOpenYAML: { resource in
                        action(resource)
                        genericResourceManifestTab = .yaml
                    },
                    onDelete: { resource in
                        viewModel.requestDeleteResource(kind: resource.kind, name: resource.name)
                    }
                )
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .padding(.vertical, 2)
        }
        .padding(.horizontal, 2)
        .id("\(viewModel.state.selectedSection.rawValue):\(viewModel.state.selectedWorkloadKind.kubernetesResourceName):\(genericResourceListIdentity(resources))")
        .transaction { transaction in
            transaction.animation = nil
        }
    }

    @ViewBuilder
    private func resourceListGate<Content: View>(
        kindTitle: String,
        visibleCount: Int,
        scopeDescription: String? = nil,
        @ViewBuilder content: () -> Content
    ) -> some View {
        let presentation = ResourceListPresentation.project(
            isLoading: viewModel.state.isLoading,
            visibleCount: visibleCount,
            kindTitle: kindTitle,
            filterQuery: viewModel.state.resourceSearchQuery,
            scopeDescription: scopeDescription ?? resourceListScopeDescription,
            freshness: currentResourceListFreshness
        )

        switch presentation {
        case .content:
            content()
        case let .state(state, action):
            RunePaneContentStateView(
                state,
                style: .card,
                action: action.map { action in
                    switch action {
                    case .clearFilter:
                        return RuneContentStateAction("Clear Filter", systemImage: "xmark.circle") {
                            viewModel.setResourceSearchQuery("")
                        }
                    case .retry:
                        return RuneContentStateAction("Retry", systemImage: "arrow.clockwise") {
                            viewModel.refreshCurrentView(debounced: false)
                        }
                    }
                }
            )
        }
    }

    private var resourceListScopeDescription: String {
        let namespace = viewModel.state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        return namespace.isEmpty ? "the current namespace scope" : "namespace \(namespace)"
    }

    @ViewBuilder
    private var keyboardNavigationBridge: some View {
        VStack(spacing: 0) {
            Button("") {
                focusNextKeyboardPane()
            }
            .keyboardShortcut(.tab, modifiers: [])

            Button("") {
                focusPreviousKeyboardPane()
            }
            .keyboardShortcut(.tab, modifiers: [.shift])

            Button("") {
                moveKeyboardSelection(.up)
            }
            .keyboardShortcut(.upArrow, modifiers: [])

            Button("") {
                moveKeyboardSelection(.down)
            }
            .keyboardShortcut(.downArrow, modifiers: [])

            Button("") {
                moveKeyboardSelection(.left)
            }
            .keyboardShortcut(.leftArrow, modifiers: [])

            Button("") {
                moveKeyboardSelection(.right)
            }
            .keyboardShortcut(.rightArrow, modifiers: [])

            Button("") {
                activateKeyboardSelection()
            }
            .keyboardShortcut(.return, modifiers: [])

            // k9s-style alternates for keyboard-only navigation.
            Button("") {
                focusPreviousKeyboardPane()
            }
            .keyboardShortcut("h", modifiers: [.control])

            Button("") {
                moveKeyboardSelection(.down)
            }
            .keyboardShortcut("j", modifiers: [.control])

            Button("") {
                moveKeyboardSelection(.up)
            }
            .keyboardShortcut("k", modifiers: [.control])

            Button("") {
                focusNextKeyboardPane()
            }
            .keyboardShortcut("l", modifiers: [.control])
        }
        .frame(width: 0, height: 0)
        .opacity(0)
        .accessibilityHidden(true)
    }

    @ViewBuilder
    private func paneFocusOutline(isFocused: Bool) -> some View {
        if isFocused {
            Rectangle()
                .stroke(Color.accentColor.opacity(0.45), lineWidth: 1)
                .padding(2)
                .allowsHitTesting(false)
        }
    }

    private var keyboardNavigationContext: RuneRootKeyboardNavigationContext {
        let keyWindow = NSApp.keyWindow
        let editableTextResponderActive = (keyWindow?.firstResponder as? NSTextView)?.isEditable == true
        return RuneRootKeyboardNavigationContext(
            commandPalettePresented: viewModel.state.isCommandPalettePresented,
            yamlEditorSheetPresented: isYAMLEditorSheetPresented,
            yamlManifestIsEditing: yamlManifestIsEditing,
            kubeConfigImportReviewPresented: viewModel.isKubeConfigImportConfirmationPending,
            addClusterPopoverPresented: addClusterPopoverPresented,
            addClusterProviderPresented: selectedAddClusterProvider != nil,
            manualNamespaceSheetPresented: isManualNamespaceSheetPresented,
            pendingWriteConfirmationPresented: viewModel.pendingWriteAction != nil,
            launchExperiencePresented: shouldShowLaunchExperience,
            appSheetAttached: keyWindow?.sheetParent != nil || keyWindow?.attachedSheet != nil,
            editableTextResponderActive: editableTextResponderActive
        )
    }

    private var keyboardNavigationSuspended: Bool {
        keyboardNavigationContext.isSuspended
    }

    private func focusNextKeyboardPane() {
        let navigationContext = keyboardNavigationContext
        guard !navigationContext.hasBlockingPresentation else { return }
        if textInputFocus != nil {
            textInputFocus = nil
            keyboardPaneFocus = advancedKeyboardPane(from: keyboardPaneFocus, forward: true)
            NSApp.keyWindow?.makeFirstResponder(nil)
            return
        }
        guard !navigationContext.isSuspended else { return }
        keyboardPaneFocus = advancedKeyboardPane(from: keyboardPaneFocus, forward: true)
        NSApp.keyWindow?.makeFirstResponder(nil)
    }

    private func focusPreviousKeyboardPane() {
        let navigationContext = keyboardNavigationContext
        guard !navigationContext.hasBlockingPresentation else { return }
        if textInputFocus != nil {
            textInputFocus = nil
            keyboardPaneFocus = advancedKeyboardPane(from: keyboardPaneFocus, forward: false)
            NSApp.keyWindow?.makeFirstResponder(nil)
            return
        }
        guard !navigationContext.isSuspended else { return }
        keyboardPaneFocus = advancedKeyboardPane(from: keyboardPaneFocus, forward: false)
        NSApp.keyWindow?.makeFirstResponder(nil)
    }

    private func moveKeyboardSelection(_ direction: MoveCommandDirection) {
        guard !keyboardNavigationSuspended else { return }
        switch keyboardPaneFocus {
        case .sidebarSections:
            moveSectionSelection(direction)
        case .sidebarContexts:
            moveContextSelection(direction)
        case .content:
            moveContentSelection(direction)
        case .detail:
            moveDetailSelection(direction)
        }
    }

    private func activateKeyboardSelection() {
        guard !keyboardNavigationSuspended else { return }
        switch keyboardPaneFocus {
        case .sidebarSections, .sidebarContexts:
            keyboardPaneFocus = .content
        case .content:
            if viewModel.state.selectedSection == .overview {
                openSelectedOverviewCard()
            } else if viewModel.isDetailPaneVisible {
                keyboardPaneFocus = .detail
            }
        case .detail:
            break
        }
    }

    private var availableKeyboardPanes: [RuneRootKeyboardPane] {
        RuneRootKeyboardPaneNavigation.availablePanes(
            sidebarVisible: viewModel.isSidebarVisible,
            detailVisible: viewModel.isDetailPaneVisible,
            skipsClusterPane: skipClusterOnTabNavigationFromSections
        )
    }

    private func advancedKeyboardPane(from current: RuneRootKeyboardPane, forward: Bool) -> RuneRootKeyboardPane {
        RuneRootKeyboardPaneNavigation.advanced(
            from: current,
            forward: forward,
            in: availableKeyboardPanes
        )
    }

    private func moveSectionSelection(_ direction: MoveCommandDirection) {
        let sections = RuneSection.allCases
        guard let currentIndex = sections.firstIndex(of: viewModel.state.selectedSection),
              let nextIndex = steppedIndex(count: sections.count, current: currentIndex, direction: direction) else {
            return
        }
        viewModel.setSection(sections[nextIndex])
    }

    private func moveContextSelection(_ direction: MoveCommandDirection) {
        let contexts = viewModel.visibleContexts
        guard !contexts.isEmpty else { return }
        let currentID = viewModel.state.selectedContext?.id
        guard let next = steppedItem(items: contexts, currentID: currentID, direction: direction) else { return }
        viewModel.setContext(next)
    }

    private func moveContentSelection(_ direction: MoveCommandDirection) {
        if moveContentKindIfNeeded(direction) {
            return
        }

        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                if let next = steppedItem(items: viewModel.visiblePods, currentID: viewModel.state.selectedPod?.id, direction: direction) {
                    viewModel.selectPod(next)
                }
            case .deployment:
                if let next = steppedItem(items: viewModel.visibleDeployments, currentID: viewModel.state.selectedDeployment?.id, direction: direction) {
                    viewModel.selectDeployment(next)
                }
            case .statefulSet:
                if let next = steppedItem(items: viewModel.visibleStatefulSets, currentID: viewModel.state.selectedStatefulSet?.id, direction: direction) {
                    viewModel.selectStatefulSet(next)
                }
            case .daemonSet:
                if let next = steppedItem(items: viewModel.visibleDaemonSets, currentID: viewModel.state.selectedDaemonSet?.id, direction: direction) {
                    viewModel.selectDaemonSet(next)
                }
            case .job:
                if let next = steppedItem(items: viewModel.visibleJobs, currentID: viewModel.state.selectedJob?.id, direction: direction) {
                    viewModel.selectJob(next)
                }
            case .cronJob:
                if let next = steppedItem(items: viewModel.visibleCronJobs, currentID: viewModel.state.selectedCronJob?.id, direction: direction) {
                    viewModel.selectCronJob(next)
                }
            case .replicaSet:
                if let next = steppedItem(items: viewModel.visibleReplicaSets, currentID: viewModel.state.selectedReplicaSet?.id, direction: direction) {
                    viewModel.selectReplicaSet(next)
                }
            case .horizontalPodAutoscaler:
                if let next = steppedItem(items: viewModel.visibleHorizontalPodAutoscalers, currentID: viewModel.state.selectedHorizontalPodAutoscaler?.id, direction: direction) {
                    viewModel.selectHorizontalPodAutoscaler(next)
                }
            default:
                break
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                if let next = steppedItem(items: viewModel.visibleServices, currentID: viewModel.state.selectedService?.id, direction: direction) {
                    viewModel.selectService(next)
                }
            case .ingress:
                if let next = steppedItem(items: viewModel.visibleIngresses, currentID: viewModel.state.selectedIngress?.id, direction: direction) {
                    viewModel.selectIngress(next)
                }
            case .endpoint:
                if let next = steppedItem(items: viewModel.visibleEndpoints, currentID: viewModel.state.selectedEndpoint?.id, direction: direction) {
                    viewModel.selectEndpoint(next)
                }
            case .networkPolicy:
                if let next = steppedItem(items: viewModel.visibleNetworkPolicies, currentID: viewModel.state.selectedNetworkPolicy?.id, direction: direction) {
                    viewModel.selectNetworkPolicy(next)
                }
            default:
                break
            }
        case .config:
            switch viewModel.state.selectedWorkloadKind {
            case .configMap:
                if let next = steppedItem(items: viewModel.visibleConfigMaps, currentID: viewModel.state.selectedConfigMap?.id, direction: direction) {
                    viewModel.selectConfigMap(next)
                }
            case .secret:
                if let next = steppedItem(items: viewModel.visibleSecrets, currentID: viewModel.state.selectedSecret?.id, direction: direction) {
                    viewModel.selectSecret(next)
                }
            default:
                break
            }
        case .storage:
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolumeClaim:
                if let next = steppedItem(items: viewModel.visiblePersistentVolumeClaims, currentID: viewModel.state.selectedPersistentVolumeClaim?.id, direction: direction) {
                    viewModel.selectPersistentVolumeClaim(next)
                }
            case .persistentVolume:
                if let next = steppedItem(items: viewModel.visiblePersistentVolumes, currentID: viewModel.state.selectedPersistentVolume?.id, direction: direction) {
                    viewModel.selectPersistentVolume(next)
                }
            case .storageClass:
                if let next = steppedItem(items: viewModel.visibleStorageClasses, currentID: viewModel.state.selectedStorageClass?.id, direction: direction) {
                    viewModel.selectStorageClass(next)
                }
            case .node:
                if let next = steppedItem(items: viewModel.visibleNodes, currentID: viewModel.state.selectedNode?.id, direction: direction) {
                    viewModel.selectNode(next)
                }
            default:
                break
            }
        case .rbac:
            if let next = steppedItem(items: viewModel.visibleRBACResources, currentID: viewModel.state.selectedRBACResource?.id, direction: direction) {
                viewModel.selectRBACResource(next)
            }
        case .events:
            if let next = steppedItem(items: viewModel.visibleEvents, currentID: viewModel.state.selectedEvent?.id, direction: direction) {
                viewModel.selectEvent(next)
            }
        case .helm:
            if let next = steppedItem(items: viewModel.visibleHelmReleases, currentID: viewModel.state.selectedHelmRelease?.id, direction: direction) {
                viewModel.selectHelmRelease(next)
            }
        case .overview:
            moveOverviewCardSelection(direction)
        case .terminal:
            break
        }
    }

    private func moveOverviewCardSelection(_ direction: MoveCommandDirection) {
        guard !overviewCardModules.isEmpty else { return }
        let current = min(max(overviewCardSelectionIndex, 0), overviewCardModules.count - 1)
        guard let next = steppedIndex(count: overviewCardModules.count, current: current, direction: direction) else {
            return
        }
        overviewCardSelectionIndex = next
    }

    private func openSelectedOverviewCard() {
        guard !overviewCardModules.isEmpty else { return }
        let index = min(max(overviewCardSelectionIndex, 0), overviewCardModules.count - 1)
        overviewCardSelectionIndex = index
        viewModel.openOverviewModule(overviewCardModules[index])
    }

    /// In the middle/content pane, use left/right to switch between kind tabs
    /// (for sections that expose segmented kinds), and leave up/down for row stepping.
    private func moveContentKindIfNeeded(_ direction: MoveCommandDirection) -> Bool {
        guard direction == .left || direction == .right else { return false }
        if viewModel.state.selectedSection == .helm {
            helmBrowserTab = advancedTab(current: helmBrowserTab, direction: direction)
            return true
        }

        guard let kinds = contentKindsForSelectedSection(), !kinds.isEmpty else { return false }
        guard let currentIndex = kinds.firstIndex(of: viewModel.state.selectedWorkloadKind) else { return false }

        let nextIndex: Int
        switch direction {
        case .right:
            nextIndex = (currentIndex + 1) % kinds.count
        case .left:
            nextIndex = (currentIndex + kinds.count - 1) % kinds.count
        default:
            return false
        }

        viewModel.setWorkloadKind(kinds[nextIndex])
        return true
    }

    private func contentKindsForSelectedSection() -> [KubeResourceKind]? {
        switch viewModel.state.selectedSection {
        case .workloads:
            return viewModel.workloadKinds
        case .networking:
            return viewModel.networkingKinds
        case .config:
            return viewModel.configKinds
        case .storage:
            return viewModel.storageKinds
        case .rbac:
            return viewModel.rbacKinds
        case .overview, .events, .helm, .terminal:
            return nil
        }
    }

    private func installLocalKeyboardMonitorIfNeeded() {
        guard localKeyEventMonitor == nil else { return }
        localKeyEventMonitor = NSEvent.addLocalMonitorForEvents(matching: [.keyDown]) { event in
            if let handledEvent = handleLocalKeyEvent(event) {
                return handledEvent
            }
            return nil
        }
    }

    private func handleLocalKeyEvent(_ event: NSEvent) -> NSEvent? {
        guard RuneRootKeyboardWindowScope.owns(
            eventWindowNumber: event.windowNumber,
            workspaceWindowNumber: workspaceWindowReference.windowNumber,
            keyWindowNumber: NSApp.keyWindow?.windowNumber
        ) else { return event }

        if shouldHandleTabNavigation(event) {
            if event.modifierFlags.contains(.shift) {
                focusPreviousKeyboardPane()
            } else {
                focusNextKeyboardPane()
            }
            return nil
        }

        if shouldHandlePaneArrowNavigation(event) {
            if event.keyCode == 123 {
                moveKeyboardSelection(.left)
            } else {
                moveKeyboardSelection(.right)
            }
            return nil
        }

        guard shouldHandleConfiguredActionKey(event) else { return event }
        guard let action = configuredAction(for: event) else { return event }
        return performConfiguredAction(action) ? nil : event
    }

    private func shouldHandleTabNavigation(_ event: NSEvent) -> Bool {
        guard event.keyCode == 48 else { return false }
        guard RuneRootKeyboardWindowScope.owns(
            eventWindowNumber: event.windowNumber,
            workspaceWindowNumber: workspaceWindowReference.windowNumber,
            keyWindowNumber: NSApp.keyWindow?.windowNumber
        ) else { return false }
        let navigationContext = keyboardNavigationContext
        guard !navigationContext.hasBlockingPresentation else { return false }
        let disallowedModifiers: NSEvent.ModifierFlags = [.command, .option, .control, .function]
        guard event.modifierFlags.isDisjoint(with: disallowedModifiers) else { return false }
        return textInputFocus != nil || !navigationContext.editableTextResponderActive
    }

    private func shouldHandleConfiguredActionKey(_ event: NSEvent) -> Bool {
        guard !keyboardNavigationSuspended else { return false }
        guard textInputFocus == nil else { return false }
        let disallowedModifiers: NSEvent.ModifierFlags = [.function]
        return event.modifierFlags.isDisjoint(with: disallowedModifiers)
    }

    private func shouldHandlePaneArrowNavigation(_ event: NSEvent) -> Bool {
        guard event.keyCode == 123 || event.keyCode == 124 else { return false }
        guard keyboardPaneFocus == .detail || (keyboardPaneFocus == .content && viewModel.state.selectedSection != .terminal) else {
            return false
        }
        guard !keyboardNavigationSuspended else { return false }
        guard textInputFocus == nil else { return false }
        let disallowedModifiers: NSEvent.ModifierFlags = [.command, .option, .control, .shift, .function]
        return event.modifierFlags.isDisjoint(with: disallowedModifiers)
    }

    private func configuredAction(for event: NSEvent) -> RuneKeyBindingAction? {
        guard let baseKey = configuredActionBaseKey(for: event) else { return nil }

        let resolver = RuneKeyBindingResolver { action in
            UserDefaults.standard.runeKeyBindingShortcut(for: action)
        }
        let modifiers = runeKeyBindingModifiers(for: event)
        if let action = resolver.action(
            for: RuneKeyBindingInput(
                baseKey: baseKey,
                modifiers: modifiers,
                isTextInputFocused: textInputFocus != nil,
                isNavigationSuspended: keyboardNavigationSuspended
            )
        ) {
            return action
        }

        guard modifiers.contains(.shift),
              let shiftedSymbol = configuredActionShiftedSymbolKey(for: event) else {
            return nil
        }
        var symbolModifiers = modifiers
        symbolModifiers.remove(.shift)
        return resolver.action(
            for: RuneKeyBindingInput(
                baseKey: shiftedSymbol,
                modifiers: symbolModifiers,
                isTextInputFocused: textInputFocus != nil,
                isNavigationSuspended: keyboardNavigationSuspended
            )
        )
    }

    private func runeKeyBindingModifiers(for event: NSEvent) -> Set<RuneKeyBindingModifier> {
        var modifiers: Set<RuneKeyBindingModifier> = []
        if event.modifierFlags.contains(.command) {
            modifiers.insert(.command)
        }
        if event.modifierFlags.contains(.option) {
            modifiers.insert(.option)
        }
        if event.modifierFlags.contains(.control) {
            modifiers.insert(.control)
        }
        if event.modifierFlags.contains(.shift) {
            modifiers.insert(.shift)
        }
        if event.modifierFlags.contains(.function) {
            modifiers.insert(.function)
        }
        return modifiers
    }

    private func configuredActionBaseKey(for event: NSEvent) -> String? {
        switch event.keyCode {
        case 123:
            return "left"
        case 124:
            return "right"
        default:
            guard let baseKey = event.charactersIgnoringModifiers?.lowercased(), baseKey.count == 1 else {
                return nil
            }
            return baseKey
        }
    }

    private func configuredActionShiftedSymbolKey(for event: NSEvent) -> String? {
        guard let key = event.characters?.lowercased(), key.count == 1 else { return nil }
        guard ["[", "]", "/", ":", "?"].contains(key) else { return nil }
        return key
    }

    private func removeLocalKeyboardMonitor() {
        guard let localKeyEventMonitor else { return }
        NSEvent.removeMonitor(localKeyEventMonitor)
        self.localKeyEventMonitor = nil
    }

    private func moveDetailSelection(_ direction: MoveCommandDirection) {
        switch direction {
        case .left, .right:
            moveDetailInspectorTab(direction)
        case .up, .down:
            break
        @unknown default:
            break
        }
    }

    private func performConfiguredAction(_ action: RuneKeyBindingAction) -> Bool {
        switch action {
        case .commandPalette:
            viewModel.presentCommandPalette()
            return true
        case .filterResources:
            return focusResourceFilterFromKeyBinding()
        case .historyBack:
            guard viewModel.canNavigateBack else { return false }
            viewModel.navigateBack()
            return true
        case .historyForward:
            guard viewModel.canNavigateForward else { return false }
            viewModel.navigateForward()
            return true
        case .focusPreviousPane:
            focusPreviousKeyboardPane()
            return true
        case .focusNextPane:
            focusNextKeyboardPane()
            return true
        case .describe:
            return openDescribeInspectorForSelection()
        case .logs:
            return openLogsInspectorForSelection()
        case .saveLogs:
            return saveCurrentDetailFromKeyBinding()
        case .shell:
            return openShellOrScaleInspectorForSelection()
        case .edit:
            return openYAMLEditorForSelection()
        case .yaml:
            return openYAMLInspectorForSelection()
        case .delete:
            return deleteSelectionFromKeyBinding()
        case .portForward:
            return openPortForwardInspectorForSelection()
        case .rollout:
            return openRolloutInspectorForSelection()
        }
    }

    private func focusResourceFilterFromKeyBinding() -> Bool {
        guard showsResourceFilterControls else { return false }
        textInputFocus = .resourceFilter
        keyboardPaneFocus = .content
        return true
    }

    private func openDescribeInspectorForSelection() -> Bool {
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                guard viewModel.state.selectedPod != nil else { return false }
                podInspectorTab = .describe
            case .deployment:
                guard viewModel.state.selectedDeployment != nil else { return false }
                deploymentInspectorTab = .describe
            default:
                guard hasGenericManifestSelection else { return false }
                genericResourceManifestTab = .describe
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                guard viewModel.state.selectedService != nil else { return false }
                serviceInspectorTab = .describe
            default:
                guard hasGenericManifestSelection else { return false }
                genericResourceManifestTab = .describe
            }
        case .config, .storage, .rbac:
            guard hasGenericManifestSelection else { return false }
            genericResourceManifestTab = .describe
        case .helm:
            guard selectedHelmInspectorMode == .operatorResource else { return false }
            genericResourceManifestTab = .describe
        case .overview, .events, .terminal:
            return false
        }

        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    private func openYAMLInspectorForSelection() -> Bool {
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                guard viewModel.state.selectedPod != nil else { return false }
                podInspectorTab = .yaml
            case .deployment:
                guard viewModel.state.selectedDeployment != nil else { return false }
                deploymentInspectorTab = .yaml
            default:
                guard hasGenericManifestSelection else { return false }
                genericResourceManifestTab = .yaml
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                guard viewModel.state.selectedService != nil else { return false }
                serviceInspectorTab = .yaml
            default:
                guard hasGenericManifestSelection else { return false }
                genericResourceManifestTab = .yaml
            }
        case .config, .storage, .rbac:
            guard hasGenericManifestSelection else { return false }
            genericResourceManifestTab = .yaml
        case .helm:
            guard selectedHelmInspectorMode == .operatorResource else { return false }
            genericResourceManifestTab = .yaml
        case .overview, .events, .terminal:
            return false
        }

        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    private func openYAMLEditorForSelection() -> Bool {
        guard openYAMLInspectorForSelection() else { return false }
        openYAMLEditorSheet()
        return true
    }

    private func deleteSelectionFromKeyBinding() -> Bool {
        guard hasDeletableSelection else { return false }
        viewModel.requestDeleteSelectedResource()
        return true
    }

    private func openLogsInspectorForSelection() -> Bool {
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                guard viewModel.state.selectedPod != nil else { return false }
                podInspectorTab = .logs
            case .deployment:
                guard viewModel.state.selectedDeployment != nil else { return false }
                deploymentInspectorTab = .unifiedLogs
            default:
                return false
            }
        case .networking:
            guard viewModel.state.selectedWorkloadKind == .service, viewModel.state.selectedService != nil else { return false }
            serviceInspectorTab = .unifiedLogs
        case .overview, .config, .storage, .rbac, .events, .helm, .terminal:
            return false
        }

        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    private func saveCurrentDetailFromKeyBinding() -> Bool {
        guard keyboardPaneFocus == .content || keyboardPaneFocus == .detail else { return false }

        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                guard viewModel.state.selectedPod != nil else { return false }
                switch podInspectorTab {
                case .logs:
                    viewModel.saveCurrentLogs()
                case .describe:
                    viewModel.saveCurrentResourceDescribe()
                case .yaml:
                    viewModel.saveCurrentResourceYAML()
                case .overview, .exec, .portForward:
                    return false
                }
            case .deployment:
                guard viewModel.state.selectedDeployment != nil else { return false }
                switch deploymentInspectorTab {
                case .unifiedLogs:
                    viewModel.saveCurrentLogs()
                case .rollout:
                    viewModel.saveCurrentRolloutHistory()
                case .describe:
                    viewModel.saveCurrentResourceDescribe()
                case .yaml:
                    viewModel.saveCurrentResourceYAML()
                case .overview:
                    return false
                }
            default:
                guard hasGenericManifestSelection else { return false }
                switch genericResourceManifestTab {
                case .describe:
                    viewModel.saveCurrentResourceDescribe()
                case .yaml:
                    viewModel.saveCurrentResourceYAML()
                case .overview:
                    return false
                }
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                guard viewModel.state.selectedService != nil else { return false }
                switch serviceInspectorTab {
                case .unifiedLogs:
                    viewModel.saveCurrentLogs()
                case .describe:
                    viewModel.saveCurrentResourceDescribe()
                case .yaml:
                    viewModel.saveCurrentResourceYAML()
                case .overview, .portForward:
                    return false
                }
            default:
                guard hasGenericManifestSelection else { return false }
                switch genericResourceManifestTab {
                case .describe:
                    viewModel.saveCurrentResourceDescribe()
                case .yaml:
                    viewModel.saveCurrentResourceYAML()
                case .overview:
                    return false
                }
            }
        case .config, .storage, .rbac:
            guard hasGenericManifestSelection else { return false }
            switch genericResourceManifestTab {
            case .describe:
                viewModel.saveCurrentResourceDescribe()
            case .yaml:
                viewModel.saveCurrentResourceYAML()
            case .overview:
                return false
            }
        case .helm:
            switch selectedHelmInspectorMode {
            case .operatorResource:
                switch genericResourceManifestTab {
                case .describe:
                    viewModel.saveCurrentResourceDescribe()
                case .yaml:
                    viewModel.saveCurrentResourceYAML()
                case .overview:
                    return false
                }
            case .release:
                switch helmInspectorTab {
                case .values:
                    viewModel.saveCurrentHelmValues()
                case .manifest:
                    viewModel.saveCurrentHelmManifest()
                case .history:
                    viewModel.saveCurrentHelmHistory()
                case .overview:
                    return false
                }
            case .none:
                return false
            }
        case .overview, .events, .terminal:
            return false
        }
        return true
    }

    private func openShellOrScaleInspectorForSelection() -> Bool {
        guard viewModel.state.selectedSection == .workloads else {
            return false
        }

        switch viewModel.state.selectedWorkloadKind {
        case .pod:
            guard viewModel.state.selectedPod != nil else { return false }
            podInspectorTab = .exec
        case .deployment:
            guard viewModel.state.selectedDeployment != nil else { return false }
            deploymentInspectorTab = .overview
        default:
            return false
        }

        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    private func openPortForwardInspectorForSelection() -> Bool {
        switch viewModel.state.selectedSection {
        case .workloads:
            guard viewModel.state.selectedWorkloadKind == .pod, viewModel.state.selectedPod != nil else { return false }
            podInspectorTab = .portForward
        case .networking:
            guard viewModel.state.selectedWorkloadKind == .service, viewModel.state.selectedService != nil else { return false }
            serviceInspectorTab = .portForward
        case .overview, .config, .storage, .rbac, .events, .helm, .terminal:
            return false
        }

        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    private func openRolloutInspectorForSelection() -> Bool {
        guard viewModel.state.selectedSection == .workloads,
              viewModel.state.selectedWorkloadKind == .deployment,
              viewModel.state.selectedDeployment != nil else {
            return false
        }
        deploymentInspectorTab = .rollout
        yamlManifestIsEditing = false
        keyboardPaneFocus = .detail
        return true
    }

    private var hasGenericManifestSelection: Bool {
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .statefulSet:
                return viewModel.state.selectedStatefulSet != nil
            case .daemonSet:
                return viewModel.state.selectedDaemonSet != nil
            case .job:
                return viewModel.state.selectedJob != nil
            case .cronJob:
                return viewModel.state.selectedCronJob != nil
            case .replicaSet:
                return viewModel.state.selectedReplicaSet != nil
            case .horizontalPodAutoscaler:
                return viewModel.state.selectedHorizontalPodAutoscaler != nil
            default:
                return false
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .endpoint:
                return viewModel.state.selectedEndpoint != nil
            case .ingress:
                return viewModel.state.selectedIngress != nil
            case .networkPolicy:
                return viewModel.state.selectedNetworkPolicy != nil
            default:
                return false
            }
        case .config:
            switch viewModel.state.selectedWorkloadKind {
            case .configMap:
                return viewModel.state.selectedConfigMap != nil
            case .secret:
                return viewModel.state.selectedSecret != nil
            default:
                return false
            }
        case .storage:
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolumeClaim:
                return viewModel.state.selectedPersistentVolumeClaim != nil
            case .persistentVolume:
                return viewModel.state.selectedPersistentVolume != nil
            case .storageClass:
                return viewModel.state.selectedStorageClass != nil
            case .node:
                return viewModel.state.selectedNode != nil
            default:
                return false
            }
        case .rbac:
            return viewModel.state.selectedRBACResource != nil
        case .overview, .events, .helm, .terminal:
            return false
        }
    }

    private var hasDeletableSelection: Bool {
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                return viewModel.state.selectedPod != nil
            case .deployment:
                return viewModel.state.selectedDeployment != nil
            default:
                return hasGenericManifestSelection
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                return viewModel.state.selectedService != nil
            default:
                return hasGenericManifestSelection
            }
        case .config, .storage, .rbac:
            return hasGenericManifestSelection
        case .overview, .events, .helm, .terminal:
            return false
        }
    }

    private func moveDetailInspectorTab(_ direction: MoveCommandDirection) {
        switch viewModel.state.selectedSection {
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .pod:
                guard viewModel.state.selectedPod != nil else { return }
                podInspectorTab = advancedTab(current: podInspectorTab, direction: direction)
            case .deployment:
                guard viewModel.state.selectedDeployment != nil else { return }
                deploymentInspectorTab = advancedTab(current: deploymentInspectorTab, direction: direction)
            case .cronJob:
                guard viewModel.state.selectedCronJob != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .statefulSet:
                guard viewModel.state.selectedStatefulSet != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .daemonSet:
                guard viewModel.state.selectedDaemonSet != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .job:
                guard viewModel.state.selectedJob != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .replicaSet:
                guard viewModel.state.selectedReplicaSet != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .horizontalPodAutoscaler:
                guard viewModel.state.selectedHorizontalPodAutoscaler != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            default:
                break
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .service:
                guard viewModel.state.selectedService != nil else { return }
                serviceInspectorTab = advancedTab(current: serviceInspectorTab, direction: direction)
            case .ingress:
                guard viewModel.state.selectedIngress != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .endpoint:
                guard viewModel.state.selectedEndpoint != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .networkPolicy:
                guard viewModel.state.selectedNetworkPolicy != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            default:
                break
            }
        case .config:
            switch viewModel.state.selectedWorkloadKind {
            case .configMap:
                guard viewModel.state.selectedConfigMap != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .secret:
                guard viewModel.state.selectedSecret != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            default:
                break
            }
        case .storage:
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolumeClaim:
                guard viewModel.state.selectedPersistentVolumeClaim != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .persistentVolume:
                guard viewModel.state.selectedPersistentVolume != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .storageClass:
                guard viewModel.state.selectedStorageClass != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .node:
                guard viewModel.state.selectedNode != nil else { return }
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            default:
                break
            }
        case .rbac:
            guard viewModel.state.selectedRBACResource != nil else { return }
            genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
        case .helm:
            switch selectedHelmInspectorMode {
            case .operatorResource:
                genericResourceManifestTab = advancedTab(current: genericResourceManifestTab, direction: direction)
            case .release:
                helmInspectorTab = advancedTab(current: helmInspectorTab, direction: direction)
            case .none:
                return
            }
        case .terminal:
            terminalInspectorTab = advancedTab(current: terminalInspectorTab, direction: direction)
        case .overview, .events:
            break
        }
    }

    private func advancedTab<T: CaseIterable & Equatable>(current: T, direction: MoveCommandDirection) -> T {
        let all = Array(T.allCases)
        guard let index = all.firstIndex(of: current), !all.isEmpty else { return current }
        switch direction {
        case .right, .down:
            return all[(index + 1) % all.count]
        case .left, .up:
            return all[(index + all.count - 1) % all.count]
        @unknown default:
            return current
        }
    }

    private func steppedIndex(count: Int, current: Int, direction: MoveCommandDirection) -> Int? {
        guard count > 0 else { return nil }
        switch direction {
        case .down, .right:
            return min(current + 1, count - 1)
        case .up, .left:
            return max(current - 1, 0)
        @unknown default:
            return current
        }
    }

    private func steppedItem<T: Identifiable>(
        items: [T],
        currentID: T.ID?,
        direction: MoveCommandDirection
    ) -> T? where T.ID: Equatable {
        guard !items.isEmpty else { return nil }
        guard let currentID,
              let currentIndex = items.firstIndex(where: { $0.id == currentID }),
              let nextIndex = steppedIndex(count: items.count, current: currentIndex, direction: direction) else {
            return items.first
        }
        return items[nextIndex]
    }

    private func emitLayoutSnapshotIfNeeded() {
        let measuredTopInset = workspaceWindowReference.measuredTopInset
        let resolvedTopInset = RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: measuredTopInset)
        if let contentMinY = layoutProbeFrames[.content]?.minY,
           let headerMinY = layoutProbeFrames[.header]?.minY,
           let detailMinY = layoutProbeFrames[.detail]?.minY {
            let minVisibleProbeY = resolvedTopInset - 1
            guard contentMinY >= minVisibleProbeY,
                  headerMinY >= minVisibleProbeY,
                  detailMinY >= minVisibleProbeY
            else { return }
        }

        let snapshot = RuneRootLayoutSnapshot(
            section: viewModel.state.selectedSection,
            workloadKind: viewModel.state.selectedWorkloadKind,
            measuredWindowTopInset: measuredTopInset,
            resolvedWindowTopInset: resolvedTopInset,
            contentMinY: layoutProbeFrames[.content]?.minY,
            headerMinY: layoutProbeFrames[.header]?.minY,
            detailMinY: layoutProbeFrames[.detail]?.minY,
            contentMinX: layoutProbeFrames[.content]?.minX,
            headerMinX: layoutProbeFrames[.header]?.minX,
            detailMinX: layoutProbeFrames[.detail]?.minX
        )

        guard snapshot != lastLayoutSnapshot else { return }

        lastLayoutSnapshot = snapshot
        RuneRootLayoutDebug.log(
            snapshot,
            shellVariant: resolvedShellVariant,
            inlineEditorImplementation: resolvedManifestInlineEditorImplementation
        )
        onLayoutSnapshotChange?(snapshot)
    }

    private func genericResourceListIdentity(_ resources: [ClusterResourceSummary]) -> String {
        "\(resources.count):\(resources.first?.id ?? ""):\(resources.last?.id ?? "")"
    }

    private func podListIdentity(_ pods: [PodSummary]) -> String {
        "\(pods.count):\(pods.first?.id ?? ""):\(pods.last?.id ?? "")"
    }

    private func advanceLayoutGeneration() {
        layoutGeneration += 1
        layoutProbeFrames = [:]
        lastLayoutSnapshot = nil
    }

    private var resolvedSidebarWidth: CGFloat {
        clampedSidebarWidth(CGFloat(forcedInitialSidebarWidth ?? persistedSidebarWidth))
    }

    private var resolvedDetailWidth: CGFloat {
        let width = forcedInitialDetailWidth.map { CGFloat($0) }
            ?? RuneRootLayoutDebug.initialDetailWidth
            ?? CGFloat(persistedDetailWidth)
        return clampedDetailWidth(width)
    }

    private var podNameColumnWidth: CGFloat {
        PodTableLayout.clampedNameColumnWidth(CGFloat(persistedPodNameColumnWidth))
    }

    private func commitPodNameColumnWidth(_ width: CGFloat) {
        let clamped = PodTableLayout.clampedNameColumnWidth(width)
        guard abs(clamped - CGFloat(persistedPodNameColumnWidth)) >= 1 else { return }
        persistedPodNameColumnWidth = Double(clamped)
        UserDefaults.standard.set(persistedPodNameColumnWidth, forKey: RuneSettingsKeys.layoutPodNameColumnWidth)
    }

    private var compactDetailIdealWidth: CGFloat {
        guard !viewModel.isSidebarVisible, viewModel.isDetailPaneVisible else {
            return resolvedDetailWidth
        }

        let expandedDefault = RuneUILayoutMetrics.splitDetailColumnIdealWidth + 260
        return clampedDetailWidth(max(resolvedDetailWidth, expandedDefault), maxWidth: compactDetailMaxWidth)
    }

    private var compactDetailMaxWidth: CGFloat {
        viewModel.isSidebarVisible
            ? RuneUILayoutMetrics.splitDetailColumnMaxWidth
            : RuneUILayoutMetrics.splitDetailColumnExpandedMaxWidth
    }

    private func clampedSidebarWidth(_ width: CGFloat) -> CGFloat {
        min(max(width, RuneUILayoutMetrics.splitSidebarMinWidth), RuneUILayoutMetrics.splitSidebarMaxWidth)
    }

    private func clampedDetailWidth(_ width: CGFloat) -> CGFloat {
        clampedDetailWidth(width, maxWidth: RuneUILayoutMetrics.splitDetailColumnMaxWidth)
    }

    private func clampedDetailWidth(_ width: CGFloat, maxWidth: CGFloat) -> CGFloat {
        min(max(width, RuneUILayoutMetrics.splitDetailColumnMinWidth), maxWidth)
    }

    private func persistPaneWidthsIfNeeded(_ paneWidths: [RuneRootPaneWidthKind: CGFloat]) {
        if let sidebarWidth = paneWidths[.sidebar], sidebarWidth > 1 {
            persistSidebarWidthIfNeeded(sidebarWidth)
        }

        if let detailWidth = paneWidths[.detail], detailWidth > 1 {
            persistDetailWidthIfNeeded(detailWidth)
        }
    }

    private func persistSidebarWidthIfNeeded(_ width: CGFloat) {
        guard !debugDisableLayoutPersistence else { return }
        let clamped = clampedSidebarWidth(width)
        if abs(clamped - CGFloat(persistedSidebarWidth)) >= 1 {
            persistedSidebarWidth = Double(clamped)
            UserDefaults.standard.set(persistedSidebarWidth, forKey: RuneSettingsKeys.layoutSidebarWidth)
        }
    }

    private func persistDetailWidthIfNeeded(_ width: CGFloat) {
        guard !debugDisableLayoutPersistence else { return }
        let clamped = clampedDetailWidth(width, maxWidth: compactDetailMaxWidth)
        if abs(clamped - CGFloat(persistedDetailWidth)) >= 1 {
            persistedDetailWidth = Double(clamped)
            UserDefaults.standard.set(persistedDetailWidth, forKey: RuneSettingsKeys.layoutDetailWidth)
        }
    }

    /// Visual resize affordance on column edges (`49c6517`); hit testing stays on the system split divider.
    private var splitColumnResizeHandle: some View {
        RoundedRectangle(cornerRadius: 999, style: .continuous)
            .fill(Color.secondary.opacity(0.42))
            .frame(width: 4, height: 44)
            .overlay {
                VStack(spacing: 4) {
                    Circle().fill(Color.primary.opacity(0.18)).frame(width: 2, height: 2)
                    Circle().fill(Color.primary.opacity(0.18)).frame(width: 2, height: 2)
                    Circle().fill(Color.primary.opacity(0.18)).frame(width: 2, height: 2)
                }
            }
            .frame(width: 14, height: 44)
            .allowsHitTesting(false)
    }

    private var productionBanner: some View {
        HStack(spacing: 8) {
            Image(systemName: "exclamationmark.triangle.fill")
            Text("Production context active")
                .font(.subheadline.weight(.bold))
        }
        .padding(.horizontal, 14)
        .padding(.vertical, 7)
        .background(Color.red.opacity(0.87), in: Capsule())
    }

    private var commandPalettePresentedBinding: Binding<Bool> {
        Binding(
            get: { viewModel.state.isCommandPalettePresented },
            set: { value in
                if value {
                    viewModel.presentCommandPalette()
                } else {
                    viewModel.dismissCommandPalette()
                }
            }
        )
    }

    private var pendingWriteActionPresentedBinding: Binding<Bool> {
        Binding(
            get: { viewModel.pendingWriteAction != nil },
            set: { value in
                if !value {
                    guard !isConfirmingPendingWriteActionFromDialog else { return }
                    viewModel.cancelPendingWriteAction()
                }
            }
        )
    }

    private func confirmPendingWriteActionFromDialog() {
        isConfirmingPendingWriteActionFromDialog = true
        viewModel.confirmPendingWriteAction()
        DispatchQueue.main.async {
            isConfirmingPendingWriteActionFromDialog = false
        }
    }

    private func cancelPendingWriteActionFromDialog() {
        isConfirmingPendingWriteActionFromDialog = false
        viewModel.cancelPendingWriteAction()
    }

    private var overviewStatusBanner: some View {
        HStack(spacing: 8) {
            Image(systemName: viewModel.isProductionContext ? "exclamationmark.shield.fill" : "checkmark.shield.fill")
                .foregroundStyle(viewModel.isProductionContext ? .red : .green)
            Text(viewModel.isProductionContext ? "Production context active" : "Non-production context")
                .font(.subheadline.weight(.semibold))
            Spacer()
            HStack(spacing: 5) {
                Circle()
                    .fill(snapshotFreshnessColor)
                    .frame(width: 7, height: 7)
                Text(snapshotFreshnessText)
                    .font(.caption.weight(.semibold))
                    .lineLimit(1)
            }
            .help(viewModel.state.snapshotFreshness.message)
            Text(viewModel.state.isReadOnlyMode ? "Read-only" : "Read/Write")
                .font(.caption.weight(.bold))
                .padding(.horizontal, 8)
                .frame(height: 28)
                .background((viewModel.state.isReadOnlyMode ? Color.orange : Color.green).opacity(0.24), in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
        }
        .padding(12)
        .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
    }

    @ViewBuilder
    private var manualNamespaceBanner: some View {
        if viewModel.state.isManualNamespaceMode {
            HStack(alignment: .top, spacing: 8) {
                Image(systemName: "square.and.pencil")
                    .foregroundStyle(.orange)
                VStack(alignment: .leading, spacing: 3) {
                    Text("Manual namespace mode")
                        .font(.subheadline.weight(.semibold))
                    Text(viewModel.state.namespaceAccessWarning ?? "You cannot list namespaces, but you can work in a namespace manually.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
                Spacer()
                Button("Enter Namespace...") {
                    manualNamespaceInput = viewModel.state.selectedNamespace
                    isManualNamespaceSheetPresented = true
                }
                .buttonStyle(.bordered)
            }
            .padding(12)
            .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
        }
    }

    @ViewBuilder
    private var authDoctorPanel: some View {
        if shouldReserveAuthDoctorPanel {
            let hasChecks = !viewModel.state.authDoctorChecks.isEmpty
            let runLabel = viewModel.state.isRunningAuthDoctor
                ? "Running..."
                : (hasChecks ? "Run Again" : "Run Auth Doctor")

            VStack(alignment: .leading, spacing: 10) {
                authDoctorHeader(runLabel: runLabel)

                if isAuthDoctorPanelExpanded {
                    requestMetricsSummaryRow

                    if viewModel.state.authDoctorChecks.isEmpty, !viewModel.state.isRunningAuthDoctor {
                        HStack(alignment: .top, spacing: 8) {
                            Image(systemName: "checkmark.shield")
                                .foregroundStyle(.secondary)
                                .frame(width: 16)
                            Text("Run Auth Doctor to check kubeconfig, context auth, namespace access, logs, and RBAC before troubleshooting workloads.")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                                .fixedSize(horizontal: false, vertical: true)
                        }
                    } else {
                        ForEach(viewModel.state.authDoctorChecks) { check in
                            authDoctorCheckRow(check)
                        }
                    }
                }
            }
            .padding(12)
            .background(panelFill, in: RoundedRectangle(cornerRadius: RuneUILayoutMetrics.groupedContentCornerRadius, style: .continuous))
            .task {
                viewModel.refreshKubernetesRequestMetricsSummary()
            }
        }
    }

    private func authDoctorHeader(runLabel: String) -> some View {
        HStack(alignment: .center, spacing: 8) {
            authDoctorDisclosureButton
            authDoctorSummaryChip

            ScrollView(.horizontal, showsIndicators: false) {
                authDoctorActions(runLabel: runLabel)
            }
            .scrollClipDisabled()
            .frame(maxWidth: .infinity, minHeight: 32, alignment: .trailing)
        }
    }

    private var authDoctorDisclosureButton: some View {
        RuneDisclosureRow(
            "Auth Doctor",
            isExpanded: isAuthDoctorPanelExpanded,
            fillsAvailableWidth: false,
            help: isAuthDoctorPanelExpanded ? "Collapse Auth Doctor output" : "Expand Auth Doctor output",
            action: {
                withAnimation(.snappy(duration: 0.16)) {
                    isAuthDoctorPanelExpanded.toggle()
                }
            }
        ) {
            Label("Auth Doctor", systemImage: "stethoscope")
                .font(.subheadline.weight(.semibold))
                .labelStyle(.titleAndIcon)
        }
        .frame(minWidth: 154, alignment: .leading)
    }

    private func authDoctorActions(runLabel: String) -> some View {
        HStack(alignment: .center, spacing: 6) {
            Button {
                isAuthDoctorPanelExpanded = true
                viewModel.runAuthDoctor()
            } label: {
                Label(runLabel, systemImage: "stethoscope")
            }
            .disabled(viewModel.state.isRunningAuthDoctor)
            .help("Run kubeconfig, auth, RBAC, logs, exec, and port-forward checks.")

            Menu {
                Button("Save Bundle") {
                    viewModel.saveSupportBundle()
                }

                Button("Save Bundle to Export Folder") {
                    viewModel.saveSupportBundleToExportFolder(openAfterSave: false)
                }

                Button("Save Bundle and Open") {
                    viewModel.saveSupportBundleToExportFolder(openAfterSave: true)
                }
            } label: {
                Label("Save Bundle", systemImage: "square.and.arrow.down")
                    .runeMinimumInteractiveTarget()
            }
            .help("Save Auth Doctor diagnostics and support data.")

            Button {
                withAnimation(.snappy(duration: 0.16)) {
                    viewModel.clearAuthDoctorOutput()
                    isAuthDoctorPanelExpanded = false
                }
            } label: {
                Label("Clear", systemImage: "xmark.circle")
            }
            .disabled(viewModel.state.isRunningAuthDoctor || viewModel.state.authDoctorChecks.isEmpty)
            .help("Clear Auth Doctor results.")
        }
        .buttonStyle(.bordered)
        .controlSize(.small)
        .fixedSize(horizontal: true, vertical: false)
    }

    private var requestMetricsSummaryRow: some View {
        let summary = viewModel.kubernetesRequestMetricsSummary

        return HStack(alignment: .top, spacing: 8) {
            Image(systemName: summary.hasFailures ? "chart.bar.xaxis.ascending.badge.exclamationmark" : "chart.bar.xaxis.ascending")
                .foregroundStyle(summary.hasFailures ? .orange : .secondary)
                .frame(width: 16)

            VStack(alignment: .leading, spacing: 2) {
                Text("API Requests")
                    .font(.caption.weight(.semibold))
                Text("\(summary.requestCountText) • \(summary.outcomeText)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                Text("\(summary.transferText) • \(summary.retainedText)")
                    .font(.caption2)
                    .foregroundStyle(.tertiary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            Spacer(minLength: 8)

            Button {
                viewModel.refreshKubernetesRequestMetricsSummary()
            } label: {
                Label("Refresh", systemImage: "arrow.clockwise")
            }
            .buttonStyle(.bordered)
            .controlSize(.small)
            .disabled(viewModel.isRefreshingKubernetesRequestMetricsSummary)
            .help("Refresh API request metrics")
        }
    }

    private func authDoctorCheckRow(_ check: RuneHealthCheck) -> some View {
        HStack(alignment: .top, spacing: 8) {
            Image(systemName: authDoctorSymbol(for: check.status))
                .foregroundStyle(authDoctorColor(for: check.status))
                .frame(width: 16)

            VStack(alignment: .leading, spacing: 2) {
                Text(check.title)
                    .font(.caption.weight(.semibold))
                Text(check.message)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            Spacer(minLength: 8)

            if let action = AuthDoctorEntryActionResolver.resolve(check: check, hasPodTarget: authDoctorTargetPod != nil) {
                Button {
                    performAuthDoctorEntryAction(action)
                } label: {
                    Label(action.title, systemImage: action.systemImage)
                }
                .buttonStyle(.bordered)
                .controlSize(.small)
                .help(action.help)
            }
        }
    }

    private func performAuthDoctorEntryAction(_ action: AuthDoctorEntryResolution) {
        switch action.destination {
        case let .section(section):
            viewModel.setSection(section)
        case let .resource(section, kind):
            viewModel.setSection(section)
            viewModel.setWorkloadKind(kind)
        case let .rbacCanIPreset(verb, resource, apiGroup, subresource, scope):
            viewModel.setSection(.rbac)
            viewModel.useRBACCanIPreset(
                verb: verb,
                resource: resource,
                apiGroup: apiGroup,
                subresource: subresource,
                scope: scope == .cluster ? .cluster : .namespace
            )
        case .podLogs:
            guard let pod = authDoctorTargetPod else { return }
            viewModel.setSection(.workloads)
            viewModel.selectPod(pod)
            podInspectorTab = .logs
            viewModel.reloadLogsForSelection()
        case .podExec:
            guard let pod = authDoctorTargetPod else { return }
            viewModel.setSection(.workloads)
            viewModel.selectPod(pod)
            podInspectorTab = .exec
        case .podPortForward:
            guard let pod = authDoctorTargetPod else { return }
            viewModel.setSection(.workloads)
            viewModel.selectPod(pod)
            podInspectorTab = .portForward
        case .kubeconfigReview:
            viewModel.reviewLoadedKubeConfigSources()
            isManualAddClusterExpanded = false
            addClusterPopoverPresented = true
        case let .documentation(url):
            NSWorkspace.shared.open(url)
        }
    }

    private var authDoctorTargetPod: PodSummary? {
        viewModel.state.selectedPod
            ?? viewModel.visiblePods.first
            ?? viewModel.state.pods.first
            ?? viewModel.state.overviewPods.first
    }

    private var shouldReserveAuthDoctorPanel: Bool {
        guard !simpleMode else { return false }
        switch viewModel.state.selectedSection {
        case .overview:
            return true
        default:
            return false
        }
    }

    @ViewBuilder
    private var authDoctorSummaryChip: some View {
        let checks = viewModel.state.authDoctorChecks
        let failed = checks.filter { $0.status == .failed }.count
        let warnings = checks.filter { $0.status == .warning }.count
        let running = viewModel.state.isRunningAuthDoctor || checks.contains { $0.status == .running }
        let text = running ? "Running" : (checks.isEmpty ? "Ready" : "\(checks.count) checks")
        let detail = failed > 0 ? "\(failed) failed" : (warnings > 0 ? "\(warnings) warnings" : "OK")
        HStack(spacing: 6) {
            Circle()
                .fill(running ? Color.blue : (failed > 0 ? Color.red : (warnings > 0 ? Color.orange : Color.green)))
                .frame(width: 7, height: 7)
            Text(text)
            if !checks.isEmpty {
                Text(detail)
                    .foregroundStyle(.secondary)
            }
        }
        .font(.caption.weight(.semibold))
        .padding(.horizontal, 8)
        .frame(minHeight: RuneUILayoutMetrics.headerChipHeight)
        .background(panelFill, in: Capsule())
    }

    private func authDoctorSymbol(for status: RuneHealthCheckStatus) -> String {
        switch status {
        case .running: return "arrow.triangle.2.circlepath"
        case .passed: return "checkmark.circle.fill"
        case .warning: return "exclamationmark.triangle.fill"
        case .failed: return "xmark.octagon.fill"
        }
    }

    private func authDoctorColor(for status: RuneHealthCheckStatus) -> Color {
        switch status {
        case .running: return .blue
        case .passed: return .green
        case .warning: return .orange
        case .failed: return .red
        }
    }

    private var snapshotFreshnessText: String {
        switch viewModel.state.snapshotFreshness.status {
        case .idle: return "No data"
        case .refreshing: return "Refreshing"
        case .reconnecting: return "Reconnecting"
        case .live: return "Live"
        case .stale: return "Stale"
        case .failed: return "Failed"
        }
    }

    private var snapshotFreshnessColor: Color {
        switch viewModel.state.snapshotFreshness.status {
        case .idle: return .secondary
        case .refreshing: return .blue
        case .reconnecting: return .blue
        case .live: return .green
        case .stale: return .orange
        case .failed: return .red
        }
    }

    private var currentResourceListFreshness: RuneResourceListFreshness? {
        guard let family = currentResourceListFamily else { return nil }
        return viewModel.state.freshness(for: family)
    }

    private var currentResourceListFamily: RuneResourceListFamily? {
        switch viewModel.state.selectedSection {
        case .overview:
            return nil
        case .workloads:
            switch viewModel.state.selectedWorkloadKind {
            case .deployment: return .deployments
            case .statefulSet: return .statefulSets
            case .daemonSet: return .daemonSets
            case .job: return .jobs
            case .cronJob: return .cronJobs
            case .replicaSet: return .replicaSets
            case .horizontalPodAutoscaler: return .horizontalPodAutoscalers
            default: return .pods
            }
        case .networking:
            switch viewModel.state.selectedWorkloadKind {
            case .endpoint: return .endpoints
            case .ingress: return .ingresses
            case .networkPolicy: return .networkPolicies
            default: return .services
            }
        case .storage:
            switch viewModel.state.selectedWorkloadKind {
            case .persistentVolume: return .persistentVolumes
            case .storageClass: return .storageClasses
            case .node: return .nodes
            default: return .persistentVolumeClaims
            }
        case .config:
            return viewModel.state.selectedWorkloadKind == .secret ? .secrets : .configMaps
        case .rbac:
            switch viewModel.state.selectedWorkloadKind {
            case .serviceAccount: return .serviceAccounts
            case .roleBinding: return .rbacRoleBindings
            case .clusterRole: return .rbacClusterRoles
            case .clusterRoleBinding: return .rbacClusterRoleBindings
            default: return .rbacRoles
            }
        case .events:
            return .events
        case .helm:
            return helmBrowserTab == .operatorResources ? .operatorResources : .helmReleases
        case .terminal:
            return .pods
        }
    }

    private func contextUsageBadge(label: String, value: String) -> some View {
        RuneHeaderCapsule(
            "\(label): \(value)",
            role: .value,
            systemImage: label == "CPU" ? "cpu" : "memorychip",
            accessibilityLabel: "\(label) usage: \(value)"
        )
    }

    private func contextUsageValue(_ value: Int?) -> String {
        if let value {
            return "\(value)%"
        }
        return viewModel.state.isLoading ? "..." : "n/a"
    }

    private func healthBadge(label: String, value: Int, color: Color) -> some View {
        HStack(spacing: 6) {
            Circle()
                .fill(color)
                .frame(width: 7, height: 7)
            Text(label + ": \(value)")
                .font(.caption.weight(.semibold))
        }
        .padding(.horizontal, 9)
        .padding(.vertical, 5)
        .background(color.opacity(0.14), in: Capsule())
    }

    private func inspectorActionButtonRow<Content: View>(@ViewBuilder content: () -> Content) -> some View {
        RuneInspectorActionRow(content: content)
    }

    private func normalizedCopyValue(_ value: String) -> String? {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, trimmed != "—", trimmed.lowercased() != "n/a" else {
            return nil
        }
        return trimmed
    }

    private func copyToClipboard(_ value: String) {
        guard let copyValue = normalizedCopyValue(value) else { return }
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        pasteboard.setString(copyValue, forType: .string)
    }

    private func copySelectedTextFromFocusedTextView() {
        guard let textView = NSApp.keyWindow?.firstResponder as? NSTextView else { return }
        let text = textView.string as NSString
        let selectedRange = textView.selectedRange()
        guard selectedRange.length > 0, NSMaxRange(selectedRange) <= text.length else { return }
        let lineRange = text.lineRange(for: selectedRange)
        let selectedLines = text.substring(with: lineRange).trimmingCharacters(in: .newlines)
        copyToClipboard(selectedLines)
    }

    @ViewBuilder
    private func genericResourceContextMenu(
        _ resource: ClusterResourceSummary,
        action: @escaping (ClusterResourceSummary?) -> Void
    ) -> some View {
        Button {
            viewModel.toggleFavoriteResource(kind: resource.kind, namespace: resource.namespace, name: resource.name)
        } label: {
            Label(
                viewModel.isFavoriteResource(kind: resource.kind, namespace: resource.namespace, name: resource.name) ? "Remove Favorite" : "Favorite Resource",
                systemImage: viewModel.isFavoriteResource(kind: resource.kind, namespace: resource.namespace, name: resource.name) ? "star.slash" : "star"
            )
        }
        Divider()
        Button {
            action(resource)
            genericResourceManifestTab = .describe
        } label: {
            Label("Describe", systemImage: "doc.text.magnifyingglass")
        }
        Button {
            action(resource)
            genericResourceManifestTab = .yaml
        } label: {
            Label("Open YAML", systemImage: "curlybraces")
        }
        Divider()
        copyMenuItem(value: resource.name, label: "\(resource.kind.singularTypeName) name")
        if let namespace = resource.namespace {
            copyMenuItem(value: namespace, label: "namespace")
        }
        Divider()
        Button(role: .destructive) {
            viewModel.requestDeleteResource(kind: resource.kind, name: resource.name)
        } label: {
            Label("Delete \(resource.kind.singularTypeName)", systemImage: "trash")
        }
        .disabled(!viewModel.canApplyClusterMutations)
    }

    @ViewBuilder
    private func copyMenuItem(value: String, label: String) -> some View {
        if normalizedCopyValue(value) != nil {
            Button("Copy \(label)") {
                copyToClipboard(value)
            }
        }
    }

    @ViewBuilder
    private func copyButton(value: String, label: String) -> some View {
        if normalizedCopyValue(value) != nil {
            RuneIconButton(
                "Copy \(label)",
                systemImage: "doc.on.doc"
            ) {
                copyToClipboard(value)
            }
        }
    }

    private func copyableInspectorTitle(_ value: String, label: String) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 8) {
            Text(value)
                .font(.title2.weight(.bold))
                .lineLimit(1)
                .textSelection(.enabled)
            copyButton(value: value, label: label)
            Spacer(minLength: 0)
            detailRefreshButton
        }
        .help(value)
        .contextMenu {
            copyMenuItem(value: value, label: label)
        }
    }

    private var detailRefreshButton: some View {
        RuneIconButton(
            "Refresh inspector",
            systemImage: "arrow.clockwise"
        ) {
            refreshDetailPane()
        }
    }

    private func refreshDetailPane() {
        let route = RuneInspectorRefreshRouting.route(
            section: viewModel.state.selectedSection,
            workloadKind: viewModel.state.selectedWorkloadKind,
            podTab: podInspectorTab,
            deploymentTab: deploymentInspectorTab,
            serviceTab: serviceInspectorTab,
            genericTab: genericResourceManifestTab,
            helmTab: helmInspectorTab,
            helmMode: selectedHelmInspectorMode
        )

        switch route {
        case .currentView:
            viewModel.refreshCurrentView(debounced: false)
        case .resourceInspector:
            viewModel.refreshResourceInspectorOnly()
        case .logs:
            viewModel.reloadLogsForSelection()
        case .helmInspector:
            viewModel.refreshSelectedHelmInspector()
        }
    }

    private func copyableInlineText(_ text: String, copyValue: String, label: String) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 6) {
            Text(text)
                .font(.body.weight(.medium))
                .textSelection(.enabled)
                .fixedSize(horizontal: false, vertical: true)
            copyButton(value: copyValue, label: label)
        }
        .contextMenu {
            copyMenuItem(value: copyValue, label: label)
        }
    }

    private func deploymentReplicaStatusColor(_ deployment: DeploymentSummary) -> Color {
        if deployment.desiredReplicas == 0 { return .secondary }
        if deployment.readyReplicas >= deployment.desiredReplicas { return .green }
        if deployment.readyReplicas > 0 { return .orange }
        return .red
    }

    private func deploymentReplicaStatusText(_ deployment: DeploymentSummary) -> String {
        if deployment.desiredReplicas == 0 {
            return "Scaled to zero"
        }
        return "\(deployment.readyReplicas) of \(deployment.desiredReplicas) ready"
    }

    private func deploymentScaleIsDirty(_ deployment: DeploymentSummary) -> Bool {
        viewModel.scaleReplicaInput != deployment.desiredReplicas
    }

    private func podOverviewSection(pod: PodSummary) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            podOverviewRow(title: "Ready", value: pod.containersReady ?? "—", symbol: "checkmark.circle")
            podOverviewRow(title: "Restarts", value: "\(pod.totalRestarts)", symbol: "arrow.clockwise")
            podOverviewRow(title: "Age", value: pod.ageDescription, symbol: "clock")
            podOverviewRow(title: "CPU", value: pod.cpuDisplay, symbol: "cpu")
            podOverviewRow(title: "Memory", value: pod.memoryDisplay, symbol: "memorychip")
            Divider()
                .opacity(0.45)
            podOverviewRow(title: "Node", value: pod.nodeName ?? "—", symbol: "server.rack")
            if let node = viewModel.selectedPodRelatedNode {
                ResourceRelationshipSection(title: "Scheduled Node") {
                    ResourceRelationshipLinkButton(
                        title: node.name,
                        subtitle: "\(node.primaryText) · \(node.secondaryText)",
                        symbol: "server.rack"
                    ) {
                        viewModel.openPodRelatedNode(node)
                    }
                }
            }
            let relatedEvents = viewModel.selectedPodRelatedEvents
            if !relatedEvents.isEmpty {
                RelatedEventsRelationshipSection(events: relatedEvents, open: viewModel.openRelatedEvent)
            }
            podOverviewRow(title: "Pod IP", value: pod.podIP ?? "—", symbol: "network")
            podOverviewRow(title: "Host IP", value: pod.hostIP ?? "—", symbol: "cable.connector")
            podOverviewRow(title: "QoS class", value: pod.qosClass ?? "—", symbol: "slider.horizontal.3")
            if let containers = pod.containerNamesLine, !containers.isEmpty {
                RuneInspectorInfoRow("Containers", systemImage: "square.stack.3d.forward.dottedline") {
                    Text(containers)
                        .font(.system(size: 12, weight: .regular, design: .monospaced))
                        .textSelection(.enabled)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
            }

        }
    }

    private func podInspectorCoreInfo(_ pod: PodSummary) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            if shouldShowResourceNamespaceLabel(pod.namespace) {
                inspectorInfoRow("Namespace", value: pod.namespace, symbol: "square.stack.3d.up")
            }
            inspectorInfoRow("Status", value: pod.status, symbol: "waveform.path.ecg")
        }
    }

    private func podOverviewRow(title: String, value: String, symbol: String) -> some View {
        inspectorInfoRow(title, value: value, symbol: symbol)
    }

    private func copyableOverviewValue(_ value: String, label: String) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 6) {
            Text(value)
                .font(.body.weight(.medium))
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
                .fixedSize(horizontal: false, vertical: true)
            copyButton(value: value, label: label.lowercased())
        }
        .contextMenu {
            copyMenuItem(value: value, label: label.lowercased())
        }
    }

    private func inspectorInfoRow(_ title: String, value: String, symbol: String) -> some View {
        RuneInspectorInfoRow(title, systemImage: symbol) {
            copyableOverviewValue(value, label: title)
        }
    }

    private func deploymentOverviewSection(deployment: DeploymentSummary) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            WorkloadReplicaScaleControlsView(
                label: "Replicas",
                isDirty: deploymentScaleIsDirty(deployment),
                canMutate: viewModel.canApplyClusterMutations,
                replicas: $viewModel.scaleReplicaInput,
                action: viewModel.requestScaleSelectedDeployment
            )

            Divider()
                .opacity(0.45)

            let relatedReplicaSets = viewModel.selectedDeploymentRelatedReplicaSets
            let shouldOfferReplicaSetHistoryLoad = simpleMode && relatedReplicaSets.isEmpty
            if !relatedReplicaSets.isEmpty || shouldOfferReplicaSetHistoryLoad {
                let historicalReplicaSetCount = relatedReplicaSets.filter(isHistoricalDeploymentReplicaSet).count
                let visibleReplicaSets = showsHistoricalDeploymentReplicaSets
                    ? relatedReplicaSets
                    : relatedReplicaSets.filter { !isHistoricalDeploymentReplicaSet($0) }

                ResourceRelationshipSection(
                    title: "Related ReplicaSets",
                    rowCount: (shouldOfferReplicaSetHistoryLoad ? 1 : visibleReplicaSets.count)
                        + ((historicalReplicaSetCount > 0 || shouldOfferReplicaSetHistoryLoad) ? 1 : 0)
                ) {
                    if shouldOfferReplicaSetHistoryLoad {
                        ResourceRelationshipEmptyRow(
                            title: "History not loaded",
                            subtitle: "Simple mode loads ReplicaSets only when you ask for rollout history."
                        )
                    } else {
                        ForEach(visibleReplicaSets) { replicaSet in
                            ResourceRelationshipLinkButton(
                                title: replicaSet.name,
                                subtitle: "\(replicaSet.namespace ?? deployment.namespace) · \(replicaSet.primaryText)",
                                symbol: "rectangle.stack"
                            ) {
                                viewModel.openDeploymentRelatedReplicaSet(replicaSet)
                            }
                        }
                    }

                    if historicalReplicaSetCount > 0 || shouldOfferReplicaSetHistoryLoad {
                        RuneDisclosureRow(
                            shouldOfferReplicaSetHistoryLoad
                                ? "Deployment history, not loaded"
                                : "Deployment history, \(historicalReplicaSetCount) inactive ReplicaSets",
                            isExpanded: showsHistoricalDeploymentReplicaSets,
                            help: shouldOfferReplicaSetHistoryLoad
                                ? "Load ReplicaSets for rollout history and debugging."
                                : showsHistoricalDeploymentReplicaSets
                                ? "Hide inactive ReplicaSets with 0/0 ready replicas."
                                : "Show inactive ReplicaSets kept for rollout history and debugging.",
                            action: {
                                if shouldOfferReplicaSetHistoryLoad {
                                    showsHistoricalDeploymentReplicaSets = true
                                    viewModel.refreshReplicaSetsForCurrentNamespace()
                                } else {
                                    showsHistoricalDeploymentReplicaSets.toggle()
                                }
                            }
                        ) {
                            HStack(spacing: 8) {
                                Image(systemName: "clock.arrow.circlepath")
                                    .frame(width: 16)
                                    .foregroundStyle(.secondary)
                                Text(showsHistoricalDeploymentReplicaSets ? "Hide history" : "Show history")
                                    .font(.caption.weight(.semibold))
                                if historicalReplicaSetCount > 0 {
                                    Text("(\(historicalReplicaSetCount))")
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                }
                                Spacer(minLength: 0)
                            }
                        }
                    }
                }
            }

            let relatedPods = viewModel.selectedDeploymentRelatedPods
            ResourceRelationshipSection(title: "Related Pods", rowCount: max(relatedPods.count, 1)) {
                if relatedPods.isEmpty {
                    ResourceRelationshipEmptyRow(
                        title: "No related pods in snapshot",
                        subtitle: "Rune has no loaded pod that matches this deployment or its ReplicaSets."
                    )
                } else {
                    ForEach(relatedPods) { pod in
                        ResourceRelationshipLinkButton(
                            title: pod.name,
                            subtitle: "\(pod.namespace) · \(pod.status)",
                            symbol: "cube.box"
                        ) {
                            viewModel.openDeploymentRelatedPod(pod)
                        }
                    }
                }
            }

            let relatedEvents = viewModel.selectedDeploymentRelatedEvents
            if !relatedEvents.isEmpty {
                RelatedEventsRelationshipSection(events: relatedEvents, open: viewModel.openRelatedEvent)
            }

        }
    }

    private func deploymentInspectorCoreInfo(_ deployment: DeploymentSummary) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            if shouldShowResourceNamespaceLabel(deployment.namespace) {
                inspectorInfoRow("Namespace", value: deployment.namespace, symbol: "square.stack.3d.up")
            }
            RuneInspectorInfoRow("Status", systemImage: "shippingbox") {
                HStack(spacing: 8) {
                    Circle()
                        .fill(deploymentReplicaStatusColor(deployment))
                        .frame(width: 8, height: 8)
                    Text(deploymentReplicaStatusText(deployment))
                        .font(.body.weight(.semibold))
                }
            }
        }
    }

    @ViewBuilder
    private var deploymentInspectorActions: some View {
        Button("Restart Rollout") {
            viewModel.requestRolloutRestartSelectedDeployment()
        }
        .buttonStyle(.bordered)
        .disabled(!viewModel.canApplyClusterMutations)

        Button(appString(.applyYAML)) {
            viewModel.requestApplySelectedResourceYAML()
        }
        .buttonStyle(.bordered)
        .disabled(!viewModel.canApplyClusterMutations)

        Menu {
            Button("Export Pod Logs ZIP") {
                viewModel.saveDeploymentPodLogsZip()
            }
            Button("Save Pod Logs ZIP to Export Folder") {
                viewModel.saveDeploymentPodLogsZipToExportFolder(openAfterSave: false)
            }
            Button("Save Pod Logs ZIP and Open") {
                viewModel.saveDeploymentPodLogsZipToExportFolder(openAfterSave: true)
            }
        } label: {
            Label("Export Pod Logs ZIP", systemImage: "archivebox")
        }
        .buttonStyle(.bordered)
        .disabled(viewModel.state.isLoadingLogs)

        Button("Export Pod YAML ZIP") {
            viewModel.saveDeploymentPodYAMLZip()
        }
        .buttonStyle(.bordered)
        .disabled(viewModel.state.isLoadingResourceDetails)

        Button("Delete", role: .destructive) {
            viewModel.requestDeleteSelectedResource()
        }
        .disabled(!viewModel.canApplyClusterMutations)
    }

    private func isHistoricalDeploymentReplicaSet(_ replicaSet: ClusterResourceSummary) -> Bool {
        replicaSet.primaryText
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
            .hasPrefix("0/0 ready")
    }

    private func inspectorEmptyState(_ state: RuneContentState, symbol: String) -> some View {
        RunePaneContentStateView(
            state,
            style: .plain,
            graphicSystemImage: symbol
        )
    }

    private var namespaceSuggestions: [String] {
        viewModel.namespaceOptions
    }

    private var hasAvailableKubernetesContexts: Bool {
        !viewModel.contextMenuOptions.isEmpty
    }

    private var manualNamespaceMenuOptions: [String] {
        viewModel.manualNamespaceOptions
    }

    private var namespaceMenuTitle: String {
        let trimmed = viewModel.state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? "Namespace" : trimmed
    }

    private func openSettingsWindow() {
        openSettings()
    }

    /// When the toolbar already scopes to a namespace, omit duplicate namespace chips/labels for resources in that namespace.
    private func shouldShowResourceNamespaceLabel(_ resourceNamespace: String?) -> Bool {
        guard let ns = resourceNamespace?.trimmingCharacters(in: .whitespacesAndNewlines), !ns.isEmpty else {
            return false
        }
        let selected = viewModel.state.selectedNamespace.trimmingCharacters(in: .whitespacesAndNewlines)
        if selected.isEmpty { return true }
        return ns.caseInsensitiveCompare(selected) != .orderedSame
    }

    private var panelFill: some ShapeStyle {
        RuneSurfaceKind.panel.fill(theme: activeAppearanceTheme)
    }

    private var editorFill: Color {
        RuneSurfaceKind.editor.fill(theme: activeAppearanceTheme)
    }

    private func contentListRowChrome(isSelected: Bool) -> some View {
        RuneSurfaceBackground(kind: .listRow(isSelected: isSelected))
    }

    private var overviewEventsCardHelp: String {
        "Open the Events view. This is the raw Kubernetes Event list for the namespace, while Cluster Signals only promotes selected warning events into triage."
    }

    private func eventHint(for event: EventSummary) -> String {
        let kind = event.involvedKind?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if kind.isEmpty {
            return "Shows details in the inspector. Use the action button to open the involved resource when listed."
        }
        return "Shows details in the inspector. \"Go to …\" switches section and selects \(kind) \(event.objectName) when present."
    }

    private func podStatusCount(_ status: String) -> Int {
        viewModel.state.overviewPods.filter { $0.status.lowercased() == status }.count
    }

    private func statusColor(for status: String) -> Color {
        switch status.lowercased() {
        case "running": return .green
        case "pending": return .orange
        case "failed": return .red
        case "succeeded": return .blue
        default: return .gray
        }
    }

    private func portForwardStatusColor(_ status: PortForwardStatus) -> Color {
        switch status {
        case .starting: return .orange
        case .active: return .green
        case .stopped: return .secondary
        case .failed: return .red
        }
    }

    private func execOutputText(for result: PodExecResult) -> String {
        let stdout = result.stdout.isEmpty ? "" : result.stdout
        let stderr = result.stderr.isEmpty ? "" : "\n[stderr]\n\(result.stderr)"
        let merged = stdout + stderr
        return merged.isEmpty ? "No output" : merged
    }
}

@MainActor
private struct TerminalWorkspacePersistenceLifecycleModifier: ViewModifier {
    let isEnabled: Bool
    let terminalInspectorTab: TerminalInspectorTab
    let terminalShellPodID: String
    let terminalPortForwardPodID: String
    let terminalLogTabState: TerminalPodLogTabState
    let terminalSessions: [PodTerminalSession]
    let activeTerminalSessionID: String?
    let onEnable: @MainActor () -> Void
    let onDisable: @MainActor () -> Void
    let onPersist: @MainActor () -> Void

    func body(content: Content) -> some View {
        content
            .onChange(of: terminalInspectorTab) { _, _ in
                onPersist()
            }
            .onChange(of: terminalShellPodID) { _, _ in
                onPersist()
            }
            .onChange(of: terminalPortForwardPodID) { _, _ in
                onPersist()
            }
            .onChange(of: terminalLogTabState) { _, _ in
                onPersist()
            }
            .onChange(of: terminalSessions) { _, _ in
                onPersist()
            }
            .onChange(of: activeTerminalSessionID) { _, _ in
                onPersist()
            }
            .onChange(of: isEnabled) { _, enabled in
                if enabled {
                    onEnable()
                } else {
                    onDisable()
                }
            }
    }
}
