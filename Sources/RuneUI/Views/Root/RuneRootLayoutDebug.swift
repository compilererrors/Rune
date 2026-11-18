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
    public let resourceFamilyFrame: CGRect?
    public let resourceToolbarFrame: CGRect?
    public let resourceFilterRailFrame: CGRect?
    public let resourceActionsRailFrame: CGRect?
    public let resourceTableSurfaceFrame: CGRect?

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
        detailMinX: CGFloat? = nil,
        resourceFamilyFrame: CGRect? = nil,
        resourceToolbarFrame: CGRect? = nil,
        resourceFilterRailFrame: CGRect? = nil,
        resourceActionsRailFrame: CGRect? = nil,
        resourceTableSurfaceFrame: CGRect? = nil
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
        self.resourceFamilyFrame = resourceFamilyFrame
        self.resourceToolbarFrame = resourceToolbarFrame
        self.resourceFilterRailFrame = resourceFilterRailFrame
        self.resourceActionsRailFrame = resourceActionsRailFrame
        self.resourceTableSurfaceFrame = resourceTableSurfaceFrame
    }
}

enum RuneRootLayoutProbeKind: Hashable {
    case content
    case header
    case detail
    case resourceFamily
    case resourceToolbar
    case resourceFilterRail
    case resourceActionsRail
    case resourceTableSurface
}

enum RuneRootPaneWidthKind: Hashable {
    case sidebar
    case detail
}

#if DEBUG
enum RuneRootLiveDebugScenarioStep: String, CaseIterable {
    case overview
    case authDoctor
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
    case workloadDeploymentYAMLValidation
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
    case rbacCanI
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
#endif

struct RuneRootLayoutProbeFrame: Equatable {
    let generation: Int
    let rect: CGRect
}

struct RuneRootPaneWidthPreferenceKey: PreferenceKey {
    static let defaultValue: [RuneRootPaneWidthKind: CGFloat] = [:]

    static func reduce(
        value: inout [RuneRootPaneWidthKind: CGFloat],
        nextValue: () -> [RuneRootPaneWidthKind: CGFloat]
    ) {
        value.merge(nextValue(), uniquingKeysWith: { _, new in new })
    }
}

struct RuneRootPaneWidthReporter: View {
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

enum RuneRootLayoutDebug {
    static let coordinateSpaceName = "RuneRootLayoutSpace"

    // Layout logging and the scripted scenario are for debug builds and
    // tests only; a release build compiles them out.
    #if DEBUG
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
    static let liveScenarioSteps: [RuneRootLiveDebugScenarioStep] = {
        guard let rawValue = ProcessInfo.processInfo.environment["RUNE_DEBUG_LAYOUT_LIVE_SCENARIO_STEPS"]?
            .trimmingCharacters(in: .whitespacesAndNewlines),
              !rawValue.isEmpty else {
            return RuneRootLiveDebugScenarioStep.allCases
        }
        let requestedStepNames = Set(
            rawValue
                .split(separator: ",")
                .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                .filter { !$0.isEmpty }
        )
        return RuneRootLiveDebugScenarioStep.allCases.filter {
            requestedStepNames.contains($0.rawValue)
        }
    }()
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
    #else
    static let isEnabled = false
    static let liveScenarioEnabled = false
    static let initialDetailWidth: CGFloat? = nil

    static func log(
        _ snapshot: RuneRootLayoutSnapshot,
        shellVariant: RuneRootShellVariant,
        inlineEditorImplementation: ManifestInlineEditorImplementation
    ) {}
    #endif
}

struct RuneRootLayoutFramePreferenceKey: PreferenceKey {
    static let defaultValue: [RuneRootLayoutProbeKind: RuneRootLayoutProbeFrame] = [:]

    static func reduce(
        value: inout [RuneRootLayoutProbeKind: RuneRootLayoutProbeFrame],
        nextValue: () -> [RuneRootLayoutProbeKind: RuneRootLayoutProbeFrame]
    ) {
        value.merge(nextValue(), uniquingKeysWith: { _, new in new })
    }
}

struct RuneRootLayoutProbe: View {
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
