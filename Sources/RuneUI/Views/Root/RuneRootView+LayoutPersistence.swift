import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

extension RuneRootView {
    func emitLayoutSnapshotIfNeeded() {
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
            detailMinX: layoutProbeFrames[.detail]?.minX,
            resourceFamilyFrame: layoutProbeFrames[.resourceFamily],
            resourceToolbarFrame: layoutProbeFrames[.resourceToolbar],
            resourceFilterRailFrame: layoutProbeFrames[.resourceFilterRail],
            resourceActionsRailFrame: layoutProbeFrames[.resourceActionsRail],
            resourceTableSurfaceFrame: layoutProbeFrames[.resourceTableSurface]
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

    func advanceLayoutGeneration() {
        layoutGeneration += 1
        layoutProbeFrames = [:]
        lastLayoutSnapshot = nil
    }

    var resolvedSidebarWidth: CGFloat {
        clampedSidebarWidth(CGFloat(forcedInitialSidebarWidth ?? persistedSidebarWidth))
    }

    var resolvedDetailWidth: CGFloat {
        let width = forcedInitialDetailWidth.map { CGFloat($0) }
            ?? RuneRootLayoutDebug.initialDetailWidth
            ?? CGFloat(persistedDetailWidth)
        return clampedDetailWidth(width)
    }

    var podNameColumnWidth: CGFloat {
        PodTableLayout.clampedNameColumnWidth(CGFloat(persistedPodNameColumnWidth))
    }

    func commitPodNameColumnWidth(_ width: CGFloat) {
        let clamped = PodTableLayout.clampedNameColumnWidth(width)
        guard abs(clamped - CGFloat(persistedPodNameColumnWidth)) >= 1 else { return }
        persistedPodNameColumnWidth = Double(clamped)
        UserDefaults.standard.set(persistedPodNameColumnWidth, forKey: RuneSettingsKeys.layoutPodNameColumnWidth)
    }

    var compactDetailIdealWidth: CGFloat {
        guard !viewModel.isSidebarVisible, viewModel.isDetailPaneVisible else {
            return resolvedDetailWidth
        }

        let expandedDefault = RuneUILayoutMetrics.splitDetailColumnIdealWidth + 260
        return clampedDetailWidth(max(resolvedDetailWidth, expandedDefault), maxWidth: compactDetailMaxWidth)
    }

    var compactDetailMaxWidth: CGFloat {
        viewModel.isSidebarVisible
            ? RuneUILayoutMetrics.splitDetailColumnMaxWidth
            : RuneUILayoutMetrics.splitDetailColumnExpandedMaxWidth
    }

    func clampedSidebarWidth(_ width: CGFloat) -> CGFloat {
        min(max(width, RuneUILayoutMetrics.splitSidebarMinWidth), RuneUILayoutMetrics.splitSidebarMaxWidth)
    }

    func clampedDetailWidth(_ width: CGFloat) -> CGFloat {
        clampedDetailWidth(width, maxWidth: RuneUILayoutMetrics.splitDetailColumnMaxWidth)
    }

    func clampedDetailWidth(_ width: CGFloat, maxWidth: CGFloat) -> CGFloat {
        min(max(width, RuneUILayoutMetrics.splitDetailColumnMinWidth), maxWidth)
    }

    func persistPaneWidthsIfNeeded(_ paneWidths: [RuneRootPaneWidthKind: CGFloat]) {
        if let sidebarWidth = paneWidths[.sidebar], sidebarWidth > 1 {
            persistSidebarWidthIfNeeded(sidebarWidth)
        }

        if let detailWidth = paneWidths[.detail], detailWidth > 1 {
            persistDetailWidthIfNeeded(detailWidth)
        }
    }

    func persistSidebarWidthIfNeeded(_ width: CGFloat) {
        guard !debugDisableLayoutPersistence else { return }
        let clamped = clampedSidebarWidth(width)
        if abs(clamped - CGFloat(persistedSidebarWidth)) >= 1 {
            persistedSidebarWidth = Double(clamped)
            UserDefaults.standard.set(persistedSidebarWidth, forKey: RuneSettingsKeys.layoutSidebarWidth)
        }
    }

    func persistDetailWidthIfNeeded(_ width: CGFloat) {
        guard !debugDisableLayoutPersistence else { return }
        let clamped = clampedDetailWidth(width, maxWidth: compactDetailMaxWidth)
        if abs(clamped - CGFloat(persistedDetailWidth)) >= 1 {
            persistedDetailWidth = Double(clamped)
            UserDefaults.standard.set(persistedDetailWidth, forKey: RuneSettingsKeys.layoutDetailWidth)
        }
    }

    /// Optional visual resize affordance on column edges; hit testing stays on the system split divider.
    @ViewBuilder
    var splitColumnResizeHandle: some View {
        if showSplitResizeHandles {
            splitColumnResizeHandleGlyph
        }
    }

    var splitColumnResizeHandleGlyph: some View {
        Capsule(style: .continuous)
            .fill(Color.secondary.opacity(0.42))
            .frame(width: 4, height: 44)
            .overlay {
                VStack(spacing: RuneSpacing.xxs) {
                    Circle().fill(Color.primary.opacity(0.18)).frame(width: 2, height: 2)
                    Circle().fill(Color.primary.opacity(0.18)).frame(width: 2, height: 2)
                    Circle().fill(Color.primary.opacity(0.18)).frame(width: 2, height: 2)
                }
            }
            .frame(width: 14, height: 44)
            .allowsHitTesting(false)
    }
}
