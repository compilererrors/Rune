import AppKit
import SwiftUI
import XCTest
@testable import RuneCore
@testable import RuneSecurity
@testable import RuneUI

final class RuneUILayoutMetricsTests: XCTestCase {
    func testResolvedWindowContentTopInsetUsesDefaultWhenNil() {
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: nil),
            RuneUILayoutMetrics.windowContentTopInset
        )
    }

    func testResolvedWindowContentTopInsetIgnoresLowerOutliers() {
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: -12),
            RuneUILayoutMetrics.windowContentTopInset
        )
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: 3),
            RuneUILayoutMetrics.windowContentTopInset
        )
    }

    func testResolvedWindowContentTopInsetIgnoresUpperOutliers() {
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: 200),
            RuneUILayoutMetrics.windowContentTopInset
        )
    }

    func testResolvedWindowContentTopInsetDoesNotChangeAfterMeasuredInsetArrives() {
        XCTAssertEqual(
            RuneUILayoutMetrics.resolvedWindowContentTopInset(measuredInset: 18),
            RuneUILayoutMetrics.windowContentTopInset
        )
    }

    func testSharedPaneMetricsStayConsistent() {
        XCTAssertGreaterThan(RuneUILayoutMetrics.paneOuterPadding, RuneUILayoutMetrics.paneInnerPadding)
        XCTAssertEqual(RuneUILayoutMetrics.contentControlSpacing, 8)
        XCTAssertEqual(RuneUILayoutMetrics.contentModuleSpacing, 12)
        XCTAssertEqual(RuneUILayoutMetrics.contentSectionSpacing, 16)
        XCTAssertEqual(RuneUILayoutMetrics.resourceListToolbarMinimumHeight, 30)
        XCTAssertEqual(RuneUILayoutMetrics.resourceListToolbarAccessibilityMinimumHeight, 44)
        XCTAssertGreaterThan(
            RuneUILayoutMetrics.resourceListToolbarAccessibilityMinimumHeight,
            RuneUILayoutMetrics.resourceListToolbarMinimumHeight
        )
        XCTAssertLessThan(
            RuneUILayoutMetrics.resourceFilterFieldMinimumWidth,
            RuneUILayoutMetrics.resourceFilterFieldIdealWidth
        )
        XCTAssertLessThan(
            RuneUILayoutMetrics.resourceFilterFieldIdealWidth,
            RuneUILayoutMetrics.resourceFilterFieldMaximumWidth
        )
        XCTAssertLessThan(
            RuneUILayoutMetrics.contentControlSpacing,
            RuneUILayoutMetrics.contentModuleSpacing
        )
        XCTAssertLessThan(
            RuneUILayoutMetrics.contentModuleSpacing,
            RuneUILayoutMetrics.contentSectionSpacing
        )
        XCTAssertGreaterThan(RuneUILayoutMetrics.headerChipHeight, 24)
        XCTAssertGreaterThan(RuneUILayoutMetrics.headerChipHorizontalPadding, 0)
        XCTAssertGreaterThanOrEqual(
            RuneUILayoutMetrics.windowContentTopInset,
            RuneUILayoutMetrics.minWindowContentTopInset
        )
        XCTAssertLessThanOrEqual(
            RuneUILayoutMetrics.windowContentTopInset,
            RuneUILayoutMetrics.maxWindowContentTopInset
        )
    }

    func testCornerRadiusFamilyKeepsIntentionalHierarchy() {
        XCTAssertEqual(RuneUILayoutMetrics.paneShellCornerRadius, 12)
        XCTAssertEqual(RuneUILayoutMetrics.groupedContentCornerRadius, 10)
        XCTAssertEqual(RuneUILayoutMetrics.interactiveRowCornerRadius, 8)
        XCTAssertEqual(RuneUILayoutMetrics.tabCornerRadius, 7)
        XCTAssertEqual(RuneUILayoutMetrics.compactGlyphCornerRadius, 6)
    }

    func testDetailPaneCanExpandFurtherWhenSidebarIsHidden() {
        XCTAssertGreaterThan(
            RuneUILayoutMetrics.splitDetailColumnExpandedMaxWidth,
            RuneUILayoutMetrics.splitDetailColumnMaxWidth
        )
    }

    func testMinimumWindowSizeSupportsCompactMacScreens() {
        XCTAssertLessThanOrEqual(RuneWindowLayoutDefaults.minimumWidth, 980)
        XCTAssertLessThanOrEqual(RuneWindowLayoutDefaults.minimumHeight, 640)
        XCTAssertGreaterThanOrEqual(
            RuneWindowLayoutDefaults.minimumWidth,
            RuneUILayoutMetrics.splitSidebarMinWidth
                + RuneUILayoutMetrics.splitContentColumnMinWidth
                + RuneUILayoutMetrics.splitDetailColumnMinWidth
        )
    }

    func testAddClusterProviderActionLayoutUsesReadableThreeColumnUtilityGrid() {
        XCTAssertEqual(RuneAddClusterProviderActionLayout.dialogWidth, 560)
        XCTAssertEqual(RuneAddClusterProviderActionLayout.minimumButtonWidth, 150)
        XCTAssertEqual(RuneAddClusterProviderActionLayout.columnCount(for: 5), 3)
        XCTAssertEqual(RuneAddClusterProviderActionLayout.rowCount(for: 5), 2)
        XCTAssertEqual(RuneAddClusterProviderActionLayout.rowCount(for: 4), 2)
        XCTAssertEqual(RuneAddClusterProviderActionLayout.rowCount(for: 3), 1)
        XCTAssertEqual(RuneAddClusterProviderActionLayout.rowCount(for: 6), 2)
        XCTAssertEqual(RuneAddClusterProviderActionLayout.rowCount(for: 0), 0)
    }

    func testAddClusterProviderActionLayoutKeepsButtonsAboveMinimumReadableWidth() {
        let actionCounts = [3, 4, 5, 6]

        for actionCount in actionCounts {
            let columns = RuneAddClusterProviderActionLayout.columnCount(for: actionCount)
            let contentWidth = RuneAddClusterProviderActionLayout.contentWidth()
            let spacing = RuneAddClusterProviderActionLayout.columnSpacing * CGFloat(max(0, columns - 1))
            let buttonWidth = (contentWidth - spacing) / CGFloat(columns)

            XCTAssertGreaterThanOrEqual(
                buttonWidth,
                RuneAddClusterProviderActionLayout.minimumButtonWidth,
                "Add Cluster action count \(actionCount) should wrap before labels are compressed below the readable width."
            )
        }
    }

    func testAddClusterProviderCredentialGridUsesTwoReadableDefaultColumns() {
        let contentWidth = RuneAddClusterProviderActionLayout.contentWidth()
        let spacing = RuneUILayoutMetrics.dialogControlSpacing
        let twoColumnWidth = (contentWidth - spacing) / 2
        let threeColumnWidth = (contentWidth - spacing * 2) / 3

        XCTAssertEqual(RuneAddClusterProviderActionLayout.minimumCredentialFieldWidth, 220)
        XCTAssertGreaterThanOrEqual(
            twoColumnWidth,
            RuneAddClusterProviderActionLayout.minimumCredentialFieldWidth
        )
        XCTAssertLessThan(
            threeColumnWidth,
            RuneAddClusterProviderActionLayout.minimumCredentialFieldWidth
        )
    }

    func testAddClusterPopoverKeepsBoundedProfessionalDensity() {
        XCTAssertEqual(RuneUILayoutMetrics.addClusterPopoverWidth, 400)
        XCTAssertEqual(RuneUILayoutMetrics.addClusterPopoverPadding, 14)
        XCTAssertEqual(RuneUILayoutMetrics.addClusterActionCardMinHeight, 62)
        XCTAssertLessThanOrEqual(
            RuneUILayoutMetrics.addClusterPopoverMaxHeight,
            RuneWindowLayoutDefaults.minimumHeight - RuneUILayoutMetrics.dialogContentPadding * 2
        )
    }

    func testDialogMetricsRemainComfortableAtMinimumWindowSize() {
        XCTAssertEqual(RuneUILayoutMetrics.iconButtonSize, 28)
        XCTAssertEqual(RuneUILayoutMetrics.dialogIconButtonSize, RuneUILayoutMetrics.iconButtonSize)
        XCTAssertGreaterThanOrEqual(RuneUILayoutMetrics.dialogButtonMinHeight, 30)
        XCTAssertLessThan(RuneUILayoutMetrics.dialogButtonLabelMinHeight, RuneUILayoutMetrics.dialogButtonMinHeight)
        XCTAssertGreaterThanOrEqual(RuneUILayoutMetrics.dialogIconButtonSize, 28)
        XCTAssertGreaterThanOrEqual(RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth, 72)
        XCTAssertEqual(RuneUILayoutMetrics.dialogContentPadding, 20)
        XCTAssertEqual(RuneUILayoutMetrics.dialogSectionSpacing, 14)
        XCTAssertEqual(RuneUILayoutMetrics.dialogControlSpacing, 8)

        XCTAssertLessThanOrEqual(
            RuneUILayoutMetrics.wideDialogWidth,
            RuneWindowLayoutDefaults.minimumWidth - RuneUILayoutMetrics.dialogContentPadding * 2
        )
        XCTAssertLessThanOrEqual(
            RuneUILayoutMetrics.commandPaletteMaxWidth,
            RuneWindowLayoutDefaults.minimumWidth - RuneUILayoutMetrics.dialogContentPadding * 2
        )
        XCTAssertLessThanOrEqual(
            RuneUILayoutMetrics.commandPaletteMaxHeight,
            RuneWindowLayoutDefaults.minimumHeight - RuneUILayoutMetrics.dialogContentPadding * 2
        )
        XCTAssertLessThan(
            RuneUILayoutMetrics.commandPaletteMinWidth,
            RuneUILayoutMetrics.commandPaletteIdealWidth
        )
        XCTAssertLessThan(
            RuneUILayoutMetrics.commandPaletteIdealWidth,
            RuneUILayoutMetrics.commandPaletteMaxWidth
        )
        XCTAssertLessThan(
            RuneUILayoutMetrics.commandPaletteMinHeight,
            RuneUILayoutMetrics.commandPaletteIdealHeight
        )
        XCTAssertLessThan(
            RuneUILayoutMetrics.commandPaletteIdealHeight,
            RuneUILayoutMetrics.commandPaletteMaxHeight
        )
        XCTAssertLessThanOrEqual(
            RuneUILayoutMetrics.wideDialogHeight,
            RuneWindowLayoutDefaults.minimumHeight - RuneUILayoutMetrics.dialogContentPadding * 2
        )
        XCTAssertLessThanOrEqual(
            RuneUILayoutMetrics.addClusterPopoverMaxHeight,
            RuneWindowLayoutDefaults.minimumHeight - RuneUILayoutMetrics.dialogContentPadding * 2
        )
        XCTAssertLessThanOrEqual(
            RuneUILayoutMetrics.providerDialogMaxHeight,
            RuneWindowLayoutDefaults.minimumHeight - RuneUILayoutMetrics.dialogContentPadding * 2
        )
        XCTAssertLessThan(
            RuneUILayoutMetrics.providerDialogBodyMinHeight,
            RuneUILayoutMetrics.providerDialogBodyIdealHeight
        )
        XCTAssertLessThan(
            RuneUILayoutMetrics.providerDialogBodyIdealHeight,
            RuneUILayoutMetrics.providerDialogBodyMaxHeight
        )
    }

    @MainActor
    func testSharedDialogControlsRenderAtMinimumProfessionalHitSizes() {
        let labelHost = NSHostingView(rootView: RuneDialogButtonLabel("Import"))
        let closeHost = NSHostingView(rootView: RuneDialogCloseButton("Cancel kubeconfig import", action: {}))
        let buttonHost = NSHostingView(rootView: Button(action: {}) {
            RuneDialogButtonLabel("Import")
        }.buttonStyle(.bordered).controlSize(.regular))

        XCTAssertGreaterThanOrEqual(
            labelHost.fittingSize.height,
            RuneUILayoutMetrics.dialogButtonLabelMinHeight
        )
        XCTAssertGreaterThanOrEqual(
            labelHost.fittingSize.width,
            RuneUILayoutMetrics.dialogFooterButtonLabelMinWidth
        )
        XCTAssertGreaterThanOrEqual(
            closeHost.fittingSize.width,
            RuneUILayoutMetrics.dialogIconButtonSize
        )
        XCTAssertGreaterThanOrEqual(
            closeHost.fittingSize.height,
            RuneUILayoutMetrics.dialogIconButtonSize
        )
        XCTAssertGreaterThanOrEqual(
            buttonHost.fittingSize.height,
            RuneUILayoutMetrics.dialogButtonMinHeight
        )
        XCTAssertLessThanOrEqual(buttonHost.fittingSize.height, 32)
    }

    @MainActor
    func testKubeConfigImportReviewSheetFitsProfessionalDialogBounds() {
        let review = KubeConfigImportReview(
            contexts: [
                KubeConfigImportContextPreview(
                    name: "synthetic-context",
                    clusterName: "synthetic-cluster",
                    userName: "synthetic-user",
                    namespace: "synthetic-namespace",
                    serverHost: "synthetic.invalid",
                    authType: "exec",
                    providerHint: nil
                )
            ],
            issues: [],
            redactedPreview: "",
            sourceName: "synthetic-kubeconfig"
        )
        let host = NSHostingView(rootView: KubeConfigImportReviewSheet(
            review: review,
            duplicateHandlingChoice: .constant(.skipDuplicate),
            metadataDrafts: [:],
            isCommitInProgress: false,
            canConfirm: true,
            onUpdateMetadata: { _, _ in },
            onConfirm: {},
            onCancel: {}
        ))

        XCTAssertGreaterThanOrEqual(host.fittingSize.width, 520)
        XCTAssertLessThanOrEqual(host.fittingSize.width, 720)
        XCTAssertGreaterThanOrEqual(host.fittingSize.height, 360)
        XCTAssertLessThanOrEqual(host.fittingSize.height, 720)
    }

    @MainActor
    func testKubeConfigImportReviewKeepsHeaderAndActionsOutsideItsOnlyScrollViewport() throws {
        let contexts = (0..<12).map { index in
            KubeConfigImportContextPreview(
                name: "synthetic-context-\(index)",
                clusterName: "synthetic-cluster-\(index)",
                userName: "synthetic-user-\(index)",
                namespace: "synthetic-namespace",
                serverHost: "synthetic.invalid",
                authType: "exec",
                providerHint: "Synthetic provider"
            )
        }
        let issues = (0..<8).map { index in
            KubeConfigImportIssue(
                id: "synthetic-warning-\(index)",
                severity: .warning,
                message: "Synthetic import warning \(index) with enough detail to exercise compact wrapping."
            )
        }
        let review = KubeConfigImportReview(
            contexts: contexts,
            issues: issues,
            redactedPreview: (0..<80).map { "synthetic-redacted-line-\($0)" }.joined(separator: "\n"),
            sourceName: "synthetic-kubeconfig",
            hasDuplicateConflicts: true
        )
        var latestSnapshot: KubeConfigImportReviewLayoutSnapshot?
        let host = NSHostingView(rootView: KubeConfigImportReviewSheet(
            review: review,
            duplicateHandlingChoice: .constant(.skipDuplicate),
            metadataDrafts: [:],
            isCommitInProgress: false,
            canConfirm: true,
            onUpdateMetadata: { _, _ in },
            onConfirm: {},
            onCancel: {},
            onLayoutSnapshotChange: { latestSnapshot = $0 }
        ))
        host.frame = NSRect(x: 0, y: 0, width: 520, height: 360)

        settle(host, until: { latestSnapshot != nil })

        let snapshot = try XCTUnwrap(latestSnapshot)
        let header = try XCTUnwrap(snapshot[.header])
        let contentViewport = try XCTUnwrap(snapshot[.contentViewport])
        let actions = try XCTUnwrap(snapshot[.actions])

        XCTAssertGreaterThan(header.height, 0)
        XCTAssertGreaterThan(contentViewport.height, 0)
        XCTAssertGreaterThanOrEqual(actions.height, RuneUILayoutMetrics.dialogButtonMinHeight)
        XCTAssertLessThanOrEqual(header.maxY, contentViewport.minY + 0.5)
        XCTAssertLessThanOrEqual(contentViewport.maxY, actions.minY + 0.5)
        XCTAssertGreaterThanOrEqual(header.minY, -0.5)
        XCTAssertLessThanOrEqual(
            actions.maxY,
            360 - 32 + 0.5,
            "The fixed action bar must remain inside the sheet's 16pt vertical insets at minimum height."
        )
        XCTAssertEqual(
            scrollViews(in: host).count,
            1,
            "The preflight sheet should have one vertical owner: its central review viewport."
        )
    }

    func testCloudCredentialDraftRequiredFieldsGateProviderRunAction() {
        XCTAssertFalse(CloudCredentialDraft().hasRequiredFields(for: .aks))
        XCTAssertFalse(CloudCredentialDraft(clusterName: "synthetic-aks").hasRequiredFields(for: .aks))
        XCTAssertTrue(CloudCredentialDraft(clusterName: "synthetic-aks", resourceGroup: "synthetic-group").hasRequiredFields(for: .aks))
        XCTAssertFalse(CloudCredentialDraft(clusterName: "  ", resourceGroup: "\n").hasRequiredFields(for: .aks))

        XCTAssertFalse(CloudCredentialDraft(clusterName: "synthetic-eks").hasRequiredFields(for: .eks))
        XCTAssertTrue(CloudCredentialDraft(clusterName: "synthetic-eks", regionOrLocation: "eu-north-1").hasRequiredFields(for: .eks))
        XCTAssertFalse(CloudCredentialDraft(clusterName: "\t", regionOrLocation: "  ").hasRequiredFields(for: .eks))

        XCTAssertFalse(CloudCredentialDraft(clusterName: "synthetic-gke", regionOrLocation: "europe-north1").hasRequiredFields(for: .gke))
        XCTAssertFalse(CloudCredentialDraft(
            clusterName: "synthetic-gke",
            regionOrLocation: "europe-north1",
            projectID: "  "
        ).hasRequiredFields(for: .gke))
        XCTAssertTrue(CloudCredentialDraft(
            clusterName: "synthetic-gke",
            regionOrLocation: "europe-north1",
            projectID: "synthetic-project"
        ).hasRequiredFields(for: .gke))
    }

    func testCloudCredentialDraftBuildsTrimmedProviderRequest() {
        let request = CloudCredentialDraft(
            clusterName: " synthetic-cluster ",
            regionOrLocation: "\teu-north-1\n",
            resourceGroup: " synthetic-group ",
            projectID: " synthetic-project ",
            profileOrSubscription: " synthetic-profile ",
            roleARN: " synthetic-role "
        ).request(provider: .eks)

        XCTAssertEqual(request.clusterName, "synthetic-cluster")
        XCTAssertEqual(request.regionOrLocation, "eu-north-1")
        XCTAssertEqual(request.resourceGroup, "synthetic-group")
        XCTAssertEqual(request.projectID, "synthetic-project")
        XCTAssertEqual(request.profileOrSubscription, "synthetic-profile")
        XCTAssertEqual(request.roleARN, "synthetic-role")
    }

    func testCloudCredentialDraftSummarizesMissingRequiredFields() {
        XCTAssertEqual(
            CloudCredentialDraft().missingRequiredFieldSummary(for: .aks),
            "cluster name, resource group"
        )
        XCTAssertEqual(
            CloudCredentialDraft(clusterName: "synthetic-eks").missingRequiredFieldSummary(for: .eks),
            "region"
        )
        XCTAssertEqual(
            CloudCredentialDraft(clusterName: "synthetic-gke", regionOrLocation: "  ").missingRequiredFieldSummary(for: .gke),
            "location, project ID"
        )
        XCTAssertNil(CloudCredentialDraft(
            clusterName: "synthetic-gke",
            regionOrLocation: "europe-north1",
            projectID: "synthetic-project"
        ).missingRequiredFieldSummary(for: .gke))
    }

    @MainActor
    private func scrollViews(in view: NSView) -> [NSScrollView] {
        var result = view is NSScrollView ? [view as! NSScrollView] : []
        for subview in view.subviews {
            result.append(contentsOf: scrollViews(in: subview))
        }
        return result
    }

    @MainActor
    private func settle(
        _ host: NSView,
        until condition: () -> Bool
    ) {
        for _ in 0..<50 {
            host.layoutSubtreeIfNeeded()
            if condition() { return }
            RunLoop.main.run(until: Date().addingTimeInterval(0.005))
        }
    }
}
