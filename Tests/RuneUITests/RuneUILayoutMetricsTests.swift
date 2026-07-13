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

    func testDialogMetricsRemainComfortableAtMinimumWindowSize() {
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
        let labelHost = NSHostingView(rootView: RuneDialogButtonLabel("Close"))
        let closeHost = NSHostingView(rootView: RuneDialogCloseButton(action: {}))
        let buttonHost = NSHostingView(rootView: Button(action: {}) {
            RuneDialogButtonLabel("Close")
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
}
