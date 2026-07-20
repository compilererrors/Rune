import RuneSecurity
import SwiftUI

struct KubeConfigImportReviewSheet: View {
    let review: KubeConfigImportReview
    @Binding var duplicateHandlingChoice: KubeConfigDuplicateHandlingChoice
    let metadataDrafts: [String: ContextDisplayMetadata]
    let isCommitInProgress: Bool
    let canConfirm: Bool
    let onUpdateMetadata: (String, ContextDisplayMetadata) -> Void
    let onConfirm: () -> Void
    let onCancel: () -> Void
    let onLayoutSnapshotChange: ((KubeConfigImportReviewLayoutSnapshot) -> Void)?

    init(
        review: KubeConfigImportReview,
        duplicateHandlingChoice: Binding<KubeConfigDuplicateHandlingChoice>,
        metadataDrafts: [String: ContextDisplayMetadata],
        isCommitInProgress: Bool,
        canConfirm: Bool,
        onUpdateMetadata: @escaping (String, ContextDisplayMetadata) -> Void,
        onConfirm: @escaping () -> Void,
        onCancel: @escaping () -> Void,
        onLayoutSnapshotChange: ((KubeConfigImportReviewLayoutSnapshot) -> Void)? = nil
    ) {
        self.review = review
        _duplicateHandlingChoice = duplicateHandlingChoice
        self.metadataDrafts = metadataDrafts
        self.isCommitInProgress = isCommitInProgress
        self.canConfirm = canConfirm
        self.onUpdateMetadata = onUpdateMetadata
        self.onConfirm = onConfirm
        self.onCancel = onCancel
        self.onLayoutSnapshotChange = onLayoutSnapshotChange
    }

    var body: some View {
        KubeConfigImportReviewPanel(
            review: review,
            duplicateHandlingChoice: $duplicateHandlingChoice,
            metadataDrafts: metadataDrafts,
            onUpdateMetadata: onUpdateMetadata,
            reviewMode: .preflight,
            isConfirmationPending: true,
            canConfirm: canConfirm,
            isCommitInProgress: isCommitInProgress,
            onConfirm: onConfirm,
            onCancel: onCancel,
            onClear: onCancel,
            showsAuthDoctorAction: false,
            onRunAuthDoctor: {},
            onLayoutSnapshotChange: onLayoutSnapshotChange
        )
        .padding(16)
        .frame(minWidth: 520, idealWidth: 620, maxWidth: 720)
        .frame(minHeight: 360, idealHeight: 520, maxHeight: 720, alignment: .top)
        .interactiveDismissDisabled(isCommitInProgress)
    }
}
