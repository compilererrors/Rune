import SwiftUI

/// Keeps the provider heading and actions visible while forms, diagnostics and
/// command output share one vertically scrolling body.
struct AddClusterProviderSheetLayout<Header: View, Content: View, Actions: View>: View {
    var width: CGFloat = RuneAddClusterProviderActionLayout.dialogWidth
    var bodyMinimumHeight: CGFloat = RuneUILayoutMetrics.providerDialogBodyMinHeight
    var bodyIdealHeight: CGFloat = RuneUILayoutMetrics.providerDialogBodyIdealHeight
    var noticeToken: String? = nil
    @ViewBuilder var header: () -> Header
    @ViewBuilder var content: () -> Content
    @ViewBuilder var actions: () -> Actions

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            header()
                .padding(.horizontal, RuneUILayoutMetrics.dialogContentPadding)
                .padding(.top, RuneUILayoutMetrics.dialogContentPadding)
                .padding(.bottom, RuneUILayoutMetrics.dialogSectionSpacing)

            Divider()

            ScrollViewReader { proxy in
                ScrollView {
                    content()
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .padding(.horizontal, RuneUILayoutMetrics.dialogContentPadding)
                        .padding(.vertical, RuneUILayoutMetrics.dialogSectionSpacing)
                        .id("rune.add-cluster.provider.body-top")
                }
                .onChange(of: noticeToken) { _, notice in
                    guard notice != nil else { return }
                    proxy.scrollTo("rune.add-cluster.provider.body-top", anchor: .top)
                }
            }
            .frame(
                minHeight: bodyMinimumHeight,
                idealHeight: bodyIdealHeight,
                maxHeight: RuneUILayoutMetrics.providerDialogBodyMaxHeight
            )
            .accessibilityIdentifier("rune.add-cluster.provider.scroll-content")

            RuneDialogActionBar(actions: actions)
                .padding(.horizontal, RuneUILayoutMetrics.dialogContentPadding)
                .padding(.bottom, RuneUILayoutMetrics.dialogContentPadding)
                .fixedSize(horizontal: false, vertical: true)
                .accessibilityIdentifier("rune.add-cluster.provider.actions")
        }
        .frame(width: width)
        .frame(maxHeight: RuneUILayoutMetrics.providerDialogMaxHeight)
    }
}
