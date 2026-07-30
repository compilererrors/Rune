import AppKit
import SwiftUI
import struct RuneSharedCore.RuneLargeTextIndex
import struct RuneSharedUI.RuneLargeTextSurface
import RuneCore

struct InspectorTextSurface<Content: View>: View {
    let minHeight: CGFloat
    @ViewBuilder var content: () -> Content

    var body: some View {
        GeometryReader { proxy in
            content()
                .frame(
                    width: max(1, proxy.size.width),
                    height: max(1, proxy.size.height),
                    alignment: .topLeading
                )
                .background {
                    RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                        .fill(.thinMaterial)
                }
                .overlay {
                    RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                        .strokeBorder(Color(nsColor: .separatorColor).opacity(0.24), lineWidth: 1)
                }
                .clipShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
        }
        .frame(minHeight: minHeight, maxHeight: .infinity, alignment: .topLeading)
    }
}

struct InspectorReadOnlyTextView: View {
    let text: String
    let resetID: String
    var resetScrollOnExternalChange = true
    var contentStyle: AppKitManifestTextView.ContentStyle = .plainText
    var externalValidationIssues: [YAMLValidationIssue] = []
    var navigationRequest: YAMLTextNavigationRequest?
    var usesLargeTextSurface = false
    var allowsAutomaticLargeTextSurface = true
    var largeTextIndex: RuneLargeTextIndex?
    var largeTextScrollTargetLine: Int?
    var largeTextScrollTargetRevision: Int?
    var largeTextScrollsOnTargetLineChange = true
    var largeTextShowsLineNumbers = false
    var showsLineNumbers = false
    var searchQuery = ""
    var searchMatchCase = false
    var selectedSearchMatchIndex = 0
    var searchIndex: InspectorFindIndex?
    var searchMatchRanges: [NSRange]?
    var searchNavigationRevision = 0
    @AppStorage(RuneSettingsKeys.terminalFontSize) private var appFontSize = RuneSettingsKeys.terminalFontSizeDefault

    private var shouldUseLargeTextSurface: Bool {
        usesLargeTextSurface || (allowsAutomaticLargeTextSurface && text.utf8.count > 250_000)
    }

    var body: some View {
        let resolvedSearchRanges = searchMatchRanges ?? searchIndex?.ranges
        let searchTargetLine = searchIndex?.matchLineNumber(selectedIndex: selectedSearchMatchIndex)
        let resolvedLargeTextIndex = largeTextIndex ?? searchIndex?.textIndex

        Group {
            if shouldUseLargeTextSurface {
                if let resolvedLargeTextIndex {
                    RuneLargeTextSurface(
                        index: resolvedLargeTextIndex,
                        placeholder: "No output",
                        scrollTargetLine: largeTextScrollTargetLine ?? searchTargetLine ?? navigationRequest?.line,
                        scrollTargetRevision: largeTextScrollTargetRevision
                            ?? (searchTargetLine == nil ? navigationRequest?.sequence : searchNavigationRevision),
                        scrollsOnTargetLineChange: largeTextScrollsOnTargetLineChange,
                        searchMatchRanges: resolvedSearchRanges ?? [],
                        selectedSearchMatchIndex: selectedSearchMatchIndex,
                        horizontalContentInset: RuneUILayoutMetrics.inspectorDocumentHorizontalInset,
                        verticalContentInset: RuneUILayoutMetrics.inspectorDocumentVerticalInset,
                        showsLineNumbers: largeTextShowsLineNumbers,
                        fontSize: CGFloat(RuneSettingsKeys.clampedTerminalFontSize(appFontSize))
                    )
                } else if searchMatchRanges != nil {
                    ProgressView("Preparing text…")
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .center)
                } else {
                    RuneLargeTextSurface(
                        text: text,
                        placeholder: "No output",
                        scrollTargetLine: largeTextScrollTargetLine ?? navigationRequest?.line,
                        scrollTargetRevision: largeTextScrollTargetRevision ?? navigationRequest?.sequence,
                        scrollsOnTargetLineChange: largeTextScrollsOnTargetLineChange,
                        searchMatchRanges: resolvedSearchRanges ?? [],
                        selectedSearchMatchIndex: selectedSearchMatchIndex,
                        horizontalContentInset: RuneUILayoutMetrics.inspectorDocumentHorizontalInset,
                        verticalContentInset: RuneUILayoutMetrics.inspectorDocumentVerticalInset,
                        showsLineNumbers: largeTextShowsLineNumbers,
                        fontSize: CGFloat(RuneSettingsKeys.clampedTerminalFontSize(appFontSize))
                    )
                }
            } else {
                AppKitManifestTextView(
                    text: .constant(text),
                    isEditable: false,
                    resetScrollOnExternalChange: resetScrollOnExternalChange,
                    contentStyle: contentStyle,
                    externalValidationIssues: externalValidationIssues,
                    navigationRequest: navigationRequest,
                    showsLineNumbers: showsLineNumbers,
                    searchQuery: searchQuery,
                    searchMatchCase: searchMatchCase,
                    selectedSearchMatchIndex: selectedSearchMatchIndex,
                    searchMatchRanges: resolvedSearchRanges,
                    searchNavigationRevision: searchNavigationRevision
                )
            }
        }
        .id(resetID)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
}

struct InspectorReadOnlyTextSurface: View {
    let text: String
    let minHeight: CGFloat
    let resetID: String
    var contentStyle: AppKitManifestTextView.ContentStyle = .plainText

    var body: some View {
        InspectorTextSurface(minHeight: minHeight) {
            InspectorReadOnlyTextView(
                text: text,
                resetID: resetID,
                contentStyle: contentStyle
            )
        }
    }
}
