import AppKit
import SwiftUI
import RuneCore

struct InspectorTextSurface<Content: View>: View {
    let minHeight: CGFloat
    @ViewBuilder var content: () -> Content

    var body: some View {
        content()
            .frame(minWidth: 0, maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .background {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                    .fill(.thinMaterial)
            }
            .overlay {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                    .strokeBorder(Color(nsColor: .separatorColor).opacity(0.24), lineWidth: 1)
            }
            .clipShape(RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous))
            .frame(minHeight: minHeight, maxHeight: .infinity, alignment: .topLeading)
    }
}

struct InspectorReadOnlyTextView: View {
    let text: String
    let resetID: String
    var contentStyle: AppKitManifestTextView.ContentStyle = .plainText
    var externalValidationIssues: [YAMLValidationIssue] = []

    var body: some View {
        AppKitManifestTextView(
            text: .constant(text),
            isEditable: false,
            resetScrollOnExternalChange: true,
            contentStyle: contentStyle,
            externalValidationIssues: externalValidationIssues
        )
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

struct InspectorPlainTextScrollSurface: View {
    let text: String
    let minHeight: CGFloat
    let resetID: String

    var body: some View {
        InspectorTextSurface(minHeight: minHeight) {
            GeometryReader { proxy in
                ScrollView([.vertical, .horizontal]) {
                    Text(text)
                        .font(.system(size: 12, weight: .regular, design: .monospaced))
                        .textSelection(.enabled)
                        .fixedSize(horizontal: false, vertical: true)
                        .frame(
                            minWidth: max(0, proxy.size.width - 20),
                            minHeight: max(0, proxy.size.height - 20),
                            alignment: .topLeading
                        )
                        .padding(10)
                }
                .id("\(resetID):\(text.count)")
            }
        }
    }
}
