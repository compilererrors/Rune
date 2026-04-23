import AppKit
import SwiftUI

struct DescribeReadOnlyTextView: NSViewRepresentable {
    let text: String
    var resetScrollOnExternalChange = true

    final class Coordinator {
        var previousText: String

        init(text: String) {
            previousText = text
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(text: text)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = NSScrollView(frame: .zero)
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = false
        scrollView.autohidesScrollers = true
        scrollView.borderType = .noBorder
        scrollView.drawsBackground = false
        scrollView.automaticallyAdjustsContentInsets = false
        scrollView.scrollerInsets = NSEdgeInsets()
        scrollView.contentInsets = NSEdgeInsets()
        scrollView.horizontalScrollElasticity = .none
        scrollView.verticalScrollElasticity = .allowed

        let textView = NSTextView(frame: .zero)
        textView.isEditable = false
        textView.isSelectable = true
        textView.isRichText = false
        textView.importsGraphics = false
        textView.usesFindBar = true
        textView.usesFontPanel = false
        textView.allowsUndo = false
        textView.isAutomaticQuoteSubstitutionEnabled = false
        textView.isAutomaticDashSubstitutionEnabled = false
        textView.isAutomaticDataDetectionEnabled = false
        textView.isAutomaticLinkDetectionEnabled = false
        textView.isAutomaticSpellingCorrectionEnabled = false
        textView.isAutomaticTextCompletionEnabled = false
        textView.isGrammarCheckingEnabled = false
        textView.isContinuousSpellCheckingEnabled = false
        textView.isHorizontallyResizable = false
        textView.isVerticallyResizable = true
        textView.minSize = .zero
        textView.maxSize = NSSize(width: CGFloat.greatestFiniteMagnitude, height: CGFloat.greatestFiniteMagnitude)
        textView.autoresizingMask = [.width]
        textView.textContainerInset = NSSize(width: 10, height: 10)
        textView.backgroundColor = .clear
        textView.drawsBackground = false
        textView.font = NSFont.monospacedSystemFont(ofSize: 12, weight: .regular)
        textView.textColor = .labelColor

        if let container = textView.textContainer {
            container.widthTracksTextView = true
            container.heightTracksTextView = false
            container.containerSize = NSSize(width: 0, height: CGFloat.greatestFiniteMagnitude)
            container.lineFragmentPadding = 0
        }

        textView.string = text
        scrollView.documentView = textView
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        guard let textView = scrollView.documentView as? NSTextView else { return }

        if let container = textView.textContainer {
            let contentWidth = max(0, scrollView.contentSize.width - textView.textContainerInset.width * 2)
            if abs(container.containerSize.width - contentWidth) > 1 {
                container.containerSize = NSSize(width: contentWidth, height: CGFloat.greatestFiniteMagnitude)
            }
        }

        if textView.string != text {
            textView.string = text
            context.coordinator.previousText = text
            textView.layoutManager?.ensureLayout(for: textView.textContainer!)
            textView.invalidateIntrinsicContentSize()
            textView.layoutSubtreeIfNeeded()
            if resetScrollOnExternalChange {
                scrollView.contentView.scroll(to: .zero)
                scrollView.reflectScrolledClipView(scrollView.contentView)
            }
        } else {
            textView.layoutManager?.ensureLayout(for: textView.textContainer!)
        }
    }
}

struct DescribeTextSurface: View {
    let text: String
    let minHeight: CGFloat
    let resetID: String

    var body: some View {
        InspectorTextSurface(minHeight: minHeight) {
            DescribeReadOnlyTextView(text: text)
                .id(resetID)
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
    }
}
