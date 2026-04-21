import AppKit
import SwiftUI

struct AppKitManifestTextView: NSViewRepresentable {
    @Binding var text: String
    var isEditable: Bool

    final class Coordinator: NSObject, NSTextViewDelegate {
        var parent: AppKitManifestTextView
        var isUpdatingFromSwiftUI = false

        init(parent: AppKitManifestTextView) {
            self.parent = parent
        }

        func textDidChange(_ notification: Notification) {
            guard !isUpdatingFromSwiftUI,
                  let textView = notification.object as? PlainManifestTextView
            else { return }

            textView.refreshLayout()
            parent.text = textView.string
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(parent: self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = NSScrollView(frame: .zero)
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.borderType = .noBorder
        scrollView.drawsBackground = false
        scrollView.automaticallyAdjustsContentInsets = false
        scrollView.scrollerInsets = NSEdgeInsets()
        scrollView.contentInsets = NSEdgeInsets()
        scrollView.horizontalScrollElasticity = .allowed
        scrollView.verticalScrollElasticity = .allowed

        let textView = PlainManifestTextView(frame: .zero)
        textView.configure(isEditable: isEditable)
        textView.delegate = context.coordinator
        textView.setStringKeepingSelection(text)

        scrollView.documentView = textView
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self

        guard let textView = scrollView.documentView as? PlainManifestTextView else { return }
        textView.configure(isEditable: isEditable)

        if textView.string != text {
            context.coordinator.isUpdatingFromSwiftUI = true
            textView.setStringKeepingSelection(text)
            context.coordinator.isUpdatingFromSwiftUI = false
        }
    }
}

private final class PlainManifestTextView: NSTextView {
    private static let baseFont = NSFont.monospacedSystemFont(ofSize: 12, weight: .regular)

    override var isOpaque: Bool { false }

    func configure(isEditable: Bool) {
        self.isEditable = isEditable
        isSelectable = true
        isRichText = false
        importsGraphics = false
        usesFindBar = true
        usesFontPanel = false
        allowsUndo = true
        isAutomaticQuoteSubstitutionEnabled = false
        isAutomaticDashSubstitutionEnabled = false
        isAutomaticDataDetectionEnabled = false
        isAutomaticLinkDetectionEnabled = false
        isAutomaticSpellingCorrectionEnabled = false
        isAutomaticTextCompletionEnabled = false
        isGrammarCheckingEnabled = false
        isContinuousSpellCheckingEnabled = false
        isHorizontallyResizable = true
        isVerticallyResizable = true
        minSize = .zero
        maxSize = NSSize(width: CGFloat.greatestFiniteMagnitude, height: CGFloat.greatestFiniteMagnitude)
        autoresizingMask = [.height]
        textContainerInset = NSSize(width: 10, height: 10)
        backgroundColor = .clear
        drawsBackground = false
        insertionPointColor = .controlAccentColor
        selectedTextAttributes = [
            .backgroundColor: NSColor.controlAccentColor.withAlphaComponent(0.22)
        ]
        font = Self.baseFont
        textColor = .labelColor
        typingAttributes = [
            .font: Self.baseFont,
            .foregroundColor: NSColor.labelColor
        ]

        if let container = textContainer {
            container.widthTracksTextView = false
            container.heightTracksTextView = false
            container.containerSize = NSSize(
                width: CGFloat.greatestFiniteMagnitude,
                height: CGFloat.greatestFiniteMagnitude
            )
            container.lineFragmentPadding = 0
        }

        refreshLayout()
    }

    func setStringKeepingSelection(_ newValue: String) {
        let selected = selectedRanges
        string = newValue
        refreshLayout()
        if !selected.isEmpty {
            selectedRanges = selected
        }
    }

    func refreshLayout() {
        guard let storage = textStorage else { return }

        storage.beginEditing()
        storage.setAttributes([
            .font: Self.baseFont,
            .foregroundColor: NSColor.labelColor
        ], range: NSRange(location: 0, length: storage.length))
        storage.endEditing()

        updateDocumentSize()
    }

    override func didChangeText() {
        super.didChangeText()
        refreshLayout()
    }

    override func viewDidEndLiveResize() {
        super.viewDidEndLiveResize()
        updateDocumentSize()
    }

    private func updateDocumentSize() {
        guard let layoutManager, let textContainer else { return }
        layoutManager.ensureLayout(for: textContainer)

        let usedRect = layoutManager.usedRect(for: textContainer)
        let visibleSize = enclosingScrollView?.contentSize ?? bounds.size
        let targetWidth = max(
            visibleSize.width,
            ceil(usedRect.width + textContainerInset.width * 2 + 40)
        )
        let targetHeight = max(
            visibleSize.height,
            ceil(usedRect.height + textContainerInset.height * 2 + 24)
        )

        if abs(frame.width - targetWidth) > 1 || abs(frame.height - targetHeight) > 1 {
            frame.size = NSSize(width: targetWidth, height: targetHeight)
        }
    }
}
