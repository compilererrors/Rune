import AppKit
import SwiftUI
import RuneCore

struct AppKitManifestTextView: NSViewRepresentable {
    enum ContentStyle: Sendable {
        case yaml
        case plainText
    }

    @Binding var text: String
    var isEditable: Bool
    var resetScrollOnExternalChange = false
    var contentStyle: ContentStyle = .plainText
    var externalValidationIssues: [YAMLValidationIssue] = []

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
        textView.configure(
            isEditable: isEditable,
            contentStyle: contentStyle,
            externalValidationIssues: externalValidationIssues
        )
        textView.delegate = context.coordinator
        textView.setStringKeepingSelection(text)

        scrollView.documentView = textView
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self

        guard let textView = scrollView.documentView as? PlainManifestTextView else { return }
        textView.configure(
            isEditable: isEditable,
            contentStyle: contentStyle,
            externalValidationIssues: externalValidationIssues
        )

        if textView.string != text {
            context.coordinator.isUpdatingFromSwiftUI = true
            textView.setStringKeepingSelection(text)
            context.coordinator.isUpdatingFromSwiftUI = false

            if resetScrollOnExternalChange {
                textView.scrollRangeToVisible(NSRange(location: 0, length: 0))
            }
        }

        // Keep scroll/document geometry in sync even when the text itself is unchanged.
        textView.refreshViewportGeometry()
    }
}

private final class PlainManifestTextView: NSTextView {
    private static let baseFont = NSFont.monospacedSystemFont(ofSize: 12, weight: .regular)

    private var contentStyle: AppKitManifestTextView.ContentStyle = .plainText
    private var externalValidationIssues: [YAMLValidationIssue] = []

    override var isOpaque: Bool { false }

    override func drawBackground(in rect: NSRect) {
        NSColor.clear.setFill()
        rect.fill()
        guard contentStyle == .yaml else { return }
        drawIndentGuides(in: rect)
        drawTabMarkers(in: rect)
    }

    func configure(
        isEditable: Bool,
        contentStyle: AppKitManifestTextView.ContentStyle,
        externalValidationIssues: [YAMLValidationIssue]
    ) {
        let styleChanged = self.contentStyle != contentStyle
        let issuesChanged = self.externalValidationIssues != externalValidationIssues
        self.isEditable = isEditable
        self.contentStyle = contentStyle
        self.externalValidationIssues = externalValidationIssues
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

        if styleChanged || issuesChanged {
            refreshLayout()
        } else {
            refreshViewportGeometry()
        }
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

        let fullRange = NSRange(location: 0, length: storage.length)
        storage.beginEditing()
        storage.setAttributes([
            .font: Self.baseFont,
            .foregroundColor: NSColor.labelColor
        ], range: fullRange)

        if contentStyle == .yaml, storage.length > 0 {
            let analysis = YAMLLanguageService.analyze(storage.string)
            applyYAMLHighlighting(in: storage, fullRange: fullRange, analysis: analysis)
            applyYAMLDiagnostics(
                in: storage,
                issues: analysis.validationIssues + externalValidationIssues.filter { $0.source != .syntax }
            )
        }
        storage.endEditing()

        updateDocumentSize()
        invalidateIntrinsicContentSize()
        needsDisplay = true
    }

    func refreshViewportGeometry() {
        updateDocumentSize()
        invalidateIntrinsicContentSize()
        needsDisplay = true
    }

    override func didChangeText() {
        super.didChangeText()
        refreshLayout()
    }

    override func insertNewline(_ sender: Any?) {
        guard isEditable, contentStyle == .yaml else {
            super.insertNewline(sender)
            return
        }

        let nsString = string as NSString
        let selection = selectedRange()
        guard selection.length == 0 else {
            super.insertNewline(sender)
            return
        }

        let lineRange = nsString.lineRange(for: selection)
        let currentLine = nsString.substring(with: trimmedLineRange(lineRange, in: nsString))
        let indentation = YAMLLanguageService.suggestedIndentation(after: currentLine)
        insertText("\n" + indentation, replacementRange: selection)
    }

    override func viewDidEndLiveResize() {
        super.viewDidEndLiveResize()
        updateDocumentSize()
    }

    override func layout() {
        super.layout()
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

    private func applyYAMLHighlighting(in storage: NSTextStorage, fullRange: NSRange, analysis: YAMLTextAnalysis) {
        for span in analysis.highlights where NSMaxRange(span.range) <= NSMaxRange(fullRange) {
            storage.addAttributes(
                [.foregroundColor: ManifestPalette.color(for: span.kind)],
                range: span.range
            )
        }
    }

    private func applyYAMLDiagnostics(in storage: NSTextStorage, issues: [YAMLValidationIssue]) {
        for issue in issues {
            guard let range = issue.range?.nsRange else { continue }
            storage.addAttributes(issue.attributes, range: range)
        }
    }

    private func drawIndentGuides(in rect: NSRect) {
        guard let layoutManager, let textContainer else { return }
        guard let visibleRange = visibleCharacterRange else { return }

        let guideColor = ManifestPalette.indentGuide
        let path = NSBezierPath()
        let indentWidth = widthOfSingleSpace()
        let insetX = textContainerInset.width
        let nsString = string as NSString

        nsString.enumerateSubstrings(in: visibleRange, options: [.byLines, .substringNotRequired]) { _, substringRange, _, _ in
            let glyphRange = layoutManager.glyphRange(forCharacterRange: substringRange, actualCharacterRange: nil)
            let usedRect = layoutManager.boundingRect(forGlyphRange: glyphRange, in: textContainer)
            let line = nsString.substring(with: substringRange)
            let leadingColumns = indentationColumns(in: line)
            guard leadingColumns >= 2 else { return }

            let levelCount = leadingColumns / 2
            for level in 1...levelCount {
                let x = insetX + CGFloat(level) * indentWidth - indentWidth / 2
                path.move(to: NSPoint(x: x, y: usedRect.minY + 1))
                path.line(to: NSPoint(x: x, y: usedRect.maxY - 1))
            }
        }

        guideColor.setStroke()
        path.lineWidth = 1
        path.stroke()
    }

    private func drawTabMarkers(in rect: NSRect) {
        guard let layoutManager, let textContainer else { return }
        guard let visibleRange = visibleCharacterRange else { return }

        let nsString = string as NSString
        let attributes: [NSAttributedString.Key: Any] = [
            .font: Self.baseFont,
            .foregroundColor: ManifestPalette.tabMarker
        ]
        let marker = NSAttributedString(string: "\u{21E5}", attributes: attributes)

        for index in visibleRange.location..<(visibleRange.location + visibleRange.length) {
            guard nsString.character(at: index) == 9 else { continue }
            let glyphRange = layoutManager.glyphRange(forCharacterRange: NSRange(location: index, length: 1), actualCharacterRange: nil)
            let glyphRect = layoutManager.boundingRect(forGlyphRange: glyphRange, in: textContainer)
            marker.draw(at: NSPoint(x: glyphRect.minX + 1, y: glyphRect.minY))
        }
    }

    private var visibleCharacterRange: NSRange? {
        guard let layoutManager, let textContainer, let scrollView = enclosingScrollView else { return nil }
        let visibleRect = scrollView.contentView.bounds
        let glyphRange = layoutManager.glyphRange(forBoundingRect: visibleRect, in: textContainer)
        return layoutManager.characterRange(forGlyphRange: glyphRange, actualGlyphRange: nil)
    }

    private func widthOfSingleSpace() -> CGFloat {
        let sample = " " as NSString
        let size = sample.size(withAttributes: [.font: Self.baseFont])
        return max(8, ceil(size.width))
    }

    private func trimmedLineRange(_ lineRange: NSRange, in nsString: NSString) -> NSRange {
        var length = lineRange.length
        while length > 0 {
            let character = nsString.character(at: lineRange.location + length - 1)
            if character == 10 || character == 13 {
                length -= 1
            } else {
                break
            }
        }
        return NSRange(location: lineRange.location, length: length)
    }
}

private struct ManifestPalette {
    static let key = NSColor.systemBlue
    static let string = NSColor.systemGreen
    static let number = NSColor.systemOrange
    static let boolean = NSColor.systemPurple
    static let comment = NSColor.secondaryLabelColor
    static let directive = NSColor.systemPink
    static let anchor = NSColor.systemTeal
    static let indentGuide = NSColor.separatorColor.withAlphaComponent(0.28)
    static let tabMarker = NSColor.systemRed.withAlphaComponent(0.85)
    static let errorUnderline = NSColor.systemRed
    static let errorBackground = NSColor.systemRed.withAlphaComponent(0.12)
    static let warningUnderline = NSColor.systemOrange
    static let warningBackground = NSColor.systemOrange.withAlphaComponent(0.1)

    static func color(for kind: YAMLHighlightKind) -> NSColor {
        switch kind {
        case .key:
            return key
        case .string:
            return string
        case .number:
            return number
        case .boolean:
            return boolean
        case .comment:
            return comment
        case .directive:
            return directive
        case .anchor, .alias:
            return anchor
        }
    }
}

extension YAMLValidationIssue {
    var attributes: [NSAttributedString.Key: Any] {
        switch severity {
        case .error:
            return [
                .underlineStyle: NSUnderlineStyle.single.rawValue,
                .underlineColor: ManifestPalette.errorUnderline,
                .backgroundColor: ManifestPalette.errorBackground
            ]
        case .warning:
            return [
                .underlineStyle: NSUnderlineStyle.single.rawValue,
                .underlineColor: ManifestPalette.warningUnderline,
                .backgroundColor: ManifestPalette.warningBackground
            ]
        }
    }
}

private func indentationColumns(in line: String) -> Int {
    var columns = 0
    for character in line {
        switch character {
        case " ":
            columns += 1
        case "\t":
            columns += 2
        default:
            return columns
        }
    }
    return columns
}
