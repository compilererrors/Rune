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
    var navigationRequest: YAMLTextNavigationRequest?

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
        textView.navigateIfNeeded(navigationRequest)
    }
}

private final class PlainManifestTextView: NSTextView {
    private static let baseFont = NSFont.monospacedSystemFont(ofSize: 12, weight: .regular)
    private static let yamlIndentWidth = 2
    private static let largeDocumentRenderPadding = 8_000

    private var contentStyle: AppKitManifestTextView.ContentStyle = .plainText
    private var externalValidationIssues: [YAMLValidationIssue] = []
    private var activeValidationIssues: [YAMLValidationIssue] = []
    private var lastNavigationRequest: YAMLTextNavigationRequest?
    private var didApplyStaticConfiguration = false

    override var isOpaque: Bool { false }

    override func drawBackground(in rect: NSRect) {
        NSColor.clear.setFill()
        rect.fill()
        guard contentStyle == .yaml else { return }
        drawIndentGuides(in: rect)
        drawIssueMarkers(in: rect)
        drawTabMarkers(in: rect)
    }

    func configure(
        isEditable: Bool,
        contentStyle: AppKitManifestTextView.ContentStyle,
        externalValidationIssues: [YAMLValidationIssue]
    ) {
        let styleChanged = self.contentStyle != contentStyle
        let issuesChanged = self.externalValidationIssues != externalValidationIssues
        self.contentStyle = contentStyle
        self.externalValidationIssues = externalValidationIssues

        if self.isEditable != isEditable {
            self.isEditable = isEditable
        }

        applyStaticConfigurationIfNeeded()

        if styleChanged || issuesChanged {
            refreshLayout()
        } else {
            refreshViewportGeometry()
        }
    }

    private func applyStaticConfigurationIfNeeded() {
        guard !didApplyStaticConfiguration else { return }
        didApplyStaticConfiguration = true

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
        defaultParagraphStyle = Self.yamlParagraphStyle(font: Self.baseFont)
        typingAttributes = [
            .font: Self.baseFont,
            .foregroundColor: NSColor.labelColor,
            .paragraphStyle: Self.yamlParagraphStyle(font: Self.baseFont)
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
        let source = storage.string
        let usesViewportAnalysis = contentStyle == .yaml && YAMLLanguageService.prefersViewportAnalysis(source)
        let styleRange = usesViewportAnalysis ? yamlViewportAnalysisRange(in: source) : fullRange
        storage.beginEditing()
        storage.setAttributes([
            .font: Self.baseFont,
            .foregroundColor: NSColor.labelColor,
            .paragraphStyle: Self.yamlParagraphStyle(font: Self.baseFont)
        ], range: styleRange)

        if contentStyle == .yaml, storage.length > 0 {
            let analysis = usesViewportAnalysis
                ? YAMLLanguageService.analyzeFragment(source, range: styleRange)
                : YAMLLanguageService.analyze(source)
            let issues = deduplicatedIssues(
                analysis.validationIssues + externalValidationIssues.filter { $0.source != .syntax }
            )
            activeValidationIssues = issues
            applyYAMLHighlighting(in: storage, fullRange: fullRange, analysis: analysis)
            applyYAMLDiagnostics(in: storage, issues: issues)
        } else {
            activeValidationIssues = []
        }
        storage.endEditing()

        updateDocumentSize()
        invalidateIntrinsicContentSize()
        needsDisplay = true
    }

    private func yamlViewportAnalysisRange(in source: String) -> NSRange {
        let nsSource = source as NSString
        guard nsSource.length > 0 else {
            return NSRange(location: 0, length: 0)
        }

        let visible = visibleCharacterRange ?? NSRange(location: 0, length: min(nsSource.length, Self.largeDocumentRenderPadding))
        let paddedLocation = max(0, visible.location - Self.largeDocumentRenderPadding)
        let paddedEnd = min(nsSource.length, NSMaxRange(visible) + Self.largeDocumentRenderPadding)
        let padded = NSRange(location: paddedLocation, length: max(0, paddedEnd - paddedLocation))
        return nsSource.lineRange(for: padded)
    }

    func refreshViewportGeometry() {
        updateDocumentSize()
        invalidateIntrinsicContentSize()
        needsDisplay = true
    }

    func navigateIfNeeded(_ request: YAMLTextNavigationRequest?) {
        guard let request, request != lastNavigationRequest else { return }
        lastNavigationRequest = request
        guard let targetRange = YAMLTextNavigation.targetRange(in: string, request: request) else { return }

        setSelectedRange(targetRange)
        scrollRangeToVisible(targetRange)

        if isEditable {
            window?.makeFirstResponder(self)
        }
    }

    override func didChangeText() {
        super.didChangeText()
        refreshLayout()
    }

    override func insertTab(_ sender: Any?) {
        guard isEditable, contentStyle == .yaml else {
            super.insertTab(sender)
            return
        }

        let selection = selectedRange()
        guard selection.length == 0 else {
            indentSelectedLines()
            return
        }

        let column = currentLineColumn(at: selection.location)
        insertText(YAMLLanguageService.softTabWhitespace(forColumn: column), replacementRange: selection)
    }

    override func insertBacktab(_ sender: Any?) {
        guard isEditable, contentStyle == .yaml else {
            super.insertBacktab(sender)
            return
        }

        outdentSelectedLines()
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
        guard let layoutManager else { return }
        guard let visibleRange = visibleCharacterRange else { return }

        let guideColor = ManifestPalette.indentGuide
        let path = NSBezierPath()
        let columnWidth = widthOfSingleSpace()
        let textOrigin = textContainerOrigin
        let nsString = string as NSString
        let guideMetrics = YAMLIndentGuideMetrics(indentWidth: Self.yamlIndentWidth)
        let lineSearchRange = nsString.lineRange(for: visibleRange)

        nsString.enumerateSubstrings(in: lineSearchRange, options: [.byLines, .substringNotRequired]) { _, substringRange, _, _ in
            let glyphRange = layoutManager.glyphRange(forCharacterRange: substringRange, actualCharacterRange: nil)
            guard glyphRange.length > 0 else { return }
            let lineRect = layoutManager.lineFragmentRect(forGlyphAt: glyphRange.location, effectiveRange: nil)
            let line = nsString.substring(with: substringRange)
            let leadingColumns = indentationColumns(in: line)

            for level in guideMetrics.guideLevels(forIndentColumns: leadingColumns) {
                let x = textOrigin.x + guideMetrics.guideXPosition(forLevel: level, columnWidth: columnWidth, insetX: 0)
                path.move(to: NSPoint(x: x, y: textOrigin.y + lineRect.minY + 1))
                path.line(to: NSPoint(x: x, y: textOrigin.y + lineRect.maxY - 1))
            }
        }

        guideColor.setStroke()
        path.lineWidth = 0.75
        path.stroke()
    }

    private func drawTabMarkers(in rect: NSRect) {
        guard let layoutManager, let textContainer else { return }
        guard let visibleRange = visibleCharacterRange else { return }

        let nsString = string as NSString
        let attributes: [NSAttributedString.Key: Any] = [
            .font: NSFont.monospacedSystemFont(ofSize: 10, weight: .semibold),
            .foregroundColor: ManifestPalette.tabMarkerText
        ]
        let marker = NSAttributedString(string: "\u{2192}", attributes: attributes)
        let markerMetrics = YAMLTabMarkerMetrics()

        for index in visibleRange.location..<(visibleRange.location + visibleRange.length) {
            guard nsString.character(at: index) == 9 else { continue }
            let glyphRange = layoutManager.glyphRange(forCharacterRange: NSRange(location: index, length: 1), actualCharacterRange: nil)
            guard glyphRange.length > 0 else { continue }
            let glyphRect = layoutManager.boundingRect(forGlyphRange: glyphRange, in: textContainer)
            let lineRect = layoutManager.lineFragmentRect(forGlyphAt: glyphRange.location, effectiveRange: nil)
            let textOrigin = textContainerOrigin
            let pillRect = markerMetrics.markerRect(
                glyphRect: glyphRect.offsetBy(dx: textOrigin.x, dy: textOrigin.y),
                lineRect: lineRect.offsetBy(dx: textOrigin.x, dy: textOrigin.y)
            )
            ManifestPalette.tabMarkerBackground.setFill()
            NSBezierPath(roundedRect: pillRect, xRadius: 4, yRadius: 4).fill()

            let markerSize = marker.size()
            marker.draw(
                at: NSPoint(
                    x: pillRect.midX - markerSize.width / 2,
                    y: pillRect.midY - markerSize.height / 2 - 0.5
                )
            )
        }
    }

    private func drawIssueMarkers(in rect: NSRect) {
        guard let layoutManager else { return }
        guard let visibleRange = visibleCharacterRange else { return }

        var drawnLines: Set<Int> = []
        let nsString = string as NSString
        let textOrigin = textContainerOrigin
        for issue in activeValidationIssues {
            guard let issueRange = issue.range?.nsRange else { continue }
            guard NSIntersectionRange(issueRange, visibleRange).length > 0 || visibleRange.contains(issueRange.location) else { continue }

            let lineRange = nsString.lineRange(for: NSRange(location: min(issueRange.location, nsString.length), length: 0))
            guard drawnLines.insert(lineRange.location).inserted else { continue }

            let glyphRange = layoutManager.glyphRange(forCharacterRange: lineRange, actualCharacterRange: nil)
            guard glyphRange.length > 0 else { continue }
            let lineRect = layoutManager.lineFragmentRect(forGlyphAt: glyphRange.location, effectiveRange: nil)
            let color = issue.severity == .error ? ManifestPalette.errorUnderline : ManifestPalette.warningUnderline
            color.setFill()

            let markerRect = NSRect(
                x: max(3, textOrigin.x - 7),
                y: textOrigin.y + lineRect.midY - 2.5,
                width: 5,
                height: 5
            )
            NSBezierPath(ovalIn: markerRect).fill()
        }
    }

    private var visibleCharacterRange: NSRange? {
        guard let layoutManager, let textContainer, let scrollView = enclosingScrollView else { return nil }
        let textOrigin = textContainerOrigin
        let visibleRect = scrollView.contentView.bounds.offsetBy(dx: -textOrigin.x, dy: -textOrigin.y)
        let glyphRange = layoutManager.glyphRange(forBoundingRect: visibleRect, in: textContainer)
        return layoutManager.characterRange(forGlyphRange: glyphRange, actualGlyphRange: nil)
    }

    private func widthOfSingleSpace() -> CGFloat {
        let sample = " " as NSString
        let size = sample.size(withAttributes: [.font: Self.baseFont])
        return max(1, size.width)
    }

    private static func yamlParagraphStyle(font: NSFont) -> NSParagraphStyle {
        let spaceWidth = max(1, (" " as NSString).size(withAttributes: [.font: font]).width)
        let interval = YAMLTabStopMetrics(indentWidth: yamlIndentWidth).defaultInterval(spaceWidth: spaceWidth)
        let style = NSMutableParagraphStyle()
        style.tabStops = (1...80).map { index in
            NSTextTab(textAlignment: .natural, location: CGFloat(index) * interval)
        }
        style.defaultTabInterval = interval
        return style
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

    private func currentLineColumn(at location: Int) -> Int {
        let nsString = string as NSString
        let safeLocation = min(max(0, location), nsString.length)
        let lineRange = nsString.lineRange(for: NSRange(location: safeLocation, length: 0))
        guard safeLocation >= lineRange.location else { return 0 }
        return safeLocation - lineRange.location
    }

    private func indentSelectedLines() {
        replaceSelectedLines { "  " + $0 }
    }

    private func outdentSelectedLines() {
        replaceSelectedLines { line in
            if line.hasPrefix("  ") {
                return String(line.dropFirst(2))
            }
            if line.hasPrefix(" ") || line.hasPrefix("\t") {
                return String(line.dropFirst())
            }
            return line
        }
    }

    private func replaceSelectedLines(transform: (String) -> String) {
        let nsString = string as NSString
        let selection = selectedRange()
        let selectedLineRange = nsString.lineRange(for: selection)
        let selectedText = nsString.substring(with: selectedLineRange)
        let hasTrailingNewline = selectedText.hasSuffix("\n") || selectedText.hasSuffix("\r")
        var lines = selectedText.split(omittingEmptySubsequences: false, whereSeparator: \.isNewline).map(String.init)

        if hasTrailingNewline, lines.last == "" {
            lines.removeLast()
        }

        let replacement = lines.map(transform).joined(separator: "\n") + (hasTrailingNewline ? "\n" : "")
        insertText(replacement, replacementRange: selectedLineRange)
        setSelectedRange(NSRange(location: selectedLineRange.location, length: (replacement as NSString).length))
    }

    private func deduplicatedIssues(_ issues: [YAMLValidationIssue]) -> [YAMLValidationIssue] {
        var seen: Set<String> = []
        return issues.filter { issue in
            seen.insert(issue.id).inserted
        }
    }
}

private struct ManifestPalette {
    static let key = NSColor.systemBlue.withAlphaComponent(0.95)
    static let string = NSColor.systemGreen.withAlphaComponent(0.9)
    static let number = NSColor.systemOrange.withAlphaComponent(0.95)
    static let boolean = NSColor.systemPurple.withAlphaComponent(0.95)
    static let comment = NSColor.secondaryLabelColor.withAlphaComponent(0.85)
    static let directive = NSColor.systemPink.withAlphaComponent(0.95)
    static let anchor = NSColor.systemTeal.withAlphaComponent(0.95)
    static let indentGuide = NSColor.separatorColor.withAlphaComponent(0.28)
    static let tabMarkerText = NSColor.systemRed.withAlphaComponent(0.95)
    static let tabMarkerBackground = NSColor.systemRed.withAlphaComponent(0.14)
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
