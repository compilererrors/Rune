import AppKit
import SwiftUI
import RuneCore

struct AppKitManifestTextView: NSViewRepresentable {
    enum ContentStyle: Sendable {
        case yaml
        case describe
        case plainText
        case ansiLogs
    }

    @Binding var text: String
    var isEditable: Bool
    var resetScrollOnExternalChange = false
    var contentStyle: ContentStyle = .plainText
    var externalValidationIssues: [YAMLValidationIssue] = []
    var navigationRequest: YAMLTextNavigationRequest?
    var showsLineNumbers = false
    var searchQuery = ""
    var searchMatchCase = false
    var selectedSearchMatchIndex = 0
    var searchMatchRanges: [NSRange]?
    var searchNavigationRevision = 0
    var editorRestorationRequest: ResourceYAMLEditorRestorationRequest?
    var onEditorEdit: ((String, ResourceYAMLEditorPresentation?) -> Void)?
    var onUndoCommand: (() -> Void)?
    @AppStorage(RuneSettingsKeys.terminalFontSize) private var appFontSize = RuneSettingsKeys.terminalFontSizeDefault
    @Environment(\.runeResolvedTheme) private var resolvedTheme

    final class Coordinator: NSObject, NSTextViewDelegate {
        var parent: AppKitManifestTextView
        var isUpdatingFromSwiftUI = false
        var appliedEditorRestorationSequence: UInt64?

        init(parent: AppKitManifestTextView) {
            self.parent = parent
        }

        func textDidChange(_ notification: Notification) {
            guard !isUpdatingFromSwiftUI,
                  let textView = notification.object as? PlainManifestTextView
            else { return }

            let presentation = textView.consumePendingEditPresentation()
            if let onEditorEdit = parent.onEditorEdit {
                onEditorEdit(textView.string, presentation)
            } else {
                parent.text = textView.string
            }
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(parent: self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = ManifestTextScrollView(frame: .zero)
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
        textView.onUndoCommand = onUndoCommand
        textView.configure(
            isEditable: isEditable,
            fontSize: clampedFontSize,
            contentStyle: contentStyle,
            externalValidationIssues: externalValidationIssues,
            showsLineNumbers: showsLineNumbers,
            manifestTheme: resolvedTheme
        )
        textView.delegate = context.coordinator
        textView.setStringKeepingSelection(text)

        scrollView.documentView = textView
        scrollView.configureLineNumberGutter(textView: textView, isVisible: contentStyle == .yaml && showsLineNumbers)
        applyEditorRestorationIfNeeded(
            to: textView,
            coordinator: context.coordinator
        )
        textView.applySearchHighlights(
            query: searchQuery,
            matchCase: searchMatchCase,
            selectedIndex: selectedSearchMatchIndex,
            precomputedRanges: searchMatchRanges,
            navigationRevision: searchNavigationRevision
        )
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self

        guard let textView = scrollView.documentView as? PlainManifestTextView else { return }
        textView.onUndoCommand = onUndoCommand
        textView.configure(
            isEditable: isEditable,
            fontSize: clampedFontSize,
            contentStyle: contentStyle,
            externalValidationIssues: externalValidationIssues,
            showsLineNumbers: showsLineNumbers,
            manifestTheme: resolvedTheme
        )

        if textView.representedText != text {
            context.coordinator.isUpdatingFromSwiftUI = true
            textView.setStringKeepingSelection(text)
            context.coordinator.isUpdatingFromSwiftUI = false

            if resetScrollOnExternalChange {
                textView.scrollRangeToVisible(NSRange(location: 0, length: 0))
            }
        }

        // Keep scroll/document geometry in sync even when the text itself is unchanged.
        textView.refreshViewportGeometry()
        applyEditorRestorationIfNeeded(
            to: textView,
            coordinator: context.coordinator
        )
        textView.navigateIfNeeded(navigationRequest)
        textView.applySearchHighlights(
            query: searchQuery,
            matchCase: searchMatchCase,
            selectedIndex: selectedSearchMatchIndex,
            precomputedRanges: searchMatchRanges,
            navigationRevision: searchNavigationRevision
        )
        (scrollView as? ManifestTextScrollView)?.configureLineNumberGutter(
            textView: textView,
            isVisible: contentStyle == .yaml && showsLineNumbers
        )
    }

    static func dismantleNSView(_ scrollView: NSScrollView, coordinator _: Coordinator) {
        guard let textView = scrollView.documentView as? PlainManifestTextView else { return }
        textView.prepareForDismantling()
    }

    private var clampedFontSize: CGFloat {
        CGFloat(RuneSettingsKeys.clampedTerminalFontSize(appFontSize))
    }

    private func applyEditorRestorationIfNeeded(
        to textView: PlainManifestTextView,
        coordinator: Coordinator
    ) {
        guard let request = editorRestorationRequest,
              request.sequence != coordinator.appliedEditorRestorationSequence,
              request.yaml == text
        else {
            return
        }
        coordinator.appliedEditorRestorationSequence = request.sequence
        textView.restoreEditorPresentation(request.presentation)
    }
}

struct YAMLLineNumberGutterMetrics {
    let font: NSFont
    let lineCount: Int

    private var digitCount: Int {
        max(2, String(max(1, lineCount)).count)
    }

    var leadingPadding: CGFloat {
        ceil(font.pointSize * 0.35)
    }

    var trailingPadding: CGFloat {
        ceil(font.pointSize * 0.55)
    }

    var textPadding: CGFloat {
        ceil(font.pointSize * 0.55)
    }

    var numberColumnWidth: CGFloat {
        let sample = String(repeating: "8", count: digitCount) as NSString
        return ceil(sample.size(withAttributes: [.font: numberFont]).width)
    }

    var gutterWidth: CGFloat {
        ceil(leadingPadding + numberColumnWidth + trailingPadding)
    }

    var textInset: CGFloat {
        gutterWidth + textPadding
    }

    var numberFont: NSFont {
        NSFont.monospacedDigitSystemFont(ofSize: max(NSFont.smallSystemFontSize, font.pointSize - 1), weight: .regular)
    }
}

private extension NSString {
    func lineNumber(atUTF16Offset target: Int) -> Int {
        let boundedTarget = max(0, min(target, length))
        var line = 1
        var index = 0
        while index < boundedTarget {
            if character(at: index) == 10 {
                line += 1
            }
            index += 1
        }
        return line
    }
}

final class ManifestTextScrollView: NSScrollView {
    let lineNumberGutterView = YAMLLineNumberGutterOverlayView(frame: .zero)
    private weak var manifestTextView: PlainManifestTextView?

    override init(frame frameRect: NSRect) {
        super.init(frame: frameRect)
        configureGutterView()
    }

    required init?(coder: NSCoder) {
        super.init(coder: coder)
        configureGutterView()
    }

    private func configureGutterView() {
        lineNumberGutterView.isHidden = true
        lineNumberGutterView.autoresizingMask = [.height]
        addSubview(lineNumberGutterView)
    }

    fileprivate func configureLineNumberGutter(textView: PlainManifestTextView, isVisible: Bool) {
        manifestTextView = textView
        lineNumberGutterView.textView = textView
        lineNumberGutterView.isHidden = !isVisible
        refreshLineNumberGutter()
    }

    func refreshLineNumberGutter() {
        guard let textView = manifestTextView, !lineNumberGutterView.isHidden else {
            lineNumberGutterView.needsDisplay = true
            return
        }

        let metrics = textView.currentLineNumberGutterMetrics
        let contentFrame = contentView.frame
        lineNumberGutterView.frame = NSRect(
            x: contentFrame.minX,
            y: contentFrame.minY,
            width: metrics.gutterWidth,
            height: contentFrame.height
        )
        lineNumberGutterView.needsDisplay = true
    }

    override func layout() {
        super.layout()
        refreshLineNumberGutter()
    }

    override func reflectScrolledClipView(_ clipView: NSClipView) {
        super.reflectScrolledClipView(clipView)
        lineNumberGutterView.needsDisplay = true
    }
}

final class YAMLLineNumberGutterOverlayView: NSView {
    fileprivate weak var textView: PlainManifestTextView?
    private var cursorTrackingArea: NSTrackingArea?

    override var isOpaque: Bool { true }
    override var isFlipped: Bool { true }

    override func updateTrackingAreas() {
        super.updateTrackingAreas()

        if let cursorTrackingArea {
            removeTrackingArea(cursorTrackingArea)
        }

        let trackingArea = NSTrackingArea(
            rect: .zero,
            options: [.cursorUpdate, .activeInKeyWindow, .inVisibleRect],
            owner: self,
            userInfo: nil
        )
        addTrackingArea(trackingArea)
        cursorTrackingArea = trackingArea
    }

    override func cursorUpdate(with event: NSEvent) {
        NSCursor.arrow.set()
    }

    override func draw(_ dirtyRect: NSRect) {
        guard let textView else { return }

        let metrics = textView.currentLineNumberGutterMetrics
        gutterBackgroundColor.setFill()
        bounds.fill()
        NSColor.separatorColor.withAlphaComponent(0.22).setStroke()
        NSBezierPath.strokeLine(
            from: NSPoint(x: bounds.maxX - 0.5, y: bounds.minY),
            to: NSPoint(x: bounds.maxX - 0.5, y: bounds.maxY)
        )

        let attributes: [NSAttributedString.Key: Any] = [
            .font: metrics.numberFont,
            .foregroundColor: NSColor.tertiaryLabelColor
        ]
        for label in visibleLineNumberLabels() {
            let text = "\(label.number)" as NSString
            let size = text.size(withAttributes: attributes)
            let x = metrics.leadingPadding + max(0, metrics.numberColumnWidth - size.width)
            text.draw(at: NSPoint(x: x, y: label.y), withAttributes: attributes)
        }
    }

    var gutterBackgroundColor: NSColor {
        NSColor.controlBackgroundColor
    }

    func visibleLineNumberLabels() -> [(number: Int, y: CGFloat)] {
        guard let textView,
              let layoutManager = textView.layoutManager,
              let textContainer = textView.textContainer,
              let scrollView = textView.enclosingScrollView
        else { return [] }

        let visibleRect = scrollView.contentView.bounds
        let textOrigin = textView.textContainerOrigin
        let glyphRange = layoutManager.glyphRange(
            forBoundingRect: visibleRect.offsetBy(dx: -textOrigin.x, dy: -textOrigin.y),
            in: textContainer
        )
        guard glyphRange.length > 0 else { return [] }

        let nsString = textView.string as NSString
        let characterRange = layoutManager.characterRange(forGlyphRange: glyphRange, actualGlyphRange: nil)
        let lineSearchRange = nsString.lineRange(for: characterRange)
        var lineNumber = nsString.lineNumber(atUTF16Offset: lineSearchRange.location)
        var labels: [(number: Int, y: CGFloat)] = []

        nsString.enumerateSubstrings(in: lineSearchRange, options: [.byLines, .substringNotRequired]) { _, lineRange, enclosingRange, _ in
            let layoutRange = enclosingRange.length > 0 ? enclosingRange : lineRange
            let lineGlyphRange = layoutManager.glyphRange(forCharacterRange: layoutRange, actualCharacterRange: nil)
            defer { lineNumber += 1 }

            guard lineGlyphRange.length > 0,
                  NSIntersectionRange(lineGlyphRange, glyphRange).length > 0
            else { return }

            let lineRect = layoutManager.lineFragmentRect(forGlyphAt: lineGlyphRange.location, effectiveRange: nil)
            labels.append((
                number: lineNumber,
                y: textOrigin.y + lineRect.minY - visibleRect.minY + max(0, (textView.currentBaseFont.pointSize - textView.currentLineNumberGutterMetrics.numberFont.pointSize) * 0.5)
            ))
        }

        return labels
    }
}

private final class PlainManifestTextView: NSTextView {
    private static let yamlIndentWidth = 2
    private static let largeDocumentRenderPadding = 8_000

    private struct DocumentLineMetrics {
        let lineCount: Int
        let maxLineUTF16Length: Int
    }

    private struct DocumentSizeCacheKey: Equatable {
        let revision: Int
        let fontSizeTenths: Int
        let visibleWidth: Int
        let visibleHeight: Int
        let usesEstimatedLargeYAMLSize: Bool
    }

    private var contentStyle: AppKitManifestTextView.ContentStyle = .plainText
    var representedText = ""
    private var configuredFontSize = CGFloat(RuneSettingsKeys.terminalFontSizeDefault)
    private var externalValidationIssues: [YAMLValidationIssue] = []
    private var activeValidationIssues: [YAMLValidationIssue] = []
    private var lastNavigationRequest: YAMLTextNavigationRequest?
    private var didApplyStaticConfiguration = false
    private var tabKeyMonitor: Any?
    private var documentRevision = 0
    private var documentLineMetricsCache: (revision: Int, metrics: DocumentLineMetrics)?
    private var documentSizeCache: (key: DocumentSizeCacheKey, size: NSSize)?
    private var showsLineNumbers = false
    private var manifestThemeID = RuneSettingsKeys.appearanceThemeDefault
    private var manifestPalette = ManifestPalette.resolved(RuneAppearanceTheme.native.resolvedTheme)
    private var searchHighlightRanges: [NSRange] = []
    private var activeSearchHighlightRange: NSRange?
    private var lastAppliedSearchKey = ""
    private var lastSearchNavigationRevision: Int?
    private var pendingEditPresentation: ResourceYAMLEditorPresentation?
    var onUndoCommand: (() -> Void)?

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
        fontSize: CGFloat,
        contentStyle: AppKitManifestTextView.ContentStyle,
        externalValidationIssues: [YAMLValidationIssue],
        showsLineNumbers: Bool,
        manifestTheme: RuneResolvedTheme
    ) {
        let styleChanged = self.contentStyle != contentStyle
        let fontSizeChanged = self.configuredFontSize != fontSize
        let issuesChanged = self.externalValidationIssues != externalValidationIssues
        let lineNumbersChanged = self.showsLineNumbers != showsLineNumbers
        let themeChanged = self.manifestThemeID != manifestTheme.id
        self.contentStyle = contentStyle
        self.configuredFontSize = fontSize
        self.externalValidationIssues = externalValidationIssues
        self.showsLineNumbers = showsLineNumbers
        self.manifestThemeID = manifestTheme.id
        manifestPalette = ManifestPalette.resolved(manifestTheme)

        if self.isEditable != isEditable {
            self.isEditable = isEditable
        }
        applyStaticConfigurationIfNeeded()
        let allowsNativeUndo = isEditable && onUndoCommand == nil
        if allowsUndo != allowsNativeUndo {
            allowsUndo = allowsNativeUndo
            if !allowsNativeUndo {
                undoManager?.removeAllActions()
            }
        }
        applyThemeConfiguration()
        let shouldUseRichText = contentStyle == .ansiLogs
        if isRichText != shouldUseRichText {
            isRichText = shouldUseRichText
        }
        if fontSizeChanged || lineNumbersChanged || themeChanged {
            documentSizeCache = nil
            applyFontConfiguration()
            applyTextContainerInsets()
        }

        if styleChanged || fontSizeChanged || issuesChanged || lineNumbersChanged || themeChanged {
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
        allowsUndo = false
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
        applyTextContainerInsets()
        backgroundColor = .clear
        drawsBackground = false
        applyThemeConfiguration()
        applyFontConfiguration()

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

    private func applyFontConfiguration() {
        let baseFont = currentBaseFont
        font = baseFont
        textColor = manifestPalette.foreground
        defaultParagraphStyle = Self.yamlParagraphStyle(font: baseFont)
        typingAttributes = [
            .font: baseFont,
            .foregroundColor: manifestPalette.foreground,
            .paragraphStyle: Self.yamlParagraphStyle(font: baseFont)
        ]
    }

    private func applyThemeConfiguration() {
        insertionPointColor = manifestPalette.accent
        selectedTextAttributes = [
            .backgroundColor: manifestPalette.selection
        ]
        needsDisplay = true
    }

    private func applyTextContainerInsets() {
        let baseHorizontalInset = showsLineNumbers
            ? currentLineNumberGutterMetrics.textInset
            : RuneUILayoutMetrics.inspectorDocumentHorizontalInset
        let targetInset = NSSize(
            width: baseHorizontalInset,
            height: RuneUILayoutMetrics.inspectorDocumentVerticalInset
        )
        guard abs(textContainerInset.width - targetInset.width) > 0.5
            || abs(textContainerInset.height - targetInset.height) > 0.5
        else {
            refreshLineNumberGutterOverlay()
            return
        }
        textContainerInset = targetInset
        documentSizeCache = nil
        refreshLineNumberGutterOverlay()
    }

    private func refreshLineNumberGutterOverlay() {
        (enclosingScrollView as? ManifestTextScrollView)?.refreshLineNumberGutter()
    }

    func setStringKeepingSelection(_ newValue: String) {
        let selected = selectedRanges
        pendingEditPresentation = nil
        representedText = newValue
        if contentStyle != .ansiLogs {
            string = newValue
        }
        invalidateDocumentMetrics()
        applyTextContainerInsets()
        refreshLayout()
        if !selected.isEmpty {
            selectedRanges = selected.map {
                NSValue(range: clampedSelectionRange($0.rangeValue))
            }
        }
    }

    override func shouldChangeText(
        in affectedCharRange: NSRange,
        replacementString: String?
    ) -> Bool {
        guard super.shouldChangeText(
            in: affectedCharRange,
            replacementString: replacementString
        ) else {
            return false
        }
        guard isEditable,
              let replacementString
        else {
            return true
        }

        let source = string as NSString
        let boundedRange = NSIntersectionRange(
            affectedCharRange,
            NSRange(location: 0, length: source.length)
        )
        guard source.substring(with: boundedRange) != replacementString else {
            return true
        }

        if pendingEditPresentation == nil {
            let viewportOrigin = enclosingScrollView?.contentView.bounds.origin ?? .zero
            pendingEditPresentation = ResourceYAMLEditorPresentation(
                selections: selectedRanges.map {
                    ResourceYAMLEditorSelection(
                        location: $0.rangeValue.location,
                        length: $0.rangeValue.length
                    )
                },
                viewportX: Double(viewportOrigin.x),
                viewportY: Double(viewportOrigin.y),
                hadKeyboardFocus: window?.firstResponder === self
            )
        }
        return true
    }

    func consumePendingEditPresentation() -> ResourceYAMLEditorPresentation? {
        defer { pendingEditPresentation = nil }
        return pendingEditPresentation
    }

    private func clampedSelectionRange(_ range: NSRange) -> NSRange {
        let textLength = string.utf16.count
        let location = max(0, min(range.location, textLength))
        return NSRange(
            location: location,
            length: max(0, min(range.length, textLength - location))
        )
    }

    func restoreEditorPresentation(_ presentation: ResourceYAMLEditorPresentation) {
        let restoredRanges = presentation.selections.map {
            NSValue(range: clampedSelectionRange(NSRange(
                location: $0.location,
                length: $0.length
            )))
        }
        if !restoredRanges.isEmpty {
            selectedRanges = restoredRanges
        }

        guard let scrollView = enclosingScrollView else { return }
        if let layoutManager, let textContainer {
            layoutManager.ensureLayout(for: textContainer)
        }
        scrollView.layoutSubtreeIfNeeded()

        let clipView = scrollView.contentView
        let proposedBounds = NSRect(
            origin: NSPoint(
                x: CGFloat(presentation.viewportX),
                y: CGFloat(presentation.viewportY)
            ),
            size: clipView.bounds.size
        )
        clipView.scroll(to: clipView.constrainBoundsRect(proposedBounds).origin)
        scrollView.reflectScrolledClipView(clipView)

        if presentation.hadKeyboardFocus, isEditable {
            DispatchQueue.main.async { [weak self] in
                guard let self, self.isEditable else { return }
                self.window?.makeFirstResponder(self)
            }
        }
    }

    func refreshLayout() {
        guard let storage = textStorage else { return }
        clearSearchHighlights(in: storage)
        lastAppliedSearchKey = ""

        let fullRange = NSRange(location: 0, length: storage.length)
        let source = storage.string
        if contentStyle == .ansiLogs {
            storage.setAttributedString(ResourceLogANSIFormatter.attributedString(
                from: representedText,
                font: currentBaseFont
            ))
            activeValidationIssues = []
            updateDocumentSize()
            invalidateIntrinsicContentSize()
            needsDisplay = true
            return
        }

        let usesViewportAnalysis = contentStyle == .yaml && YAMLLanguageService.prefersViewportAnalysis(source)
        let styleRange = usesViewportAnalysis ? yamlViewportAnalysisRange(in: source) : fullRange
        let baseFont = currentBaseFont
        storage.beginEditing()
        storage.setAttributes([
            .font: baseFont,
            .foregroundColor: manifestPalette.foreground,
            .paragraphStyle: Self.yamlParagraphStyle(font: baseFont)
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
        } else if contentStyle == .describe, storage.length > 0 {
            activeValidationIssues = []
            applyDescribeHighlighting(in: storage, range: styleRange)
        } else {
            activeValidationIssues = []
        }
        storage.endEditing()

        updateDocumentSize()
        invalidateIntrinsicContentSize()
        needsDisplay = true
        refreshLineNumberGutterOverlay()
    }

    func applySearchHighlights(
        query: String,
        matchCase: Bool,
        selectedIndex: Int,
        precomputedRanges: [NSRange]?,
        navigationRevision: Int
    ) {
        guard let storage = textStorage, let layoutManager else { return }
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedQuery.isEmpty else {
            clearSearchHighlights(in: storage)
            lastAppliedSearchKey = ""
            lastSearchNavigationRevision = navigationRevision
            return
        }

        let ranges = precomputedRanges ?? InspectorFindIndex(
            text: storage.string,
            query: trimmedQuery,
            matchCase: matchCase
        ).ranges
        let firstRange = ranges.first ?? NSRange(location: 0, length: 0)
        let lastRange = ranges.last ?? NSRange(location: 0, length: 0)
        let searchKey = [
            "\(documentRevision)",
            trimmedQuery,
            matchCase ? "case" : "folded",
            "\(ranges.count)",
            "\(firstRange.location):\(firstRange.length)",
            "\(lastRange.location):\(lastRange.length)"
        ].joined(separator: ":")

        if lastAppliedSearchKey != searchKey {
            clearSearchHighlights(in: storage)
            lastAppliedSearchKey = searchKey
            let passiveLimit = min(ranges.count, 4_096)
            searchHighlightRanges.reserveCapacity(passiveLimit)
            for range in ranges.prefix(passiveLimit) where NSMaxRange(range) <= storage.length {
                applyPassiveSearchHighlight(range, layoutManager: layoutManager)
                searchHighlightRanges.append(range)
            }
        }

        guard !ranges.isEmpty else {
            activeSearchHighlightRange = nil
            lastSearchNavigationRevision = navigationRevision
            return
        }

        let activeIndex = min(max(selectedIndex, 0), ranges.count - 1)
        let activeRange = ranges[activeIndex]
        guard NSMaxRange(activeRange) <= storage.length else { return }

        if activeSearchHighlightRange != activeRange {
            if let previousActiveRange = activeSearchHighlightRange {
                if searchHighlightRanges.contains(previousActiveRange) {
                    applyPassiveSearchHighlight(previousActiveRange, layoutManager: layoutManager)
                } else {
                    removeSearchHighlightAttributes(previousActiveRange, layoutManager: layoutManager)
                }
            }
            applyActiveSearchHighlight(activeRange, layoutManager: layoutManager)
            activeSearchHighlightRange = activeRange
        }

        guard navigationRevision != lastSearchNavigationRevision else { return }
        setSelectedRange(activeRange)
        guard enclosingScrollView != nil else { return }
        lastSearchNavigationRevision = navigationRevision
        centerSearchRange(
            activeRange,
            animated: !NSWorkspace.shared.accessibilityDisplayShouldReduceMotion
        )
    }

    private func clearSearchHighlights(in storage: NSTextStorage) {
        guard let layoutManager else {
            searchHighlightRanges.removeAll(keepingCapacity: true)
            activeSearchHighlightRange = nil
            return
        }
        for range in searchHighlightRanges where NSMaxRange(range) <= storage.length {
            removeSearchHighlightAttributes(range, layoutManager: layoutManager)
        }
        if let activeSearchHighlightRange,
           NSMaxRange(activeSearchHighlightRange) <= storage.length,
           !searchHighlightRanges.contains(activeSearchHighlightRange) {
            removeSearchHighlightAttributes(activeSearchHighlightRange, layoutManager: layoutManager)
        }
        searchHighlightRanges.removeAll(keepingCapacity: true)
        activeSearchHighlightRange = nil
    }

    private func applyPassiveSearchHighlight(_ range: NSRange, layoutManager: NSLayoutManager) {
        layoutManager.addTemporaryAttributes([
            .backgroundColor: manifestPalette.searchMatchBackground,
            .underlineColor: manifestPalette.searchMatchUnderline,
            .underlineStyle: NSUnderlineStyle.single.rawValue
        ], forCharacterRange: range)
    }

    private func applyActiveSearchHighlight(_ range: NSRange, layoutManager: NSLayoutManager) {
        layoutManager.addTemporaryAttributes([
            .backgroundColor: manifestPalette.activeSearchMatchBackground,
            .underlineColor: manifestPalette.activeSearchMatchUnderline,
            .underlineStyle: NSUnderlineStyle.thick.rawValue
        ], forCharacterRange: range)
    }

    private func removeSearchHighlightAttributes(_ range: NSRange, layoutManager: NSLayoutManager) {
        layoutManager.removeTemporaryAttribute(.backgroundColor, forCharacterRange: range)
        layoutManager.removeTemporaryAttribute(.underlineColor, forCharacterRange: range)
        layoutManager.removeTemporaryAttribute(.underlineStyle, forCharacterRange: range)
    }

    private func centerSearchRange(_ range: NSRange, animated: Bool) {
        guard let layoutManager,
              let textContainer,
              let scrollView = enclosingScrollView,
              range.location <= string.utf16.count
        else {
            return
        }

        layoutManager.ensureLayout(forCharacterRange: range)
        let glyphRange = layoutManager.glyphRange(
            forCharacterRange: range,
            actualCharacterRange: nil
        )
        guard glyphRange.location < layoutManager.numberOfGlyphs else { return }

        var targetRect = layoutManager.boundingRect(
            forGlyphRange: glyphRange,
            in: textContainer
        )
        let textOrigin = textContainerOrigin
        targetRect.origin.x += textOrigin.x
        targetRect.origin.y += textOrigin.y

        let clipView = scrollView.contentView
        let visibleBounds = clipView.bounds
        let documentBounds = scrollView.documentView?.bounds ?? bounds
        var targetOrigin = visibleBounds.origin
        targetOrigin.y = targetRect.midY - visibleBounds.height / 2
        if targetRect.minX < visibleBounds.minX || targetRect.maxX > visibleBounds.maxX {
            targetOrigin.x = targetRect.midX - visibleBounds.width / 2
        }
        targetOrigin.x = min(
            max(documentBounds.minX, targetOrigin.x),
            max(documentBounds.minX, documentBounds.maxX - visibleBounds.width)
        )
        targetOrigin.y = min(
            max(documentBounds.minY, targetOrigin.y),
            max(documentBounds.minY, documentBounds.maxY - visibleBounds.height)
        )

        if animated, window != nil {
            NSAnimationContext.runAnimationGroup { context in
                context.duration = 0.12
                context.allowsImplicitAnimation = true
                clipView.animator().setBoundsOrigin(targetOrigin)
            }
        } else {
            clipView.scroll(to: targetOrigin)
        }
        scrollView.reflectScrolledClipView(clipView)
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

    fileprivate var currentBaseFont: NSFont {
        NSFont.monospacedSystemFont(ofSize: configuredFontSize, weight: .regular)
    }

    func refreshViewportGeometry() {
        updateDocumentSize()
        invalidateIntrinsicContentSize()
        needsDisplay = true
        refreshLineNumberGutterOverlay()
    }

    fileprivate var currentLineNumberGutterMetrics: YAMLLineNumberGutterMetrics {
        YAMLLineNumberGutterMetrics(
            font: currentBaseFont,
            lineCount: max(1, documentLineMetrics(for: string).lineCount)
        )
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
        representedText = string
        invalidateDocumentMetrics()
        applyTextContainerInsets()
        refreshLayout()
    }

    override func keyDown(with event: NSEvent) {
        if let onUndoCommand, shouldHandleUndoCommand(event) {
            onUndoCommand()
            return
        }
        if handleYAMLTabKey(event) {
            return
        }
        super.keyDown(with: event)
    }

    private func shouldHandleUndoCommand(_ event: NSEvent) -> Bool {
        guard isEditable,
              event.type == .keyDown,
              event.charactersIgnoringModifiers?.lowercased() == "z"
        else {
            return false
        }

        let modifiers = event.modifierFlags.intersection(.deviceIndependentFlagsMask)
        let disallowedModifiers: NSEvent.ModifierFlags = [.shift, .option, .control, .function]
        return modifiers.contains(.command) && modifiers.isDisjoint(with: disallowedModifiers)
    }

    override func insertTab(_ sender: Any?) {
        guard isEditable, contentStyle == .yaml else {
            super.insertTab(sender)
            return
        }

        insertSoftTabOrIndentSelection()
    }

    override func insertBacktab(_ sender: Any?) {
        guard isEditable, contentStyle == .yaml else {
            super.insertBacktab(sender)
            return
        }

        outdentSelectedLines()
    }

    private func handleYAMLTabKey(_ event: NSEvent) -> Bool {
        guard shouldHandleYAMLTabKey(event) else { return false }

        if event.modifierFlags.contains(.shift) {
            outdentSelectedLines()
        } else {
            insertSoftTabOrIndentSelection()
        }
        return true
    }

    private func insertSoftTabOrIndentSelection() {
        let selection = selectedRange()
        guard selection.length == 0 else {
            indentSelectedLines()
            return
        }

        let column = currentLineColumn(at: selection.location)
        insertText(YAMLLanguageService.softTabWhitespace(forColumn: column), replacementRange: selection)
    }

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        if window == nil {
            removeTabKeyMonitor()
        } else {
            installTabKeyMonitorIfNeeded()
        }
    }

    private func installTabKeyMonitorIfNeeded() {
        guard tabKeyMonitor == nil else { return }
        tabKeyMonitor = NSEvent.addLocalMonitorForEvents(matching: .keyDown) { [weak self] event in
            guard let self else { return event }
            guard self.window === event.window else { return event }
            guard self.window?.firstResponder === self else { return event }
            guard self.handleYAMLTabKey(event) else { return event }
            return nil
        }
    }

    fileprivate func prepareForDismantling() {
        removeTabKeyMonitor()
    }

    private func removeTabKeyMonitor() {
        if let tabKeyMonitor {
            NSEvent.removeMonitor(tabKeyMonitor)
            self.tabKeyMonitor = nil
        }
    }

    isolated deinit {
        removeTabKeyMonitor()
    }

    private func shouldHandleYAMLTabKey(_ event: NSEvent) -> Bool {
        guard isEditable, contentStyle == .yaml, event.keyCode == 48 else { return false }
        let disallowedModifiers: NSEvent.ModifierFlags = [.command, .option, .control, .function]
        return event.modifierFlags.isDisjoint(with: disallowedModifiers)
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
        documentSizeCache = nil
        updateDocumentSize()
    }

    override func layout() {
        super.layout()
        updateDocumentSize()
    }

    private func updateDocumentSize() {
        guard let layoutManager, let textContainer, let storage = textStorage else { return }
        let visibleSize = enclosingScrollView?.contentSize ?? bounds.size
        let source = storage.string
        let usesEstimatedLargeYAMLSize = contentStyle == .yaml && YAMLLanguageService.prefersViewportAnalysis(source)
        let cacheKey = DocumentSizeCacheKey(
            revision: documentRevision,
            fontSizeTenths: Int((configuredFontSize * 10).rounded()),
            visibleWidth: Int(visibleSize.width.rounded(.up)),
            visibleHeight: Int(visibleSize.height.rounded(.up)),
            usesEstimatedLargeYAMLSize: usesEstimatedLargeYAMLSize
        )

        if let cached = documentSizeCache, cached.key == cacheKey {
            applyDocumentSizeIfNeeded(cached.size)
            return
        }

        let targetSize: NSSize
        if usesEstimatedLargeYAMLSize {
            let metrics = documentLineMetrics(for: source)
            let lineHeight = max(1, layoutManager.defaultLineHeight(for: currentBaseFont))
            let columnWidth = widthOfSingleSpace()
            targetSize = NSSize(
                width: max(
                    visibleSize.width,
                    ceil(CGFloat(metrics.maxLineUTF16Length) * columnWidth + textContainerInset.width * 2 + 40)
                ),
                height: max(
                    visibleSize.height,
                    ceil(CGFloat(max(1, metrics.lineCount)) * lineHeight + textContainerInset.height * 2 + 24)
                )
            )
        } else {
            layoutManager.ensureLayout(for: textContainer)
            let usedRect = layoutManager.usedRect(for: textContainer)
            targetSize = NSSize(
                width: max(
                    visibleSize.width,
                    ceil(usedRect.width + textContainerInset.width * 2 + 40)
                ),
                height: max(
                    visibleSize.height,
                    ceil(usedRect.height + textContainerInset.height * 2 + 24)
                )
            )
        }

        documentSizeCache = (cacheKey, targetSize)
        applyDocumentSizeIfNeeded(targetSize)
    }

    private func applyDocumentSizeIfNeeded(_ size: NSSize) {
        if abs(frame.width - size.width) > 1 || abs(frame.height - size.height) > 1 {
            frame.size = size
        }
    }

    private func invalidateDocumentMetrics() {
        documentRevision += 1
        documentLineMetricsCache = nil
        documentSizeCache = nil
    }

    private func documentLineMetrics(for source: String) -> DocumentLineMetrics {
        if let cached = documentLineMetricsCache, cached.revision == documentRevision {
            return cached.metrics
        }

        var lineCount = source.isEmpty ? 0 : 1
        var currentLineLength = 0
        var maxLineLength = 0

        for character in source.utf16 {
            switch character {
            case 10:
                maxLineLength = max(maxLineLength, currentLineLength)
                currentLineLength = 0
                lineCount += 1
            case 13:
                continue
            default:
                currentLineLength += 1
            }
        }

        maxLineLength = max(maxLineLength, currentLineLength)
        let metrics = DocumentLineMetrics(lineCount: lineCount, maxLineUTF16Length: maxLineLength)
        documentLineMetricsCache = (documentRevision, metrics)
        return metrics
    }

    private func applyYAMLHighlighting(in storage: NSTextStorage, fullRange: NSRange, analysis: YAMLTextAnalysis) {
        for span in analysis.highlights where NSMaxRange(span.range) <= NSMaxRange(fullRange) {
            storage.addAttributes(
                [.foregroundColor: manifestPalette.color(for: span.kind)],
                range: span.range
            )
        }
    }

    private func applyYAMLDiagnostics(in storage: NSTextStorage, issues: [YAMLValidationIssue]) {
        for issue in issues {
            guard let range = issue.range?.nsRange else { continue }
            storage.addAttributes(issue.attributes(palette: manifestPalette), range: range)
        }
    }

    private func applyDescribeHighlighting(in storage: NSTextStorage, range: NSRange) {
        let nsString = storage.string as NSString
        let lineSearchRange = nsString.lineRange(for: range)
        nsString.enumerateSubstrings(in: lineSearchRange, options: [.byLines, .substringNotRequired]) { _, lineRange, _, _ in
            let trimmedRange = self.trimmedLineRange(lineRange, in: nsString)
            guard trimmedRange.length > 0 else { return }

            let line = nsString.substring(with: trimmedRange)
            guard let colonIndex = line.firstIndex(of: ":") else { return }
            let keyLength = line.distance(from: line.startIndex, to: colonIndex)
            guard keyLength > 0 else { return }

            let leadingWhitespace = line.prefix { $0 == " " || $0 == "\t" }.count
            let keyRange = NSRange(location: trimmedRange.location + leadingWhitespace, length: max(0, keyLength - leadingWhitespace))
            guard keyRange.length > 0 else { return }

            let isSectionHeader = line[line.index(after: colonIndex)...].trimmingCharacters(in: .whitespaces).isEmpty
            storage.addAttributes([
                .foregroundColor: isSectionHeader ? self.manifestPalette.describeSection : self.manifestPalette.describeKey,
                .font: NSFont.monospacedSystemFont(ofSize: self.configuredFontSize, weight: isSectionHeader ? .semibold : .medium)
            ], range: keyRange)

            let colonLocation = trimmedRange.location + keyLength
            storage.addAttribute(
                .foregroundColor,
                value: self.manifestPalette.describeColon,
                range: NSRange(location: colonLocation, length: 1)
            )
        }
    }

    private func drawIndentGuides(in rect: NSRect) {
        guard let layoutManager else { return }
        guard let visibleRange = visibleCharacterRange else { return }

        let guideColor = manifestPalette.indentGuide
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
            .foregroundColor: manifestPalette.tabMarkerText
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
            manifestPalette.tabMarkerBackground.setFill()
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
            let color = issue.severity == .error ? manifestPalette.errorUnderline : manifestPalette.warningUnderline
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
        let size = sample.size(withAttributes: [.font: currentBaseFont])
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
    let foreground: NSColor
    let accent: NSColor
    let selection: NSColor
    let searchMatchBackground: NSColor
    let searchMatchUnderline: NSColor
    let activeSearchMatchBackground: NSColor
    let activeSearchMatchUnderline: NSColor
    let key: NSColor
    let string: NSColor
    let number: NSColor
    let boolean: NSColor
    let comment: NSColor
    let directive: NSColor
    let anchor: NSColor
    let indentGuide: NSColor
    let tabMarkerText: NSColor
    let tabMarkerBackground: NSColor
    let errorUnderline: NSColor
    let errorBackground: NSColor
    let warningUnderline: NSColor
    let warningBackground: NSColor
    let describeKey: NSColor
    let describeSection: NSColor
    let describeColon: NSColor

    static func resolved(_ theme: RuneResolvedTheme) -> ManifestPalette {
        guard !theme.isNative,
              let appKit = theme.appKitPalette,
              let syntax = theme.syntaxPalette else {
            return native
        }
        return themed(
            foreground: appKit.foreground,
            accent: appKit.accent,
            key: syntax.key,
            string: syntax.string,
            number: syntax.number,
            boolean: syntax.boolean,
            comment: syntax.comment,
            directive: syntax.directive,
            anchor: syntax.anchor,
            danger: appKit.danger,
            warning: appKit.warning,
            selectionAlpha: selectionAlpha(for: theme, fallback: appKit.selectedAlpha),
            usesLightBackground: theme.preferredColorScheme == .light
        )
    }

    private static func selectionAlpha(for theme: RuneResolvedTheme, fallback: CGFloat) -> CGFloat {
        switch theme.builtin {
        case .contrastDark: return 0.30
        case .contrastLight: return 0.20
        case .paper, .daylight: return 0.18
        case .aurora, .graphiteBlue, .emberGlass, .mossTerminal, .fjord: return 0.24
        case .native: return fallback
        case nil: return fallback
        }
    }

    private static let native = ManifestPalette(
        foreground: .labelColor,
        accent: .controlAccentColor,
        selection: NSColor.controlAccentColor.withAlphaComponent(0.22),
        searchMatchBackground: NSColor.findHighlightColor.withAlphaComponent(0.58),
        searchMatchUnderline: NSColor.systemOrange,
        activeSearchMatchBackground: NSColor.controlAccentColor.withAlphaComponent(0.48),
        activeSearchMatchUnderline: NSColor.controlAccentColor,
        key: NSColor.systemBlue.withAlphaComponent(0.95),
        string: NSColor.systemGreen.withAlphaComponent(0.9),
        number: NSColor.systemOrange.withAlphaComponent(0.95),
        boolean: NSColor.systemPurple.withAlphaComponent(0.95),
        comment: NSColor.secondaryLabelColor.withAlphaComponent(0.85),
        directive: NSColor.systemPink.withAlphaComponent(0.95),
        anchor: NSColor.systemTeal.withAlphaComponent(0.95),
        indentGuide: NSColor.separatorColor.withAlphaComponent(0.28),
        tabMarkerText: NSColor.systemRed.withAlphaComponent(0.95),
        tabMarkerBackground: NSColor.systemRed.withAlphaComponent(0.14),
        errorUnderline: .systemRed,
        errorBackground: NSColor.systemRed.withAlphaComponent(0.12),
        warningUnderline: .systemOrange,
        warningBackground: NSColor.systemOrange.withAlphaComponent(0.1),
        describeKey: NSColor.systemCyan.withAlphaComponent(0.9),
        describeSection: NSColor.controlAccentColor.withAlphaComponent(0.95),
        describeColon: NSColor.secondaryLabelColor.withAlphaComponent(0.8)
    )

    private static func themed(
        foreground: String,
        accent: String,
        key: String,
        string: String,
        number: String,
        boolean: String,
        comment: String,
        directive: String,
        anchor: String,
        danger: String,
        warning: String,
        selectionAlpha: CGFloat = 0.24,
        usesLightBackground: Bool = false
    ) -> ManifestPalette {
        let accentColor = NSColor.runeHex(accent)
        let dangerColor = NSColor.runeHex(danger)
        let warningColor = NSColor.runeHex(warning)
        return ManifestPalette(
            foreground: NSColor.runeHex(foreground),
            accent: accentColor,
            selection: accentColor.withAlphaComponent(selectionAlpha),
            searchMatchBackground: warningColor.withAlphaComponent(usesLightBackground ? 0.30 : 0.38),
            searchMatchUnderline: warningColor.withAlphaComponent(0.98),
            activeSearchMatchBackground: accentColor.withAlphaComponent(usesLightBackground ? 0.38 : 0.52),
            activeSearchMatchUnderline: accentColor,
            key: NSColor.runeHex(key),
            string: NSColor.runeHex(string),
            number: NSColor.runeHex(number),
            boolean: NSColor.runeHex(boolean),
            comment: NSColor.runeHex(comment).withAlphaComponent(0.9),
            directive: NSColor.runeHex(directive),
            anchor: NSColor.runeHex(anchor),
            indentGuide: NSColor.runeHex(comment).withAlphaComponent(0.32),
            tabMarkerText: dangerColor.withAlphaComponent(0.95),
            tabMarkerBackground: dangerColor.withAlphaComponent(0.15),
            errorUnderline: dangerColor,
            errorBackground: dangerColor.withAlphaComponent(0.12),
            warningUnderline: warningColor,
            warningBackground: warningColor.withAlphaComponent(0.12),
            describeKey: NSColor.runeHex(key),
            describeSection: accentColor.withAlphaComponent(0.96),
            describeColon: NSColor.runeHex(comment).withAlphaComponent(0.82)
        )
    }

    func color(for kind: YAMLHighlightKind) -> NSColor {
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
    fileprivate func attributes(palette: ManifestPalette) -> [NSAttributedString.Key: Any] {
        switch severity {
        case .error:
            return [
                .underlineStyle: NSUnderlineStyle.single.rawValue,
                .underlineColor: palette.errorUnderline,
                .backgroundColor: palette.errorBackground
            ]
        case .warning:
            return [
                .underlineStyle: NSUnderlineStyle.single.rawValue,
                .underlineColor: palette.warningUnderline,
                .backgroundColor: palette.warningBackground
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
