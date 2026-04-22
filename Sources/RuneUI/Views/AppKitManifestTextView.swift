import AppKit
import SwiftUI

struct AppKitManifestTextView: NSViewRepresentable {
    enum ContentStyle: Sendable {
        case yaml
        case plainText
    }

    @Binding var text: String
    var isEditable: Bool
    var resetScrollOnExternalChange = false
    var contentStyle: ContentStyle = .plainText

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
        textView.configure(isEditable: isEditable, contentStyle: contentStyle)
        textView.delegate = context.coordinator
        textView.setStringKeepingSelection(text)

        scrollView.documentView = textView
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self

        guard let textView = scrollView.documentView as? PlainManifestTextView else { return }
        textView.configure(isEditable: isEditable, contentStyle: contentStyle)

        if textView.string != text {
            context.coordinator.isUpdatingFromSwiftUI = true
            textView.setStringKeepingSelection(text)
            context.coordinator.isUpdatingFromSwiftUI = false

            if resetScrollOnExternalChange {
                textView.scrollRangeToVisible(NSRange(location: 0, length: 0))
            }
        }

        // Recompute document geometry even when the text is unchanged. Inspector tabs can
        // reuse the same backing text while the available viewport height changes, and the
        // scroll range must track that new size.
        textView.refreshLayout()
    }
}

private final class PlainManifestTextView: NSTextView {
    private static let baseFont = NSFont.monospacedSystemFont(ofSize: 12, weight: .regular)
    private static let yamlHighlightPatterns: [(NSRegularExpression, NSColor)] = [
        (try! NSRegularExpression(pattern: #"(?m)^[ ]*---[ ]*$|^[ ]*\.\.\.[ ]*$"#), ManifestPalette.directive),
        (try! NSRegularExpression(pattern: #"(?m)(^|[\s\[\{:,])(&|[*])[A-Za-z0-9_.-]+"#), ManifestPalette.anchor),
        (try! NSRegularExpression(pattern: #"(?m)(^|[\s:\[-])(true|false|yes|no|on|off|null|~)(?=$|[\s,\]\}#])"#, options: [.caseInsensitive]), ManifestPalette.boolean),
        (try! NSRegularExpression(pattern: #"(?m)(^|[\s:\[-])[-+]?[0-9]+(\.[0-9]+)?(?=$|[\s,\]\}#])"#), ManifestPalette.number),
        (try! NSRegularExpression(pattern: #""([^"\\]|\\.)*"|'([^'\\]|\\.)*'"#), ManifestPalette.string)
    ]

    private var contentStyle: AppKitManifestTextView.ContentStyle = .plainText

    override var isOpaque: Bool { false }

    override func drawBackground(in rect: NSRect) {
        NSColor.clear.setFill()
        rect.fill()
        guard contentStyle == .yaml else { return }
        drawIndentGuides(in: rect)
        drawTabMarkers(in: rect)
    }

    func configure(isEditable: Bool, contentStyle: AppKitManifestTextView.ContentStyle) {
        self.isEditable = isEditable
        self.contentStyle = contentStyle
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

        let fullRange = NSRange(location: 0, length: storage.length)
        storage.beginEditing()
        storage.setAttributes([
            .font: Self.baseFont,
            .foregroundColor: NSColor.labelColor
        ], range: fullRange)

        if contentStyle == .yaml, storage.length > 0 {
            applyYAMLHighlighting(in: storage, fullRange: fullRange)
            applyYAMLDiagnostics(in: storage)
        }
        storage.endEditing()

        updateDocumentSize()
        invalidateIntrinsicContentSize()
        needsDisplay = true
    }

    override func didChangeText() {
        super.didChangeText()
        refreshLayout()
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

    private func applyYAMLHighlighting(in storage: NSTextStorage, fullRange: NSRange) {
        let source = storage.string
        applyYAMLKeyHighlighting(in: source, storage: storage)
        applyYAMLCommentHighlighting(in: source, storage: storage)

        for (pattern, color) in Self.yamlHighlightPatterns {
            pattern.enumerateMatches(in: source, range: fullRange) { match, _, _ in
                guard let match else { return }
                storage.addAttributes([.foregroundColor: color], range: match.range)
            }
        }
    }

    private func applyYAMLDiagnostics(in storage: NSTextStorage) {
        for diagnostic in yamlDiagnostics(in: storage.string) {
            storage.addAttributes(diagnostic.attributes, range: diagnostic.range)
        }
    }

    private func applyYAMLKeyHighlighting(in source: String, storage: NSTextStorage) {
        let nsSource = source as NSString
        let lines = nsSource.components(separatedBy: .newlines)
        var location = 0

        for line in lines {
            defer { location += nsSource.substring(with: NSRange(location: location, length: line.utf16.count)).utf16.count + 1 }
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            guard !trimmed.isEmpty, !trimmed.hasPrefix("#") else { continue }
            guard let colonOffset = yamlKeyColonOffset(in: line) else { continue }

            let keyText = String(line.prefix(colonOffset))
            let leadingTrimmedKey = keyText.trimmingCharacters(in: .whitespaces)
            guard !leadingTrimmedKey.isEmpty else { continue }

            let keyStartInLine = keyText.distance(from: keyText.startIndex, to: keyText.firstIndex(where: { !$0.isWhitespace }) ?? keyText.startIndex)
            let keyLength = max(0, colonOffset - keyStartInLine)
            guard keyLength > 0 else { continue }

            storage.addAttributes(
                [.foregroundColor: ManifestPalette.key],
                range: NSRange(location: location + keyStartInLine, length: keyLength)
            )
        }
    }

    private func applyYAMLCommentHighlighting(in source: String, storage: NSTextStorage) {
        let nsSource = source as NSString
        let lines = nsSource.components(separatedBy: .newlines)
        var location = 0

        for line in lines {
            defer { location += nsSource.substring(with: NSRange(location: location, length: line.utf16.count)).utf16.count + 1 }
            guard let commentOffset = yamlCommentOffset(in: line) else { continue }
            let commentLength = line.utf16.count - commentOffset
            guard commentLength > 0 else { continue }

            storage.addAttributes(
                [.foregroundColor: ManifestPalette.comment],
                range: NSRange(location: location + commentOffset, length: commentLength)
            )
        }
    }

    private func yamlKeyColonOffset(in line: String) -> Int? {
        var inSingleQuotes = false
        var inDoubleQuotes = false
        var escaped = false

        for (offset, character) in line.enumerated() {
            if escaped {
                escaped = false
                continue
            }

            switch character {
            case "\\" where inDoubleQuotes:
                escaped = true
            case "'" where !inDoubleQuotes:
                inSingleQuotes.toggle()
            case "\"" where !inSingleQuotes:
                inDoubleQuotes.toggle()
            case ":" where !inSingleQuotes && !inDoubleQuotes:
                let nextIndex = line.index(after: line.index(line.startIndex, offsetBy: offset))
                if nextIndex == line.endIndex || line[nextIndex].isWhitespace {
                    return offset
                }
            case "#" where !inSingleQuotes && !inDoubleQuotes:
                return nil
            default:
                break
            }
        }

        return nil
    }

    private func yamlCommentOffset(in line: String) -> Int? {
        var inSingleQuotes = false
        var inDoubleQuotes = false
        var escaped = false

        for (offset, character) in line.enumerated() {
            if escaped {
                escaped = false
                continue
            }

            switch character {
            case "\\" where inDoubleQuotes:
                escaped = true
            case "'" where !inDoubleQuotes:
                inSingleQuotes.toggle()
            case "\"" where !inSingleQuotes:
                inDoubleQuotes.toggle()
            case "#" where !inSingleQuotes && !inDoubleQuotes:
                return offset
            default:
                break
            }
        }

        return nil
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
}

private struct YAMLDiagnostic {
    enum Severity {
        case error
        case warning
    }

    let range: NSRange
    let severity: Severity

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

private func yamlDiagnostics(in source: String) -> [YAMLDiagnostic] {
    var diagnostics: [YAMLDiagnostic] = []

    source.enumerateSubstrings(in: source.startIndex..<source.endIndex, options: .byLines) { substring, lineRange, _, _ in
        guard let substring else { return }
        let absoluteRange = NSRange(lineRange, in: source)

        for (offset, character) in substring.enumerated() where character == "\t" {
            diagnostics.append(
                YAMLDiagnostic(
                    range: NSRange(location: absoluteRange.location + offset, length: 1),
                    severity: .error
                )
            )
        }

        if let problemRange = unmatchedQuoteRange(in: substring) {
            diagnostics.append(
                YAMLDiagnostic(
                    range: NSRange(location: absoluteRange.location + problemRange.location, length: problemRange.length),
                    severity: .error
                )
            )
        }
    }

    diagnostics.append(contentsOf: unmatchedFlowDelimiterDiagnostics(in: source))
    return diagnostics
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

private func unmatchedQuoteRange(in line: String) -> NSRange? {
    var inSingleQuotes = false
    var inDoubleQuotes = false
    var escaped = false
    var singleStart: Int?
    var doubleStart: Int?

    for (offset, character) in line.enumerated() {
        if escaped {
            escaped = false
            continue
        }

        switch character {
        case "\\" where inDoubleQuotes:
            escaped = true
        case "'" where !inDoubleQuotes:
            inSingleQuotes.toggle()
            singleStart = inSingleQuotes ? offset : nil
        case "\"" where !inSingleQuotes:
            inDoubleQuotes.toggle()
            doubleStart = inDoubleQuotes ? offset : nil
        case "#" where !inSingleQuotes && !inDoubleQuotes:
            return nil
        default:
            break
        }
    }

    if let singleStart {
        return NSRange(location: singleStart, length: max(1, line.count - singleStart))
    }
    if let doubleStart {
        return NSRange(location: doubleStart, length: max(1, line.count - doubleStart))
    }
    return nil
}

private func unmatchedFlowDelimiterDiagnostics(in source: String) -> [YAMLDiagnostic] {
    struct StackItem {
        let character: Character
        let offset: Int
    }

    var stack: [StackItem] = []
    var diagnostics: [YAMLDiagnostic] = []
    var inSingleQuotes = false
    var inDoubleQuotes = false
    var escaped = false

    for (offset, character) in source.enumerated() {
        if escaped {
            escaped = false
            continue
        }

        switch character {
        case "\\" where inDoubleQuotes:
            escaped = true
        case "'" where !inDoubleQuotes:
            inSingleQuotes.toggle()
        case "\"" where !inSingleQuotes:
            inDoubleQuotes.toggle()
        case "[" , "{":
            guard !inSingleQuotes && !inDoubleQuotes else { continue }
            stack.append(StackItem(character: character, offset: offset))
        case "]", "}":
            guard !inSingleQuotes && !inDoubleQuotes else { continue }
            guard let last = stack.last else {
                diagnostics.append(YAMLDiagnostic(range: NSRange(location: offset, length: 1), severity: .error))
                continue
            }
            let expected: Character = character == "]" ? "[" : "{"
            if last.character == expected {
                stack.removeLast()
            } else {
                diagnostics.append(YAMLDiagnostic(range: NSRange(location: offset, length: 1), severity: .error))
            }
        default:
            break
        }
    }

    for item in stack {
        diagnostics.append(YAMLDiagnostic(range: NSRange(location: item.offset, length: 1), severity: .error))
    }

    return diagnostics
}
