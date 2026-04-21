import AppKit
import SwiftUI

struct AppKitManifestTextView: NSViewRepresentable {
    enum ContentStyle: Sendable {
        case yaml
        case plainText
    }

    @Binding var text: String
    var isEditable: Bool
    var contentStyle: ContentStyle = .yaml

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

        let textView = YAMLTextView(frame: .zero)
        textView.configure(isEditable: isEditable, contentStyle: contentStyle)
        textView.delegate = context.coordinator
        textView.setStringKeepingSelection(text)

        scrollView.documentView = textView
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self

        guard let textView = scrollView.documentView as? YAMLTextView else { return }
        textView.configure(isEditable: isEditable, contentStyle: contentStyle)

        if textView.string != text {
            context.coordinator.isUpdatingFromSwiftUI = true
            textView.setStringKeepingSelection(text)
            context.coordinator.isUpdatingFromSwiftUI = false
        }
    }

    final class Coordinator: NSObject, NSTextViewDelegate {
        var parent: AppKitManifestTextView
        var isUpdatingFromSwiftUI = false

        init(parent: AppKitManifestTextView) {
            self.parent = parent
        }

        func textDidChange(_ notification: Notification) {
            guard !isUpdatingFromSwiftUI,
                  let textView = notification.object as? YAMLTextView
            else { return }

            textView.refreshPresentation()
            parent.text = textView.string
        }
    }
}

private final class YAMLTextView: NSTextView {
    private static let baseFont = NSFont.monospacedSystemFont(ofSize: 12, weight: .regular)
    private static let palette = YAMLPalette()
    private static let highlightPatterns: [(NSRegularExpression, NSColor)] = [
        (try! NSRegularExpression(pattern: #"(?m)^[ ]*---[ ]*$|^[ ]*\.\.\.[ ]*$"#), YAMLPalette.directive),
        (try! NSRegularExpression(pattern: #"(?m)(^|[\s\[\{:,])(&|[*])[A-Za-z0-9_.-]+"#), YAMLPalette.anchor),
        (try! NSRegularExpression(pattern: #"(?m)(^|[\s:\[-])(true|false|yes|no|on|off|null|~)(?=$|[\s,\]\}#])"#, options: [.caseInsensitive]), YAMLPalette.boolean),
        (try! NSRegularExpression(pattern: #"(?m)(^|[\s:\[-])[-+]?[0-9]+(\.[0-9]+)?(?=$|[\s,\]\}#])"#), YAMLPalette.number),
        (try! NSRegularExpression(pattern: #""([^"\\]|\\.)*"|'([^'\\]|\\.)*'"#), YAMLPalette.string)
    ]

    private var isApplyingPresentation = false
    private var contentStyle: AppKitManifestTextView.ContentStyle = .yaml

    override var isOpaque: Bool { false }

    override func drawBackground(in rect: NSRect) {
        NSColor.clear.setFill()
        rect.fill()
        guard contentStyle == .yaml else { return }
        drawIndentGuides(in: rect)
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
        textContainerInset = NSSize(width: 14, height: 12)
        backgroundColor = .clear
        drawsBackground = false
        insertionPointColor = .controlAccentColor
        selectedTextAttributes = [
            .backgroundColor: NSColor.controlAccentColor.withAlphaComponent(0.22)
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

        typingAttributes = [
            .font: Self.baseFont,
            .foregroundColor: Self.palette.plain
        ]

        refreshPresentation()
    }

    func setStringKeepingSelection(_ newValue: String) {
        let selected = selectedRanges
        string = newValue
        refreshPresentation()
        if !selected.isEmpty {
            selectedRanges = selected
        }
    }

    func refreshPresentation() {
        guard !isApplyingPresentation else { return }
        guard let storage = textStorage else { return }

        isApplyingPresentation = true
        defer {
            isApplyingPresentation = false
            invalidateIntrinsicContentSize()
            needsDisplay = true
        }

        let fullRange = NSRange(location: 0, length: storage.length)
        storage.beginEditing()
        storage.setAttributes([
            .font: Self.baseFont,
            .foregroundColor: Self.palette.plain
        ], range: fullRange)

        if contentStyle == .yaml {
            applyKeyHighlight(in: storage.string, storage: storage)
            applyCommentHighlight(in: storage.string, storage: storage)

            for (pattern, color) in Self.highlightPatterns {
                pattern.enumerateMatches(in: storage.string, range: fullRange) { match, _, _ in
                    guard let match else { return }
                    let range = match.numberOfRanges > 1 ? match.range(at: 0) : match.range
                    storage.addAttributes([.foregroundColor: color], range: range)
                }
            }

            for diagnostic in yamlDiagnostics(in: storage.string) {
                storage.addAttributes([
                    .underlineStyle: NSUnderlineStyle.single.rawValue,
                    .underlineColor: NSColor.systemRed,
                    .foregroundColor: diagnostic.severity == .error ? NSColor.systemRed : Self.palette.plain
                ], range: diagnostic.range)
            }
        }
        storage.endEditing()

        updateDocumentSize()
    }

    override func didChangeText() {
        super.didChangeText()
        guard !isApplyingPresentation else { return }
        refreshPresentation()
    }

    override func viewDidEndLiveResize() {
        super.viewDidEndLiveResize()
        updateDocumentSize()
    }

    private func applyKeyHighlight(in source: String, storage: NSTextStorage) {
        let nsSource = source as NSString
        source.enumerateSubstrings(in: source.startIndex..<source.endIndex, options: .byLines) { substring, lineRange, _, _ in
            guard let substring else { return }
            let trimmed = substring.trimmingCharacters(in: .whitespaces)
            guard !trimmed.isEmpty, !trimmed.hasPrefix("#") else { return }

            let keyStartOffset = substring.prefix { $0 == " " || $0 == "\t" || $0 == "-" }.count
            let keyPortion = substring.dropFirst(keyStartOffset)
            guard let colonOffset = keyPortion.firstIndex(of: ":") else { return }

            let rawKey = keyPortion[..<colonOffset].trimmingCharacters(in: .whitespaces)
            guard !rawKey.isEmpty else { return }

            let keyRangeInLine = NSRange(
                location: keyStartOffset + keyPortion.distance(from: keyPortion.startIndex, to: keyPortion.firstIndex(of: rawKey.first!) ?? keyPortion.startIndex),
                length: rawKey.count
            )
            let lineNSRange = NSRange(lineRange, in: source)
            let absoluteRange = NSRange(location: lineNSRange.location + keyRangeInLine.location, length: keyRangeInLine.length)
            guard NSMaxRange(absoluteRange) <= nsSource.length else { return }
            storage.addAttributes([
                .foregroundColor: YAMLPalette.key
            ], range: absoluteRange)
        }
    }

    private func applyCommentHighlight(in source: String, storage: NSTextStorage) {
        source.enumerateSubstrings(in: source.startIndex..<source.endIndex, options: .byLines) { substring, lineRange, _, _ in
            guard let substring, let commentOffset = commentStartIndex(in: substring) else { return }
            let lineNSRange = NSRange(lineRange, in: source)
            let absoluteRange = NSRange(
                location: lineNSRange.location + commentOffset,
                length: substring.count - commentOffset
            )
            storage.addAttributes([
                .foregroundColor: YAMLPalette.comment
            ], range: absoluteRange)
        }
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

    private func drawIndentGuides(in dirtyRect: NSRect) {
        guard let layoutManager, let visibleRange = visibleCharacterRange else { return }

        let guideColor = NSColor.separatorColor.withAlphaComponent(0.16)
        let path = NSBezierPath()
        let indentWidth = max(8, (Self.baseFont.advancement(forGlyph: 32).width * 2).rounded(.up))
        let insetX = textContainerInset.width

        let glyphRange = layoutManager.glyphRange(forCharacterRange: visibleRange, actualCharacterRange: nil)
        layoutManager.enumerateLineFragments(forGlyphRange: glyphRange) { _, usedRect, _, glyphRange, _ in
            let characterRange = layoutManager.characterRange(forGlyphRange: glyphRange, actualGlyphRange: nil)
            let lineString = (self.string as NSString).substring(with: characterRange)
            let leadingColumns = indentationColumns(in: lineString)
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

    private var visibleCharacterRange: NSRange? {
        guard let layoutManager, let textContainer, let scrollView = enclosingScrollView else { return nil }
        let visibleRect = scrollView.contentView.bounds
        let glyphRange = layoutManager.glyphRange(forBoundingRect: visibleRect, in: textContainer)
        return layoutManager.characterRange(forGlyphRange: glyphRange, actualGlyphRange: nil)
    }
}

private struct YAMLPalette {
    static let key = NSColor.systemBlue
    static let string = NSColor.systemGreen
    static let number = NSColor.systemOrange
    static let boolean = NSColor.systemPurple
    static let comment = NSColor.secondaryLabelColor
    static let directive = NSColor.systemPink
    static let anchor = NSColor.systemTeal

    let plain = NSColor.labelColor
}

private struct YAMLDiagnostic {
    enum Severity {
        case error
        case warning
    }

    let range: NSRange
    let severity: Severity
}

private func yamlDiagnostics(in source: String) -> [YAMLDiagnostic] {
    var diagnostics: [YAMLDiagnostic] = []
    let nsSource = source as NSString

    source.enumerateSubstrings(in: source.startIndex..<source.endIndex, options: .byLines) { substring, lineRange, _, _ in
        guard let substring else { return }
        let absoluteRange = NSRange(lineRange, in: source)

        if let tabIndex = substring.firstIndex(of: "\t") {
            let offset = substring.distance(from: substring.startIndex, to: tabIndex)
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

    diagnostics.append(contentsOf: unmatchedFlowDelimiterDiagnostics(in: source, nsSource: nsSource))
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

private func commentStartIndex(in line: String) -> Int? {
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

private func unmatchedFlowDelimiterDiagnostics(in source: String, nsSource: NSString) -> [YAMLDiagnostic] {
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
        case "#" where !inSingleQuotes && !inDoubleQuotes:
            let remaining = nsSource.substring(from: offset)
            if let newline = remaining.firstIndex(of: "\n") {
                let delta = remaining.distance(from: remaining.startIndex, to: newline)
                if delta > 0 {
                    continue
                }
            }
        case "[", "{":
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
