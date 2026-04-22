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
        }
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
}

private struct ManifestPalette {
    static let key = NSColor.systemBlue
    static let string = NSColor.systemGreen
    static let number = NSColor.systemOrange
    static let boolean = NSColor.systemPurple
    static let comment = NSColor.secondaryLabelColor
    static let directive = NSColor.systemPink
    static let anchor = NSColor.systemTeal
}
