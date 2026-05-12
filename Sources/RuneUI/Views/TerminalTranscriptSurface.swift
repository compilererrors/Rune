import AppKit
import SwiftUI
import struct RuneSharedCore.RuneLargeTextIndex
import struct RuneSharedUI.RuneLargeTextSurface

struct TerminalTranscriptSurface: View {
    let text: String
    let height: CGFloat
    let resetID: String
    let fontSize: CGFloat
    var onPasteText: (String) -> Void = { _ in }
    var onResizeGrid: (Int, Int) -> Void = { _, _ in }
    @State private var isSearchVisible = false
    @State private var searchQuery = ""
    @State private var searchMatchCase = false
    @State private var selectedSearchMatchIndex = 0
    @State private var isLargeTextPinnedToBottom = true
    @State private var lastReportedGridSize: (columns: Int, rows: Int)?

    private var shouldUseLargeTextSurface: Bool {
        text.utf8.count > 250_000
    }

    var body: some View {
        let model = TerminalTranscriptRenderModel(
            text: text,
            query: searchQuery,
            matchCase: searchMatchCase,
            usesLargeTextSurface: shouldUseLargeTextSurface
        )
        let resolvedSearchMatchIndex = model.searchIndex.clampedIndex(selectedSearchMatchIndex)

        InspectorTextSurface(minHeight: height) {
            Group {
                if shouldUseLargeTextSurface, let textIndex = model.textIndex {
                    RuneLargeTextSurface(
                        index: textIndex,
                        placeholder: "No terminal output",
                        scrollTargetLine: model.scrollTargetLine(
                            selectedIndex: resolvedSearchMatchIndex,
                            isPinnedToBottom: isLargeTextPinnedToBottom
                        ),
                        showsLineNumbers: true,
                        fontSize: fontSize,
                        onNearBottomChange: { isNearBottom in
                            isLargeTextPinnedToBottom = isNearBottom
                        }
                    )
                } else {
                    TerminalTranscriptTextView(
                        text: text,
                        fontSize: fontSize,
                        searchQuery: searchQuery,
                        searchMatchCase: searchMatchCase,
                        selectedSearchMatchIndex: resolvedSearchMatchIndex,
                        onPasteText: onPasteText
                    )
                }
            }
                .id(resetID)
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
        .frame(height: height)
        .background {
            GeometryReader { proxy in
                Color.clear
                    .onAppear {
                        reportGridSize(proxy.size)
                    }
                    .onChange(of: proxy.size) { _, newSize in
                        reportGridSize(newSize)
                    }
                    .onChange(of: fontSize) { _, _ in
                        reportGridSize(proxy.size)
                    }
            }
        }
        .overlay(alignment: .topTrailing) {
            if isSearchVisible {
                searchBar(searchIndex: model.searchIndex, resolvedSearchMatchIndex: resolvedSearchMatchIndex)
                    .padding(10)
            } else {
                Button {
                    isSearchVisible = true
                } label: {
                    Image(systemName: "magnifyingglass")
                        .frame(width: 28, height: 24)
                }
                .buttonStyle(.plain)
                .background(
                    RoundedRectangle(cornerRadius: 6, style: .continuous)
                        .fill(Color(nsColor: .controlBackgroundColor).opacity(0.76))
                )
                .overlay(
                    RoundedRectangle(cornerRadius: 6, style: .continuous)
                        .stroke(Color.primary.opacity(0.16), lineWidth: 1)
                )
                .padding(10)
                .help("Find in terminal")
                .accessibilityLabel("Find in terminal")
                .keyboardShortcut("f", modifiers: [.command])
            }
        }
        .onChange(of: searchQuery) { _, _ in
            selectedSearchMatchIndex = 0
        }
        .onChange(of: searchMatchCase) { _, _ in
            selectedSearchMatchIndex = 0
        }
        .onChange(of: text) { _, _ in
            selectedSearchMatchIndex = model.searchIndex.clampedIndex(selectedSearchMatchIndex)
        }
    }

    private func searchBar(searchIndex: TerminalTranscriptSearchIndex, resolvedSearchMatchIndex: Int) -> some View {
        HStack(spacing: 8) {
            Image(systemName: "magnifyingglass")
                .foregroundStyle(.secondary)

            TextField("Find in terminal", text: $searchQuery)
                .textFieldStyle(.roundedBorder)
                .font(.system(size: 12))
                .frame(minWidth: 190, idealWidth: 240, maxWidth: 280)
                .onSubmit {
                    advanceSearch(by: 1)
                }
                .terminalSearchCursor(.arrow)

            Text(searchIndex.statusText(selectedIndex: resolvedSearchMatchIndex))
                .font(.caption.weight(.medium))
                .foregroundStyle(.secondary)
                .frame(minWidth: 56, alignment: .trailing)

            Button {
                advanceSearch(by: -1)
            } label: {
                Image(systemName: "chevron.up")
                    .frame(width: 20, height: 20)
            }
            .buttonStyle(.plain)
            .disabled(searchIndex.ranges.isEmpty)
            .help("Previous match")

            Button {
                advanceSearch(by: 1)
            } label: {
                Image(systemName: "chevron.down")
                    .frame(width: 20, height: 20)
            }
            .buttonStyle(.plain)
            .disabled(searchIndex.ranges.isEmpty)
            .help("Next match")

            Button {
                searchMatchCase.toggle()
            } label: {
                Text("Aa")
                    .font(.caption.weight(.semibold))
                    .frame(width: 24, height: 20)
            }
            .buttonStyle(.plain)
            .background(
                RoundedRectangle(cornerRadius: 5, style: .continuous)
                    .fill(searchMatchCase ? Color.accentColor.opacity(0.22) : Color.clear)
            )
            .help("Match case")

            Button {
                isSearchVisible = false
                searchQuery = ""
            } label: {
                Image(systemName: "xmark")
                    .frame(width: 20, height: 20)
            }
            .buttonStyle(.plain)
            .help("Close find")
        }
        .controlSize(.small)
        .padding(.horizontal, 10)
        .padding(.vertical, 8)
        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 8, style: .continuous))
        .overlay(
            RoundedRectangle(cornerRadius: 8, style: .continuous)
                .stroke(Color.primary.opacity(0.16), lineWidth: 1)
        )
        .shadow(color: .black.opacity(0.16), radius: 10, x: 0, y: 5)
        .contentShape(Rectangle())
        .terminalSearchCursor(.arrow)
        .onExitCommand {
            isSearchVisible = false
            searchQuery = ""
        }
    }

    private func advanceSearch(by delta: Int) {
        let searchIndex = TerminalTranscriptSearchIndex(text: text, query: searchQuery, matchCase: searchMatchCase)
        let resolvedSearchMatchIndex = searchIndex.clampedIndex(selectedSearchMatchIndex)
        let count = searchIndex.ranges.count
        guard count > 0 else { return }
        selectedSearchMatchIndex = (resolvedSearchMatchIndex + delta + count) % count
    }

    private func reportGridSize(_ size: CGSize) {
        let grid = TerminalTranscriptSurface.terminalGridSize(surfaceSize: size, fontSize: fontSize)
        guard lastReportedGridSize?.columns != grid.columns || lastReportedGridSize?.rows != grid.rows else {
            return
        }
        lastReportedGridSize = grid
        onResizeGrid(grid.columns, grid.rows)
    }

    nonisolated static func terminalGridSize(surfaceSize: CGSize, fontSize: CGFloat) -> (columns: Int, rows: Int) {
        let innerWidth = max(0, surfaceSize.width - 24)
        let innerHeight = max(0, surfaceSize.height - 20)
        let characterWidth = max(1, fontSize * 0.62)
        let lineHeight = max(1, fontSize * 1.34)
        return (
            columns: max(20, Int(innerWidth / characterWidth)),
            rows: max(4, Int(innerHeight / lineHeight))
        )
    }
}

private struct TerminalSearchCursorModifier: ViewModifier {
    let cursor: NSCursor
    @State private var didPushCursor = false

    func body(content: Content) -> some View {
        content
            .onHover { isHovering in
                if isHovering {
                    if !didPushCursor {
                        cursor.push()
                        didPushCursor = true
                    } else {
                        cursor.set()
                    }
                } else if didPushCursor {
                    NSCursor.pop()
                    didPushCursor = false
                }
            }
            .onDisappear {
                if didPushCursor {
                    NSCursor.pop()
                    didPushCursor = false
                }
            }
    }
}

private extension View {
    func terminalSearchCursor(_ cursor: NSCursor) -> some View {
        modifier(TerminalSearchCursorModifier(cursor: cursor))
    }
}

struct TerminalTranscriptRenderModel: Equatable {
    let textIndex: RuneLargeTextIndex?
    let searchIndex: TerminalTranscriptSearchIndex

    init(text: String, query: String, matchCase: Bool, usesLargeTextSurface: Bool) {
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        let shouldBuildLineIndex = usesLargeTextSurface || !trimmedQuery.isEmpty
        let textIndex = shouldBuildLineIndex ? RuneLargeTextIndex(text: text) : nil
        self.textIndex = textIndex
        self.searchIndex = TerminalTranscriptSearchIndex(
            text: text,
            textIndex: textIndex,
            normalizedQuery: trimmedQuery,
            matchCase: matchCase
        )
    }

    func scrollTargetLine(selectedIndex: Int, isPinnedToBottom: Bool = true) -> Int? {
        if let matchLineNumber = searchIndex.matchLineNumber(selectedIndex: selectedIndex) {
            return matchLineNumber
        }
        guard isPinnedToBottom else { return nil }
        return textIndex?.lineCount
    }
}

struct TerminalTranscriptSearchIndex: Equatable {
    let ranges: [NSRange]
    let matchLineNumbers: [Int]
    private let hasQuery: Bool

    init(text: String, query: String, matchCase: Bool) {
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        self.init(text: text, textIndex: nil, normalizedQuery: trimmedQuery, matchCase: matchCase)
    }

    init(
        text: String,
        textIndex providedTextIndex: RuneLargeTextIndex?,
        normalizedQuery trimmedQuery: String,
        matchCase: Bool
    ) {
        hasQuery = !trimmedQuery.isEmpty
        guard !trimmedQuery.isEmpty else {
            ranges = []
            matchLineNumbers = []
            return
        }

        let index = providedTextIndex ?? RuneLargeTextIndex(text: text)
        let options: NSString.CompareOptions = matchCase ? [] : [.caseInsensitive]
        let result = index.search(query: trimmedQuery, options: options)
        ranges = result.matches.map(\.range)
        matchLineNumbers = result.matches.map(\.lineNumber)
    }

    func clampedIndex(_ index: Int) -> Int {
        guard !ranges.isEmpty else { return 0 }
        return min(max(index, 0), ranges.count - 1)
    }

    func statusText(selectedIndex: Int) -> String {
        guard hasQuery else { return "" }
        guard !ranges.isEmpty else { return "No matches" }
        return "\(clampedIndex(selectedIndex) + 1) of \(ranges.count)"
    }

    func matchLineNumber(selectedIndex: Int) -> Int? {
        guard !matchLineNumbers.isEmpty else { return nil }
        return matchLineNumbers[clampedIndex(selectedIndex)]
    }
}

private struct TerminalTranscriptTextView: NSViewRepresentable {
    let text: String
    let fontSize: CGFloat
    let searchQuery: String
    let searchMatchCase: Bool
    let selectedSearchMatchIndex: Int
    let onPasteText: (String) -> Void

    func makeCoordinator() -> Coordinator {
        Coordinator()
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

        let textView = TerminalTranscriptInteractionTextView(frame: .zero)
        textView.onPasteText = onPasteText
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
        textView.isHorizontallyResizable = true
        textView.isVerticallyResizable = true
        textView.minSize = .zero
        textView.maxSize = NSSize(width: CGFloat.greatestFiniteMagnitude, height: CGFloat.greatestFiniteMagnitude)
        textView.autoresizingMask = [.width, .height]
        textView.textContainerInset = NSSize(width: 10, height: 10)
        textView.backgroundColor = .clear
        textView.drawsBackground = false
        textView.font = terminalFont
        textView.textColor = .labelColor

        if let container = textView.textContainer {
            container.widthTracksTextView = false
            container.heightTracksTextView = false
            container.containerSize = NSSize(
                width: CGFloat.greatestFiniteMagnitude,
                height: CGFloat.greatestFiniteMagnitude
            )
            container.lineFragmentPadding = 0
        }

        textView.string = text
        scrollView.documentView = textView
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        guard let textView = scrollView.documentView as? NSTextView else { return }
        if let terminalTextView = textView as? TerminalTranscriptInteractionTextView {
            terminalTextView.onPasteText = onPasteText
        }
        updateFontIfNeeded(in: textView, coordinator: context.coordinator)

        if let container = textView.textContainer {
            let contentSize = NSSize(
                width: CGFloat.greatestFiniteMagnitude,
                height: CGFloat.greatestFiniteMagnitude
            )
            if container.containerSize != contentSize {
                container.containerSize = contentSize
            }
        }

        let shouldStickToBottom = isNearBottom(scrollView)
        if textView.string != text {
            if text.hasPrefix(textView.string),
               let textStorage = textView.textStorage {
                let suffix = String(text.dropFirst(textView.string.count))
                textStorage.append(
                    NSAttributedString(
                        string: suffix,
                            attributes: [
                            .font: textView.font ?? terminalFont,
                            .foregroundColor: textView.textColor ?? NSColor.labelColor
                        ]
                    )
                )
            } else {
                textView.string = text
            }
            textView.layoutManager?.ensureLayout(for: textView.textContainer!)
            applySearchHighlights(in: textView, coordinator: context.coordinator)
            textView.invalidateIntrinsicContentSize()
            textView.layoutSubtreeIfNeeded()
            if shouldStickToBottom {
                let range = NSRange(location: max(0, text.utf16.count - 1), length: 0)
                textView.scrollRangeToVisible(range)
            }
        } else {
            textView.layoutManager?.ensureLayout(for: textView.textContainer!)
            applySearchHighlights(in: textView, coordinator: context.coordinator)
        }
    }

    private func isNearBottom(_ scrollView: NSScrollView) -> Bool {
        guard let documentView = scrollView.documentView else { return true }
        let clipBounds = scrollView.contentView.bounds
        let maxOffset = max(0, documentView.frame.maxY - clipBounds.height)
        return maxOffset - clipBounds.origin.y < 28
    }

    private func applySearchHighlights(in textView: NSTextView, coordinator: Coordinator) {
        guard let storage = textView.textStorage else { return }
        let fullRange = NSRange(location: 0, length: storage.length)
        let trimmedQuery = searchQuery.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedQuery.isEmpty else {
            if coordinator.hasAppliedSearchHighlights {
                storage.removeAttribute(.backgroundColor, range: fullRange)
                coordinator.hasAppliedSearchHighlights = false
                coordinator.lastAppliedSearchKey = ""
            }
            return
        }

        let searchKey = "\(textView.string.hashValue):\(trimmedQuery):\(searchMatchCase):\(selectedSearchMatchIndex)"
        guard coordinator.lastAppliedSearchKey != searchKey else { return }
        coordinator.lastAppliedSearchKey = searchKey

        storage.removeAttribute(.backgroundColor, range: fullRange)

        let index = TerminalTranscriptSearchIndex(
            text: textView.string,
            query: trimmedQuery,
            matchCase: searchMatchCase
        )
        guard !index.ranges.isEmpty else {
            coordinator.hasAppliedSearchHighlights = false
            return
        }

        let highlightColor = NSColor.systemYellow.withAlphaComponent(0.28)
        let activeColor = NSColor.controlAccentColor.withAlphaComponent(0.42)
        for (offset, range) in index.ranges.enumerated() where NSMaxRange(range) <= storage.length {
            storage.addAttribute(
                .backgroundColor,
                value: offset == index.clampedIndex(selectedSearchMatchIndex) ? activeColor : highlightColor,
                range: range
            )
        }

        let activeRange = index.ranges[index.clampedIndex(selectedSearchMatchIndex)]
        textView.scrollRangeToVisible(activeRange)
        coordinator.hasAppliedSearchHighlights = true
    }

    final class Coordinator {
        var lastAppliedSearchKey = ""
        var hasAppliedSearchHighlights = false
        var lastFontSize: CGFloat?
    }

    private var terminalFont: NSFont {
        NSFont.monospacedSystemFont(ofSize: fontSize, weight: .regular)
    }

    private func updateFontIfNeeded(in textView: NSTextView, coordinator: Coordinator) {
        guard coordinator.lastFontSize != fontSize else { return }
        coordinator.lastFontSize = fontSize
        let font = terminalFont
        textView.font = font
        if let storage = textView.textStorage {
            storage.addAttribute(.font, value: font, range: NSRange(location: 0, length: storage.length))
        }
        textView.invalidateIntrinsicContentSize()
        textView.needsLayout = true
        coordinator.lastAppliedSearchKey = ""
    }
}

private final class TerminalTranscriptInteractionTextView: NSTextView {
    var onPasteText: ((String) -> Void)?

    override var acceptsFirstResponder: Bool {
        true
    }

    override func performKeyEquivalent(with event: NSEvent) -> Bool {
        let flags = event.modifierFlags.intersection(.deviceIndependentFlagsMask)
        guard flags.contains(.command),
              let key = event.charactersIgnoringModifiers?.lowercased() else {
            return super.performKeyEquivalent(with: event)
        }

        switch key {
        case "a":
            selectAll(nil)
            return true
        case "c":
            guard selectedRange().length > 0 else { return false }
            copy(nil)
            return true
        case "v":
            guard let paste = NSPasteboard.general.string(forType: .string),
                  !paste.isEmpty else {
                return false
            }
            onPasteText?(paste)
            return true
        default:
            return super.performKeyEquivalent(with: event)
        }
    }

    override func paste(_ sender: Any?) {
        guard let paste = NSPasteboard.general.string(forType: .string),
              !paste.isEmpty else {
            NSSound.beep()
            return
        }
        onPasteText?(paste)
    }

    override func menu(for event: NSEvent) -> NSMenu? {
        let menu = NSMenu()
        let hasSelection = selectedRange().length > 0
        let hasPasteText = NSPasteboard.general.string(forType: .string)?.isEmpty == false

        let copyItem = NSMenuItem(title: "Copy", action: #selector(NSText.copy(_:)), keyEquivalent: "")
        copyItem.target = self
        copyItem.isEnabled = hasSelection
        menu.addItem(copyItem)

        let pasteItem = NSMenuItem(title: "Paste to Prompt", action: #selector(paste(_:)), keyEquivalent: "")
        pasteItem.target = self
        pasteItem.isEnabled = hasPasteText && onPasteText != nil
        menu.addItem(pasteItem)

        menu.addItem(.separator())

        let selectAllItem = NSMenuItem(title: "Select All", action: #selector(NSText.selectAll(_:)), keyEquivalent: "")
        selectAllItem.target = self
        selectAllItem.isEnabled = !string.isEmpty
        menu.addItem(selectAllItem)

        let clearSelectionItem = NSMenuItem(title: "Clear Selection", action: #selector(clearSelection(_:)), keyEquivalent: "")
        clearSelectionItem.target = self
        clearSelectionItem.isEnabled = hasSelection
        menu.addItem(clearSelectionItem)

        return menu
    }

    @objc private func clearSelection(_ sender: Any?) {
        setSelectedRange(NSRange(location: selectedRange().location, length: 0))
    }
}
