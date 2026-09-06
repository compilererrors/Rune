import AppKit
import SwiftUI
import RuneCore
import struct RuneSharedCore.RuneLargeTextIndex
import struct RuneSharedUI.RuneLargeTextSurface

struct TerminalTranscriptSurface: View {
    let text: String
    let height: CGFloat
    let resetID: String
    let fontSize: CGFloat
    var onPasteText: (String) -> Void = { _ in }
    var onInputSequence: (String) -> Void = { _ in }
    var onResizeGrid: (Int, Int) -> Void = { _, _ in }
    @State private var isSearchVisible = false
    @State private var searchQuery = ""
    @State private var searchMatchCase = false
    @State private var selectedSearchMatchIndex = 0
    @State private var searchNavigationRevision = 0
    @State private var publishedRenderModel: TerminalTranscriptRenderModel?
    @State private var publishedSearchRevision = 0
    @State private var searchTask: Task<Void, Never>?
    @State private var searchLifecycleGeneration = 0
    @State private var searchWorkLane =
        RuneSingleFlightLatestPendingWorkLane<TerminalTranscriptRenderModel>()
    @State private var isLargeTextPinnedToBottom = true
    @State private var lastReportedGridSize: (columns: Int, rows: Int)?
    @Environment(\.accessibilityReduceMotion) private var accessibilityReduceMotion
    @Environment(\.runeThemePalette) private var runeThemePalette

    private var shouldUseLargeTextSurface: Bool {
        text.utf8.count > 250_000
    }

    private var hasTrimmedScrollback: Bool {
        Self.hasTrimmedScrollback(text)
    }

    var body: some View {
        let warning = RuneSemanticColorRole.warning.color(in: runeThemePalette)
        let activeModel = publishedRenderModel.flatMap { model in
            model.isSearchNavigableSnapshot(
                for: text,
                query: searchQuery,
                matchCase: searchMatchCase
            )
                ? model
                : nil
        }
        let activeSearchIndex = activeModel?.searchIndex
        let resolvedSearchMatchIndex = activeSearchIndex?.clampedIndex(selectedSearchMatchIndex) ?? 0
        let displayedTextIndex = publishedRenderModel.flatMap { model in
            model.isRenderableSnapshot(for: text) ? model.textIndex : nil
        }
        let normalizedQuery = searchQuery.trimmingCharacters(in: .whitespacesAndNewlines)
        let largeTextScrollTargetLine: Int? = if !normalizedQuery.isEmpty {
            activeModel?.scrollTargetLine(
                selectedIndex: resolvedSearchMatchIndex,
                isPinnedToBottom: false
            )
        } else if isLargeTextPinnedToBottom {
            displayedTextIndex?.lineCount
        } else {
            nil
        }

        InspectorTextSurface(minHeight: height) {
            Group {
                if shouldUseLargeTextSurface {
                    if let displayedTextIndex {
                        RuneLargeTextSurface(
                            index: displayedTextIndex,
                            placeholder: "No terminal output",
                            scrollTargetLine: largeTextScrollTargetLine,
                            scrollTargetRevision: Self.largeTextSearchNavigationRevision(
                                normalizedQuery: normalizedQuery,
                                searchIndex: activeSearchIndex,
                                navigationRevision: searchNavigationRevision
                            ),
                            scrollsOnTargetLineChange: false,
                            searchMatchRanges: activeSearchIndex?.ranges ?? [],
                            selectedSearchMatchIndex: resolvedSearchMatchIndex,
                            horizontalContentInset: RuneUILayoutMetrics.inspectorDocumentHorizontalInset,
                            verticalContentInset: RuneUILayoutMetrics.inspectorDocumentVerticalInset,
                            showsLineNumbers: false,
                            fontSize: fontSize,
                            onNearBottomChange: { isNearBottom in
                                isLargeTextPinnedToBottom = isNearBottom
                            }
                        )
                    } else {
                        ProgressView()
                            .controlSize(.small)
                            .frame(maxWidth: .infinity, maxHeight: .infinity)
                            .accessibilityLabel("Preparing terminal output")
                    }
                } else {
                    TerminalTranscriptTextView(
                        text: text,
                        fontSize: fontSize,
                        searchIndex: activeSearchIndex,
                        searchResultRevision: publishedSearchRevision,
                        selectedSearchMatchIndex: resolvedSearchMatchIndex,
                        searchNavigationRevision: searchNavigationRevision,
                        reduceMotion: accessibilityReduceMotion,
                        onPasteText: onPasteText,
                        onInputSequence: onInputSequence
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
                TerminalTranscriptSearchBar(
                    searchIndex: activeSearchIndex,
                    resolvedSearchMatchIndex: resolvedSearchMatchIndex,
                    query: $searchQuery,
                    matchCase: $searchMatchCase,
                    onPrevious: { advanceSearch(by: -1, in: activeSearchIndex) },
                    onNext: { advanceSearch(by: 1, in: activeSearchIndex) },
                    onClose: {
                        isSearchVisible = false
                        searchQuery = ""
                    }
                )
                    .padding(RuneUILayoutMetrics.inspectorOverlayInset)
            } else {
                RuneIconButton("Find in terminal", systemImage: "magnifyingglass") {
                    isSearchVisible = true
                }
                .background(
                    RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous)
                        .fill(Color(nsColor: .controlBackgroundColor).opacity(0.76))
                )
                .overlay(
                    RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous)
                        .stroke(Color.primary.opacity(0.16), lineWidth: 1)
                )
                .padding(RuneUILayoutMetrics.inspectorOverlayInset)
                .keyboardShortcut("f", modifiers: [.command])
                .runePointerCursor()
            }
        }
        .overlay(alignment: .topLeading) {
            if hasTrimmedScrollback {
                Label("Scrollback trimmed", systemImage: "scissors")
                    .font(.caption2.weight(.semibold))
                    .padding(.horizontal, 8)
                    .padding(.vertical, 5)
                    .background(
                        RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous)
                            .fill(Color(nsColor: .controlBackgroundColor).opacity(0.84))
                    )
                    .overlay(
                        RoundedRectangle(cornerRadius: RuneUILayoutMetrics.compactGlyphCornerRadius, style: .continuous)
                            .stroke(warning.opacity(0.42), lineWidth: 1)
                    )
                    .foregroundStyle(warning)
                    .padding(RuneUILayoutMetrics.inspectorOverlayInset)
                    .help("Older terminal output was discarded by the scrollback limit")
                    .accessibilityLabel("Scrollback trimmed")
                    .runePointerCursor()
            }
        }
        .onChange(of: searchQuery) { _, _ in
            selectedSearchMatchIndex = 0
            searchNavigationRevision &+= 1
            scheduleSearch(debounceNanoseconds: TerminalTranscriptSearchWork.queryDebounceNanoseconds)
        }
        .onChange(of: searchMatchCase) { _, _ in
            selectedSearchMatchIndex = 0
            searchNavigationRevision &+= 1
            scheduleSearch(debounceNanoseconds: TerminalTranscriptSearchWork.queryDebounceNanoseconds)
        }
        .onChange(of: text) { _, _ in
            scheduleSearch(debounceNanoseconds: TerminalTranscriptSearchWork.textDebounceNanoseconds)
        }
        .onAppear {
            scheduleSearch(debounceNanoseconds: 0)
        }
        .onDisappear {
            searchLifecycleGeneration &+= 1
            searchTask?.cancel()
            Task {
                await searchWorkLane.cancel()
            }
        }
    }

    private func advanceSearch(by delta: Int, in searchIndex: TerminalTranscriptSearchIndex?) {
        guard let searchIndex else { return }
        let resolvedSearchMatchIndex = searchIndex.clampedIndex(selectedSearchMatchIndex)
        let count = searchIndex.ranges.count
        guard count > 0 else { return }
        selectedSearchMatchIndex = (resolvedSearchMatchIndex + delta + count) % count
        searchNavigationRevision &+= 1
    }

    private func scheduleSearch(debounceNanoseconds: UInt64) {
        searchTask?.cancel()
        let requestedText = text
        let requestedQuery = searchQuery
        let requestedMatchCase = searchMatchCase
        let usesLargeTextSurface = shouldUseLargeTextSurface
        let lifecycleGeneration = searchLifecycleGeneration
        let reusableTextIndex = publishedRenderModel?.originalText == requestedText
            ? publishedRenderModel?.textIndex
            : nil

        let task = Task { @MainActor in
            if debounceNanoseconds > 0 {
                try? await Task.sleep(nanoseconds: debounceNanoseconds)
            }
            guard !Task.isCancelled,
                  searchLifecycleGeneration == lifecycleGeneration else {
                return
            }
            if debounceNanoseconds > 0 {
                searchTask = nil
            }

            guard let result = await searchWorkLane.submit(
                priority: .userInitiated,
                operation: {
                    try TerminalTranscriptRenderModel(
                        text: requestedText,
                        query: requestedQuery,
                        matchCase: requestedMatchCase,
                        usesLargeTextSurface: usesLargeTextSurface,
                        reusableTextIndex: reusableTextIndex,
                        cancellationCheck: { try Task.checkCancellation() }
                    )
                }
            ) else {
                return
            }

            guard searchLifecycleGeneration == lifecycleGeneration,
                  searchQuery == requestedQuery,
                  searchMatchCase == requestedMatchCase else {
                return
            }
            publishedRenderModel = result
            publishedSearchRevision &+= 1
            selectedSearchMatchIndex = result.searchIndex.clampedIndex(selectedSearchMatchIndex)
        }
        if debounceNanoseconds > 0 {
            searchTask = task
        }
    }

    private func reportGridSize(_ size: CGSize) {
        let grid = TerminalTranscriptSurface.terminalGridSize(surfaceSize: size, fontSize: fontSize)
        guard lastReportedGridSize?.columns != grid.columns || lastReportedGridSize?.rows != grid.rows else {
            return
        }
        lastReportedGridSize = grid
        onResizeGrid(grid.columns, grid.rows)
    }

    static func largeTextSearchNavigationRevision(
        normalizedQuery: String,
        searchIndex: TerminalTranscriptSearchIndex?,
        navigationRevision: Int
    ) -> Int? {
        guard !normalizedQuery.isEmpty, searchIndex != nil else { return nil }
        return navigationRevision
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

    nonisolated static func hasTrimmedScrollback(_ text: String) -> Bool {
        text.hasPrefix(TerminalScrollbackRetention.truncationMarker)
    }
}

struct TerminalTranscriptSearchBar: View {
    let searchIndex: TerminalTranscriptSearchIndex?
    let resolvedSearchMatchIndex: Int
    @Binding var query: String
    @Binding var matchCase: Bool
    let onPrevious: () -> Void
    let onNext: () -> Void
    let onClose: () -> Void
    @FocusState private var isSearchFieldFocused: Bool

    var body: some View {
        RuneFindBarChrome("Terminal find controls") {
            searchField
        } secondary: {
            navigationControls
        }
        .onExitCommand(perform: onClose)
        .onAppear {
            focusSearchField()
        }
        .onChange(of: matchCase) { _, _ in
            focusSearchField()
        }
    }

    private var searchField: some View {
        RuneInspectorControlGridRow {
            Image(systemName: "magnifyingglass")
                .foregroundStyle(.runeSecondary)
        } content: {
            TextField("Find in terminal", text: $query)
                .textFieldStyle(.roundedBorder)
                .font(.system(size: 12))
                .frame(
                    minWidth: RuneFindBarMetrics.minimumSearchFieldWidth,
                    idealWidth: RuneFindBarMetrics.idealSearchFieldWidth,
                    maxWidth: .infinity
                )
                .focused($isSearchFieldFocused)
                .onSubmit {
                    navigate(onNext)
                }
                .runeTextInputCursor()
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var navigationControls: some View {
        HStack(spacing: 8) {
            Text(statusText)
                .font(.caption.weight(.medium))
                .foregroundStyle(.runeSecondary)
                .monospacedDigit()
                .frame(minWidth: 56, alignment: .trailing)

            RuneIconButton(
                "Previous match",
                systemImage: "chevron.up",
                isDisabled: searchIndex?.ranges.isEmpty != false,
                action: { navigate(onPrevious) }
            )

            RuneIconButton(
                "Next match",
                systemImage: "chevron.down",
                isDisabled: searchIndex?.ranges.isEmpty != false,
                action: { navigate(onNext) }
            )

            RuneMatchCaseButton(isSelected: $matchCase)

            RuneIconButton("Close find", systemImage: "xmark", action: onClose)
        }
        .fixedSize(horizontal: true, vertical: false)
    }

    private var statusText: String {
        guard !query.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return "" }
        guard let searchIndex else { return "…" }
        return searchIndex.statusText(selectedIndex: resolvedSearchMatchIndex)
    }

    private func navigate(_ action: () -> Void) {
        action()
        focusSearchField()
    }

    private func focusSearchField() {
        isSearchFieldFocused = true
        DispatchQueue.main.async {
            isSearchFieldFocused = true
        }
    }
}

struct TerminalTranscriptRenderModel: Equatable, Sendable {
    let originalText: String
    let query: String
    let matchCase: Bool
    let textIndex: RuneLargeTextIndex?
    let searchIndex: TerminalTranscriptSearchIndex

    init(text: String, query: String, matchCase: Bool, usesLargeTextSurface: Bool) {
        self.init(
            text: text,
            query: query,
            matchCase: matchCase,
            usesLargeTextSurface: usesLargeTextSurface,
            reusableTextIndex: nil,
            cancellationCheck: {}
        )
    }

    init(
        text: String,
        query: String,
        matchCase: Bool,
        usesLargeTextSurface: Bool,
        reusableTextIndex: RuneLargeTextIndex?,
        cancellationCheck: () throws -> Void
    ) rethrows {
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        originalText = text
        self.query = trimmedQuery
        self.matchCase = matchCase
        let shouldBuildLineIndex = usesLargeTextSurface || !trimmedQuery.isEmpty
        let textIndex: RuneLargeTextIndex?
        if !shouldBuildLineIndex {
            textIndex = nil
        } else if let reusableTextIndex, reusableTextIndex.text == text {
            try cancellationCheck()
            textIndex = reusableTextIndex
        } else {
            textIndex = try RuneLargeTextIndex(
                text: text,
                cancellationCheck: cancellationCheck
            )
        }
        self.textIndex = textIndex
        self.searchIndex = try TerminalTranscriptSearchIndex(
            text: text,
            textIndex: textIndex,
            normalizedQuery: trimmedQuery,
            matchCase: matchCase,
            cancellationCheck: cancellationCheck
        )
    }

    func isCurrent(text: String, query: String, matchCase: Bool) -> Bool {
        originalText == text
            && self.query == query.trimmingCharacters(in: .whitespacesAndNewlines)
            && self.matchCase == matchCase
    }

    func isRenderableSnapshot(for text: String) -> Bool {
        originalText == text
            || (!originalText.isEmpty && text.hasPrefix(originalText))
    }

    func isSearchNavigableSnapshot(
        for text: String,
        query: String,
        matchCase: Bool
    ) -> Bool {
        isRenderableSnapshot(for: text)
            && self.query == query.trimmingCharacters(in: .whitespacesAndNewlines)
            && self.matchCase == matchCase
    }

    func scrollTargetLine(selectedIndex: Int, isPinnedToBottom: Bool = true) -> Int? {
        if let matchLineNumber = searchIndex.matchLineNumber(selectedIndex: selectedIndex) {
            return matchLineNumber
        }
        guard isPinnedToBottom else { return nil }
        return textIndex?.lineCount
    }
}

struct TerminalTranscriptSearchIndex: Equatable, Sendable {
    let textIndex: RuneLargeTextIndex?
    let ranges: [NSRange]
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
        self.init(
            text: text,
            textIndex: providedTextIndex,
            normalizedQuery: trimmedQuery,
            matchCase: matchCase,
            cancellationCheck: {}
        )
    }

    init(
        text: String,
        textIndex providedTextIndex: RuneLargeTextIndex?,
        normalizedQuery trimmedQuery: String,
        matchCase: Bool,
        cancellationCheck: () throws -> Void
    ) rethrows {
        hasQuery = !trimmedQuery.isEmpty
        guard !trimmedQuery.isEmpty else {
            textIndex = providedTextIndex
            ranges = []
            return
        }

        let index: RuneLargeTextIndex
        if let providedTextIndex {
            try cancellationCheck()
            index = providedTextIndex
        } else {
            index = try RuneLargeTextIndex(
                text: text,
                cancellationCheck: cancellationCheck
            )
        }
        textIndex = index
        let options: NSString.CompareOptions = matchCase ? [] : [.caseInsensitive]
        ranges = try index.searchRanges(
            query: trimmedQuery,
            options: options,
            cancellationCheck: cancellationCheck
        )
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
        guard let textIndex, !ranges.isEmpty else { return nil }
        return textIndex.lineNumber(
            containingUTF16Location: ranges[clampedIndex(selectedIndex)].location
        )
    }
}

private enum TerminalTranscriptSearchWork {
    static let queryDebounceNanoseconds: UInt64 = 35_000_000
    static let textDebounceNanoseconds: UInt64 = 0
}

private struct TerminalTranscriptTextView: NSViewRepresentable {
    @Environment(\.runeThemePalette) private var palette
    let text: String
    let fontSize: CGFloat
    let searchIndex: TerminalTranscriptSearchIndex?
    let searchResultRevision: Int
    let selectedSearchMatchIndex: Int
    let searchNavigationRevision: Int
    let reduceMotion: Bool
    let onPasteText: (String) -> Void
    let onInputSequence: (String) -> Void

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
        textView.onInputSequence = onInputSequence
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
        textView.textContainerInset = NSSize(
            width: RuneUILayoutMetrics.inspectorDocumentHorizontalInset,
            height: RuneUILayoutMetrics.inspectorDocumentVerticalInset
        )
        textView.backgroundColor = .clear
        textView.drawsBackground = false
        textView.font = terminalFont
        textView.textColor = palette.map { NSColor($0.foreground) } ?? .labelColor

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
            terminalTextView.onInputSequence = onInputSequence
        }
        textView.textColor = palette.map { NSColor($0.foreground) } ?? .labelColor
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
            clearSearchHighlights(in: textView, coordinator: context.coordinator)
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
            textView.invalidateIntrinsicContentSize()
            textView.layoutSubtreeIfNeeded()
            let hasActiveSearchMatch = applySearchHighlights(
                in: textView,
                coordinator: context.coordinator
            )
            if shouldStickToBottom, !hasActiveSearchMatch {
                let range = NSRange(location: max(0, text.utf16.count - 1), length: 0)
                textView.scrollRangeToVisible(range)
            }
        } else {
            textView.layoutManager?.ensureLayout(for: textView.textContainer!)
            _ = applySearchHighlights(in: textView, coordinator: context.coordinator)
        }
    }

    private func isNearBottom(_ scrollView: NSScrollView) -> Bool {
        guard let documentView = scrollView.documentView else { return true }
        let clipBounds = scrollView.contentView.bounds
        let maxOffset = max(0, documentView.frame.maxY - clipBounds.height)
        return maxOffset - clipBounds.origin.y < 28
    }

    @discardableResult
    private func applySearchHighlights(
        in textView: NSTextView,
        coordinator: Coordinator
    ) -> Bool {
        guard let storage = textView.textStorage,
              let layoutManager = textView.layoutManager else {
            return false
        }
        guard let searchIndex else {
            clearSearchHighlights(in: textView, coordinator: coordinator)
            return false
        }

        if coordinator.appliedSearchResultRevision != searchResultRevision {
            clearSearchHighlights(in: textView, coordinator: coordinator)
            coordinator.appliedSearchResultRevision = searchResultRevision

            let passiveRanges = searchIndex.ranges.prefix(Coordinator.passiveHighlightLimit)
            coordinator.passiveSearchHighlightRanges.reserveCapacity(passiveRanges.count)
            for range in passiveRanges where NSMaxRange(range) <= storage.length {
                applyPassiveSearchHighlight(range, layoutManager: layoutManager)
                coordinator.passiveSearchHighlightRanges.append(range)
            }
        }

        guard !searchIndex.ranges.isEmpty else {
            coordinator.activeSearchHighlightRange = nil
            coordinator.lastCenteredNavigationRevision = searchNavigationRevision
            return false
        }

        let activeIndex = searchIndex.clampedIndex(selectedSearchMatchIndex)
        let activeRange = searchIndex.ranges[activeIndex]
        guard NSMaxRange(activeRange) <= storage.length else { return false }

        if coordinator.activeSearchHighlightRange != activeRange {
            if let previousRange = coordinator.activeSearchHighlightRange {
                removeSearchHighlightAttributes(previousRange, layoutManager: layoutManager)
                if coordinator.passiveSearchHighlightRanges.contains(previousRange) {
                    applyPassiveSearchHighlight(previousRange, layoutManager: layoutManager)
                }
            }
            applyActiveSearchHighlight(activeRange, layoutManager: layoutManager)
            coordinator.activeSearchHighlightRange = activeRange
        }

        let shouldCenter =
            coordinator.lastCenteredNavigationRevision != searchNavigationRevision
        if shouldCenter {
            coordinator.lastCenteredNavigationRevision = searchNavigationRevision
            centerSearchRange(activeRange, in: textView, animated: !reduceMotion)
        }
        return true
    }

    private func clearSearchHighlights(in textView: NSTextView, coordinator: Coordinator) {
        guard let layoutManager = textView.layoutManager else {
            coordinator.resetSearchHighlights()
            return
        }
        let textLength = textView.textStorage?.length ?? 0
        for range in coordinator.passiveSearchHighlightRanges where NSMaxRange(range) <= textLength {
            removeSearchHighlightAttributes(range, layoutManager: layoutManager)
        }
        if let activeRange = coordinator.activeSearchHighlightRange,
           NSMaxRange(activeRange) <= textLength,
           !coordinator.passiveSearchHighlightRanges.contains(activeRange) {
            removeSearchHighlightAttributes(activeRange, layoutManager: layoutManager)
        }
        coordinator.resetSearchHighlights()
    }

    private func applyPassiveSearchHighlight(_ range: NSRange, layoutManager: NSLayoutManager) {
        let highlightColor = NSColor.systemYellow.withAlphaComponent(0.56)
        layoutManager.addTemporaryAttributes([
            .backgroundColor: highlightColor,
            .underlineColor: NSColor.systemYellow,
            .underlineStyle: NSUnderlineStyle.single.rawValue
        ], forCharacterRange: range)
    }

    private func applyActiveSearchHighlight(_ range: NSRange, layoutManager: NSLayoutManager) {
        layoutManager.addTemporaryAttributes([
            .backgroundColor: NSColor.selectedContentBackgroundColor.withAlphaComponent(0.90),
            .foregroundColor: NSColor.selectedTextColor,
            .underlineColor: NSColor.selectedTextColor,
            .underlineStyle: NSUnderlineStyle.thick.rawValue
        ], forCharacterRange: range)
    }

    private func removeSearchHighlightAttributes(_ range: NSRange, layoutManager: NSLayoutManager) {
        layoutManager.removeTemporaryAttribute(.backgroundColor, forCharacterRange: range)
        layoutManager.removeTemporaryAttribute(.foregroundColor, forCharacterRange: range)
        layoutManager.removeTemporaryAttribute(.underlineColor, forCharacterRange: range)
        layoutManager.removeTemporaryAttribute(.underlineStyle, forCharacterRange: range)
    }

    private func centerSearchRange(_ activeRange: NSRange, in textView: NSTextView, animated: Bool) {
        guard let layoutManager = textView.layoutManager,
              let textContainer = textView.textContainer,
              let scrollView = textView.enclosingScrollView else {
            textView.scrollRangeToVisible(activeRange)
            return
        }

        layoutManager.ensureLayout(forCharacterRange: activeRange)
        let glyphRange = layoutManager.glyphRange(
            forCharacterRange: activeRange,
            actualCharacterRange: nil
        )
        guard glyphRange.location < layoutManager.numberOfGlyphs else {
            textView.scrollRangeToVisible(activeRange)
            return
        }

        var targetRect = layoutManager.boundingRect(forGlyphRange: glyphRange, in: textContainer)
        let textOrigin = textView.textContainerOrigin
        targetRect.origin.x += textOrigin.x
        targetRect.origin.y += textOrigin.y

        let clipView = scrollView.contentView
        let visibleBounds = clipView.bounds
        let documentBounds = scrollView.documentView?.bounds ?? textView.bounds
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

        if animated, textView.window != nil {
            NSAnimationContext.runAnimationGroup { context in
                context.duration = 0.10
                clipView.animator().setBoundsOrigin(targetOrigin)
            }
        } else {
            clipView.scroll(to: targetOrigin)
        }
        scrollView.reflectScrolledClipView(clipView)
    }

    final class Coordinator {
        static let passiveHighlightLimit = 4_096

        var appliedSearchResultRevision: Int?
        var passiveSearchHighlightRanges: [NSRange] = []
        var activeSearchHighlightRange: NSRange?
        var lastCenteredNavigationRevision: Int?
        var lastFontSize: CGFloat?

        func resetSearchHighlights() {
            appliedSearchResultRevision = nil
            passiveSearchHighlightRanges.removeAll(keepingCapacity: true)
            activeSearchHighlightRange = nil
        }
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
    }
}

private final class TerminalTranscriptInteractionTextView: NSTextView {
    var onPasteText: ((String) -> Void)?
    var onInputSequence: ((String) -> Void)?

    override var acceptsFirstResponder: Bool {
        true
    }

    override func keyDown(with event: NSEvent) {
        guard let sequence = Self.terminalInputSequence(for: event) else {
            super.keyDown(with: event)
            return
        }
        onInputSequence?(sequence)
    }

    static func terminalInputSequence(for event: NSEvent) -> String? {
        let modifiers = event.modifierFlags.intersection(.deviceIndependentFlagsMask)
        if modifiers.contains(.command) || modifiers.contains(.option) {
            return nil
        }
        switch event.keyCode {
        case 36, 76: return "\r"
        case 48: return "\t"
        case 51: return "\u{007F}"
        case 53: return "\u{001B}"
        case 115: return "\u{001B}[H"
        case 116: return "\u{001B}[5~"
        case 117: return "\u{001B}[3~"
        case 119: return "\u{001B}[F"
        case 121: return "\u{001B}[6~"
        case 123: return "\u{001B}[D"
        case 124: return "\u{001B}[C"
        case 125: return "\u{001B}[B"
        case 126: return "\u{001B}[A"
        default:
            return event.characters?.isEmpty == false ? event.characters : nil
        }
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
