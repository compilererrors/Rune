import SwiftUI
import RuneCore
import struct RuneSharedCore.RuneLargeTextIndex

struct InspectorFindIndex: Equatable, Sendable {
    let originalText: String
    let query: String
    let matchCase: Bool
    let textIndex: RuneLargeTextIndex
    let ranges: [NSRange]

    init(text: String, query: String, matchCase: Bool) {
        self.init(
            textIndex: RuneLargeTextIndex(text: text),
            query: query,
            matchCase: matchCase
        )
    }

    init(textIndex: RuneLargeTextIndex, query: String, matchCase: Bool) {
        self.init(
            textIndex: textIndex,
            query: query,
            matchCase: matchCase,
            cancellationCheck: {}
        )
    }

    init(
        textIndex: RuneLargeTextIndex,
        query: String,
        matchCase: Bool,
        cancellationCheck: () throws -> Void
    ) rethrows {
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        originalText = textIndex.text
        self.query = trimmedQuery
        self.matchCase = matchCase
        self.textIndex = textIndex
        guard !trimmedQuery.isEmpty else {
            ranges = []
            return
        }

        let options: NSString.CompareOptions = matchCase ? [] : [.caseInsensitive, .diacriticInsensitive]
        ranges = try textIndex.searchRanges(
            query: trimmedQuery,
            options: options,
            cancellationCheck: cancellationCheck
        )
    }

    func isCurrent(text: String, query: String, matchCase: Bool) -> Bool {
        originalText == text
            && self.query == query.trimmingCharacters(in: .whitespacesAndNewlines)
            && self.matchCase == matchCase
    }

    func clampedIndex(_ index: Int) -> Int {
        guard !ranges.isEmpty else { return 0 }
        return min(max(index, 0), ranges.count - 1)
    }

    func statusText(selectedIndex: Int) -> String {
        guard !query.isEmpty else { return "" }
        guard !ranges.isEmpty else { return "No matches" }
        return "\(clampedIndex(selectedIndex) + 1) of \(ranges.count)"
    }

    func matchLineNumber(selectedIndex: Int) -> Int? {
        guard !ranges.isEmpty else { return nil }
        let range = ranges[clampedIndex(selectedIndex)]
        return textIndex.lineNumber(containingUTF16Location: range.location)
    }
}

struct InspectorFindBar: View {
    let placeholder: String
    let searchIndex: InspectorFindIndex?
    @Binding var query: String
    @Binding var matchCase: Bool
    @Binding var selectedMatchIndex: Int
    @Binding var isPresented: Bool
    @Binding var searchNavigationRevision: Int
    @State private var isJumpPopoverPresented = false
    @State private var jumpText = ""
    @FocusState private var isFocused: Bool

    init(
        placeholder: String,
        searchIndex: InspectorFindIndex?,
        query: Binding<String>,
        matchCase: Binding<Bool>,
        selectedMatchIndex: Binding<Int>,
        isPresented: Binding<Bool>,
        searchNavigationRevision: Binding<Int> = .constant(0)
    ) {
        self.placeholder = placeholder
        self.searchIndex = searchIndex
        _query = query
        _matchCase = matchCase
        _selectedMatchIndex = selectedMatchIndex
        _isPresented = isPresented
        _searchNavigationRevision = searchNavigationRevision
    }

    var body: some View {
        RuneFindBarChrome("Inspector find controls") {
            searchField
        } secondary: {
            navigationControls
        }
        .onAppear {
            isFocused = true
            DispatchQueue.main.async {
                isFocused = true
            }
        }
        .onExitCommand {
            isPresented = false
            query = ""
        }
        .accessibilityIdentifier("inspector-find-bar")
    }

    private var searchField: some View {
        RuneInspectorControlGridRow {
            Image(systemName: "magnifyingglass")
                .foregroundStyle(.secondary)
        } content: {
            TextField(placeholder, text: $query)
                .textFieldStyle(.roundedBorder)
                .font(.system(size: 12))
                .frame(
                    minWidth: RuneFindBarMetrics.minimumSearchFieldWidth,
                    idealWidth: RuneFindBarMetrics.idealSearchFieldWidth,
                    maxWidth: .infinity
                )
                .focused($isFocused)
                .onSubmit {
                    advanceSearch(by: 1)
                }
                .runeTextInputCursor()
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var navigationControls: some View {
        HStack(spacing: 8) {
            Button {
                prepareJumpPopover()
            } label: {
                Text(statusText)
                    .font(.caption.weight(.medium))
                    .foregroundStyle(.secondary)
                    .monospacedDigit()
                    .runeMinimumInteractiveTarget(minWidth: 74, alignment: .trailing)
            }
            .buttonStyle(.plain)
            .disabled(searchIndex?.ranges.isEmpty != false)
            .popover(isPresented: $isJumpPopoverPresented, arrowEdge: .bottom) {
                jumpToMatchPopover
            }
            .help("Go to match number")

            RuneIconButton(
                "Previous match",
                systemImage: "chevron.up",
                isDisabled: searchIndex?.ranges.isEmpty != false
            ) {
                advanceSearch(by: -1)
            }

            RuneIconButton(
                "Next match",
                systemImage: "chevron.down",
                isDisabled: searchIndex?.ranges.isEmpty != false
            ) {
                advanceSearch(by: 1)
            }

            RuneMatchCaseButton(isSelected: $matchCase)

            RuneIconButton("Close find", systemImage: "xmark") {
                isPresented = false
                query = ""
            }
        }
        .fixedSize(horizontal: true, vertical: false)
    }

    private func advanceSearch(by delta: Int) {
        guard let searchIndex else { return }
        let count = searchIndex.ranges.count
        guard count > 0 else { return }
        selectedMatchIndex = (searchIndex.clampedIndex(selectedMatchIndex) + delta + count) % count
        searchNavigationRevision &+= 1
    }

    private var jumpToMatchPopover: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Go to match")
                .font(.headline)

            HStack(spacing: 8) {
                TextField("Match", text: $jumpText)
                    .textFieldStyle(.roundedBorder)
                    .font(.system(size: 12, weight: .regular, design: .monospaced))
                    .frame(width: 82)
                    .onSubmit(commitJump)
                    .runeTextInputCursor()

                Text("of \(searchIndex?.ranges.count ?? 0)")
                    .font(.caption.weight(.medium))
                    .foregroundStyle(.secondary)
                    .monospacedDigit()
            }

            HStack {
                Spacer()
                Button("Go", action: commitJump)
                    .keyboardShortcut(.defaultAction)
            }
        }
        .padding(12)
        .frame(width: 180)
        .runePointerCursor()
        .runeCursorScope(default: .pointer)
    }

    private func prepareJumpPopover() {
        guard let searchIndex else { return }
        guard !searchIndex.ranges.isEmpty else { return }
        jumpText = "\(searchIndex.clampedIndex(selectedMatchIndex) + 1)"
        isJumpPopoverPresented = true
    }

    private func commitJump() {
        guard let searchIndex else { return }
        let count = searchIndex.ranges.count
        guard count > 0 else { return }
        let requestedMatch = Int(jumpText.trimmingCharacters(in: .whitespacesAndNewlines)) ?? 1
        selectedMatchIndex = min(max(requestedMatch, 1), count) - 1
        searchNavigationRevision &+= 1
        isJumpPopoverPresented = false
    }

    private var statusText: String {
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmedQuery.isEmpty else { return "" }
        guard let searchIndex else { return "…" }
        return searchIndex.statusText(selectedIndex: selectedMatchIndex)
    }
}

struct FindableInspectorSurface<Content: View>: View {
    let text: String
    let placeholder: String
    @Binding var query: String
    @Binding var matchCase: Bool
    @Binding var selectedMatchIndex: Int
    @Binding var isFindPresented: Bool
    @ViewBuilder var content: (InspectorFindIndex?, Int) -> Content
    @State private var searchIndex: InspectorFindIndex?
    @State private var searchNavigationRevision = 0
    @State private var searchTask: Task<Void, Never>?
    @State private var searchWorkLane = InspectorFindLatestWorkLane()
    @State private var pendingInitialNavigation: InspectorFindInitialNavigationRequest?

    var body: some View {
        let activeSearchIndex = searchIndex.flatMap {
            $0.isCurrent(text: text, query: query, matchCase: matchCase) ? $0 : nil
        }

        content(activeSearchIndex, searchNavigationRevision)
            .overlay(alignment: .topTrailing) {
                if isFindPresented {
                    InspectorFindBar(
                        placeholder: placeholder,
                        searchIndex: activeSearchIndex,
                        query: $query,
                        matchCase: $matchCase,
                        selectedMatchIndex: $selectedMatchIndex,
                        isPresented: $isFindPresented,
                        searchNavigationRevision: $searchNavigationRevision
                    )
                    .padding(RuneUILayoutMetrics.inspectorOverlayInset)
                    .runePointerCursor()
                    .runeCursorScope(default: .pointer)
                } else {
                    RuneIconButton(placeholder, systemImage: "magnifyingglass") {
                        isFindPresented = true
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
            .onChange(of: query) { _, _ in
                selectedMatchIndex = 0
                prepareInitialNavigation()
                scheduleSearch(debounceNanoseconds: InspectorFindWork.queryDebounceNanoseconds)
            }
            .onChange(of: matchCase) { _, _ in
                selectedMatchIndex = 0
                prepareInitialNavigation()
                scheduleSearch(debounceNanoseconds: InspectorFindWork.queryDebounceNanoseconds)
            }
            .onChange(of: text) { _, _ in
                pendingInitialNavigation = nil
                scheduleSearch(debounceNanoseconds: InspectorFindWork.textDebounceNanoseconds)
            }
            .onAppear {
                prepareInitialNavigation()
                scheduleSearch(debounceNanoseconds: 0)
            }
            .onDisappear {
                searchTask?.cancel()
                pendingInitialNavigation = nil
                Task {
                    await searchWorkLane.cancel()
                }
            }
    }

    private func scheduleSearch(debounceNanoseconds: UInt64) {
        searchTask?.cancel()
        let requestedText = text
        let requestedQuery = query
        let requestedMatchCase = matchCase
        let reusableTextIndex = searchIndex?.originalText == requestedText
            ? searchIndex?.textIndex
            : nil

        searchTask = Task { @MainActor in
            if debounceNanoseconds > 0 {
                try? await Task.sleep(nanoseconds: debounceNanoseconds)
            }
            guard !Task.isCancelled else { return }

            guard let result = try? await searchWorkLane.run(
                priority: .userInitiated,
                operation: {
                    let textIndex: RuneLargeTextIndex
                    if let reusableTextIndex {
                        textIndex = reusableTextIndex
                    } else {
                        textIndex = try RuneLargeTextIndex(
                            text: requestedText,
                            cancellationCheck: { try Task.checkCancellation() }
                        )
                    }
                    return try InspectorFindIndex(
                        textIndex: textIndex,
                        query: requestedQuery,
                        matchCase: requestedMatchCase,
                        cancellationCheck: { try Task.checkCancellation() }
                    )
                }
            ) else {
                return
            }

            guard !Task.isCancelled,
                  text == requestedText,
                  query == requestedQuery,
                  matchCase == requestedMatchCase else {
                return
            }
            searchIndex = result
            selectedMatchIndex = result.clampedIndex(selectedMatchIndex)
            let completedRequest = InspectorFindInitialNavigationRequest(
                text: requestedText,
                query: requestedQuery,
                matchCase: requestedMatchCase
            )
            if pendingInitialNavigation == completedRequest {
                pendingInitialNavigation = nil
                if !result.ranges.isEmpty {
                    searchNavigationRevision &+= 1
                }
            }
        }
    }

    private func prepareInitialNavigation() {
        guard !query.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            pendingInitialNavigation = nil
            return
        }
        pendingInitialNavigation = InspectorFindInitialNavigationRequest(
            text: text,
            query: query,
            matchCase: matchCase
        )
    }
}

private struct InspectorFindInitialNavigationRequest: Equatable {
    let text: String
    let query: String
    let matchCase: Bool
}

private enum InspectorFindWork {
    static let queryDebounceNanoseconds: UInt64 = 40_000_000
    static let textDebounceNanoseconds: UInt64 = 80_000_000
}

private actor InspectorFindLatestWorkLane {
    private var generation: UInt64 = 0
    private var currentTask: Task<InspectorFindIndex, Error>?

    func run(
        priority: TaskPriority,
        operation: @escaping @Sendable () throws -> InspectorFindIndex
    ) async throws -> InspectorFindIndex {
        generation &+= 1
        let requestedGeneration = generation

        if let currentTask {
            currentTask.cancel()
            _ = await currentTask.result
        }

        try Task.checkCancellation()
        guard requestedGeneration == generation else { throw CancellationError() }

        let workTask = Task.detached(priority: priority) {
            try Task.checkCancellation()
            let result = try operation()
            try Task.checkCancellation()
            return result
        }
        currentTask = workTask

        let result = try await withTaskCancellationHandler {
            try await workTask.value
        } onCancel: {
            workTask.cancel()
        }

        try Task.checkCancellation()
        guard requestedGeneration == generation else { throw CancellationError() }
        currentTask = nil
        return result
    }

    func cancel() async {
        generation &+= 1
        currentTask?.cancel()
        if let currentTask {
            _ = await currentTask.result
        }
        currentTask = nil
    }
}
