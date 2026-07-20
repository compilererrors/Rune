import SwiftUI
import RuneCore
import struct RuneSharedCore.RuneLargeTextIndex

struct InspectorFindIndex: Equatable {
    let ranges: [NSRange]
    let matchLineNumbers: [Int]
    private let hasQuery: Bool

    init(text: String, query: String, matchCase: Bool) {
        let trimmedQuery = query.trimmingCharacters(in: .whitespacesAndNewlines)
        hasQuery = !trimmedQuery.isEmpty
        guard hasQuery else {
            ranges = []
            matchLineNumbers = []
            return
        }

        let index = RuneLargeTextIndex(text: text)
        let options: NSString.CompareOptions = matchCase ? [] : [.caseInsensitive, .diacriticInsensitive]
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

struct InspectorFindBar: View {
    let placeholder: String
    let searchIndex: InspectorFindIndex
    @Binding var query: String
    @Binding var matchCase: Bool
    @Binding var selectedMatchIndex: Int
    @Binding var isPresented: Bool
    @State private var isJumpPopoverPresented = false
    @State private var jumpText = ""
    @FocusState private var isFocused: Bool

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
        HStack(spacing: 8) {
            Image(systemName: "magnifyingglass")
                .foregroundStyle(.secondary)

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
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var navigationControls: some View {
        HStack(spacing: 8) {
            Button {
                prepareJumpPopover()
            } label: {
                Text(searchIndex.statusText(selectedIndex: selectedMatchIndex))
                    .font(.caption.weight(.medium))
                    .foregroundStyle(.secondary)
                    .monospacedDigit()
                    .runeMinimumInteractiveTarget(minWidth: 74, alignment: .trailing)
            }
            .buttonStyle(.plain)
            .disabled(searchIndex.ranges.isEmpty)
            .popover(isPresented: $isJumpPopoverPresented, arrowEdge: .bottom) {
                jumpToMatchPopover
            }
            .help("Go to match number")

            RuneIconButton(
                "Previous match",
                systemImage: "chevron.up",
                isDisabled: searchIndex.ranges.isEmpty
            ) {
                advanceSearch(by: -1)
            }

            RuneIconButton(
                "Next match",
                systemImage: "chevron.down",
                isDisabled: searchIndex.ranges.isEmpty
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
        let count = searchIndex.ranges.count
        guard count > 0 else { return }
        selectedMatchIndex = (searchIndex.clampedIndex(selectedMatchIndex) + delta + count) % count
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

                Text("of \(searchIndex.ranges.count)")
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
    }

    private func prepareJumpPopover() {
        guard !searchIndex.ranges.isEmpty else { return }
        jumpText = "\(searchIndex.clampedIndex(selectedMatchIndex) + 1)"
        isJumpPopoverPresented = true
    }

    private func commitJump() {
        let count = searchIndex.ranges.count
        guard count > 0 else { return }
        let requestedMatch = Int(jumpText.trimmingCharacters(in: .whitespacesAndNewlines)) ?? 1
        selectedMatchIndex = min(max(requestedMatch, 1), count) - 1
        isJumpPopoverPresented = false
    }
}

struct FindableInspectorSurface<Content: View>: View {
    let text: String
    let placeholder: String
    @Binding var query: String
    @Binding var matchCase: Bool
    @Binding var selectedMatchIndex: Int
    @Binding var isFindPresented: Bool
    @ViewBuilder var content: () -> Content

    var body: some View {
        let searchIndex = InspectorFindIndex(text: text, query: query, matchCase: matchCase)

        content()
            .overlay(alignment: .topTrailing) {
                if isFindPresented {
                    InspectorFindBar(
                        placeholder: placeholder,
                        searchIndex: searchIndex,
                        query: $query,
                        matchCase: $matchCase,
                        selectedMatchIndex: $selectedMatchIndex,
                        isPresented: $isFindPresented
                    )
                    .padding(10)
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
                    .padding(10)
                    .keyboardShortcut("f", modifiers: [.command])
                }
            }
            .onChange(of: query) { _, _ in
                selectedMatchIndex = 0
            }
            .onChange(of: matchCase) { _, _ in
                selectedMatchIndex = 0
            }
            .onChange(of: text) { _, _ in
                selectedMatchIndex = searchIndex.clampedIndex(selectedMatchIndex)
            }
    }
}
