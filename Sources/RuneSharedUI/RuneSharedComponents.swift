import AppKit
import RuneSharedCore
import SwiftUI

public struct RuneMetricPill: View {
    private let title: String
    private let value: Int?

    public init(title: String, value: Int?) {
        self.title = title
        self.value = value
    }

    public var body: some View {
        Text("\(title): \(value.map { "\($0)%" } ?? "-")")
            .font(.caption.weight(.semibold))
            .foregroundStyle(.secondary)
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(.secondary.opacity(0.12), in: Capsule())
    }
}

public struct RuneStatusBadge: View {
    private let status: String

    public init(_ status: String) {
        self.status = status
    }

    public var body: some View {
        Text(status)
            .font(.caption2.weight(.bold))
            .padding(.horizontal, 7)
            .padding(.vertical, 3)
            .background(statusColor.opacity(0.16), in: Capsule())
            .foregroundStyle(statusColor)
    }

    private var statusColor: Color {
        switch status.lowercased() {
        case "running", "succeeded", "deployed", "normal":
            return .green
        case "pending", "warning":
            return .orange
        case "failed", "error":
            return .red
        default:
            return .secondary
        }
    }
}

public struct RuneMonospaceTextPanel: View {
    private let text: String
    private let placeholder: String

    public init(text: String, placeholder: String) {
        self.text = text
        self.placeholder = placeholder
    }

    public var body: some View {
        ScrollView {
            Text(text.isEmpty ? placeholder : text)
                .font(.system(.footnote, design: .monospaced))
                .foregroundStyle(text.isEmpty ? .secondary : .primary)
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .topLeading)
                .padding()
        }
        .background(.secondary.opacity(0.08), in: RoundedRectangle(cornerRadius: 8))
    }
}

public struct RuneLargeTextSurface: View {
    private let index: RuneLargeTextIndex
    private let placeholder: String
    private let scrollTargetLine: Int?
    private let scrollTargetRevision: Int?
    private let scrollsOnTargetLineChange: Bool
    private let searchMatchRanges: [NSRange]
    private let selectedSearchMatchIndex: Int
    private let horizontalContentInset: CGFloat
    private let verticalContentInset: CGFloat
    private let showsLineNumbers: Bool
    private let fontSize: CGFloat
    private let onNearBottomChange: (Bool) -> Void
    @State private var verticalOffset: CGFloat = 0
    @State private var pendingSearchScrollTask: Task<Void, Never>?
    @Environment(\.accessibilityReduceMotion) private var accessibilityReduceMotion

    public init(
        text: String,
        placeholder: String = "No output",
        scrollTargetLine: Int? = nil,
        scrollTargetRevision: Int? = nil,
        scrollsOnTargetLineChange: Bool = true,
        searchMatchRanges: [NSRange] = [],
        selectedSearchMatchIndex: Int = 0,
        horizontalContentInset: CGFloat = 10,
        verticalContentInset: CGFloat = 8,
        showsLineNumbers: Bool = true,
        fontSize: CGFloat = 12,
        onNearBottomChange: @escaping (Bool) -> Void = { _ in }
    ) {
        self.index = RuneLargeTextIndex(text: text)
        self.placeholder = placeholder
        self.scrollTargetLine = scrollTargetLine
        self.scrollTargetRevision = scrollTargetRevision
        self.scrollsOnTargetLineChange = scrollsOnTargetLineChange
        self.searchMatchRanges = searchMatchRanges
        self.selectedSearchMatchIndex = selectedSearchMatchIndex
        self.horizontalContentInset = horizontalContentInset
        self.verticalContentInset = verticalContentInset
        self.showsLineNumbers = showsLineNumbers
        self.fontSize = fontSize
        self.onNearBottomChange = onNearBottomChange
    }

    public init(
        index: RuneLargeTextIndex,
        placeholder: String = "No output",
        scrollTargetLine: Int? = nil,
        scrollTargetRevision: Int? = nil,
        scrollsOnTargetLineChange: Bool = true,
        searchMatchRanges: [NSRange] = [],
        selectedSearchMatchIndex: Int = 0,
        horizontalContentInset: CGFloat = 10,
        verticalContentInset: CGFloat = 8,
        showsLineNumbers: Bool = true,
        fontSize: CGFloat = 12,
        onNearBottomChange: @escaping (Bool) -> Void = { _ in }
    ) {
        self.index = index
        self.placeholder = placeholder
        self.scrollTargetLine = scrollTargetLine
        self.scrollTargetRevision = scrollTargetRevision
        self.scrollsOnTargetLineChange = scrollsOnTargetLineChange
        self.searchMatchRanges = searchMatchRanges
        self.selectedSearchMatchIndex = selectedSearchMatchIndex
        self.horizontalContentInset = horizontalContentInset
        self.verticalContentInset = verticalContentInset
        self.showsLineNumbers = showsLineNumbers
        self.fontSize = fontSize
        self.onNearBottomChange = onNearBottomChange
    }

    public var body: some View {
        ScrollViewReader { proxy in
            GeometryReader { outerProxy in
                let viewportHeight = Self.clampedFiniteViewportHeight(outerProxy.size.height)
                ScrollView([.vertical, .horizontal]) {
                    if index.lineCount == 0 {
                        Text(placeholder)
                            .font(.system(size: fontSize, weight: .regular, design: .monospaced))
                            .foregroundStyle(.secondary)
                            .frame(maxWidth: .infinity, alignment: .topLeading)
                            .padding(.horizontal, horizontalContentInset)
                            .padding(.vertical, verticalContentInset)
                    } else {
                        sparseContent(viewportHeight: viewportHeight)
                    }
                }
                .coordinateSpace(name: coordinateSpaceName)
                .onPreferenceChange(RuneLargeTextVerticalOffsetPreferenceKey.self) { offset in
                    let normalizedOffset = max(0, offset)
                    verticalOffset = normalizedOffset
                    onNearBottomChange(isNearBottom(verticalOffset: normalizedOffset, viewportHeight: viewportHeight))
                }
                .onChange(of: index.lineCount) { _, _ in
                    clampScrollIfNeeded(proxy: proxy, viewportHeight: viewportHeight)
                }
                .onChange(of: index.utf16Length) { _, _ in
                    clampScrollIfNeeded(proxy: proxy, viewportHeight: viewportHeight)
                }
                .onChange(of: outerProxy.size.height) { _, height in
                    clampScrollIfNeeded(proxy: proxy, viewportHeight: Self.clampedFiniteViewportHeight(height))
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            .background(.secondary.opacity(0.08), in: RoundedRectangle(cornerRadius: 8))
            .onAppear {
                scheduleScrollToActiveSearchTarget(proxy: proxy)
            }
            .onChange(of: scrollTargetLine) { _, _ in
                if scrollsOnTargetLineChange {
                    scheduleScrollToActiveSearchTarget(proxy: proxy)
                }
            }
            .onChange(of: scrollTargetRevision) { _, _ in
                scheduleScrollToActiveSearchTarget(proxy: proxy)
            }
            .onDisappear {
                pendingSearchScrollTask?.cancel()
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    /// `GeometryReader` inside a `ScrollViewReader` can receive a collapsed or non-finite proposal from SwiftUI;
    /// virtualization math then uses a bogus viewport height and the surface can look “cut off” after roughly
    /// `visibleLimit + overscan*2` rows (~80 on typical laptop metrics).
    private static func clampedFiniteViewportHeight(_ raw: CGFloat) -> CGFloat {
        guard raw.isFinite, raw > 2 else { return 480 }
        return min(max(raw, 120), 16_384)
    }

    private var coordinateSpaceName: String {
        "RuneLargeTextSurfaceScroll"
    }

    private var rowHeight: CGFloat {
        ceil(fontSize * 1.45)
    }

    private var verticalPadding: CGFloat {
        max(0, verticalContentInset)
    }

    private var lineNumberColumnWidth: CGFloat {
        guard showsLineNumbers else { return 0 }
        return CGFloat(max(3, String(max(1, index.lineCount)).count)) * max(7, fontSize * 0.66)
    }

    private func sparseContent(viewportHeight: CGFloat) -> some View {
        let visible = visibleViewport(viewportHeight: viewportHeight)
        let contentHeight = layout(viewportHeight: viewportHeight).contentHeight
        let targetLine = scrollTargetLine.flatMap { line in
            line > 0 && line <= index.lineCount ? line : nil
        }

        return ZStack(alignment: .topLeading) {
            GeometryReader { proxy in
                Color.clear.preference(
                    key: RuneLargeTextVerticalOffsetPreferenceKey.self,
                    value: -proxy.frame(in: .named(coordinateSpaceName)).minY
                )
            }
            .frame(width: 1, height: 1)

            if let targetLine {
                scrollAnchor(
                    y: yOffset(for: targetLine),
                    contentHeight: CGFloat(contentHeight),
                    id: matchScrollTargetID(for: targetLine)
                )
            }

            if index.lineCount > 0, targetLine != index.lineCount {
                scrollAnchor(
                    y: yOffset(for: index.lineCount),
                    contentHeight: CGFloat(contentHeight),
                    id: bottomScrollTargetID(for: index.lineCount)
                )
            }

            VStack(alignment: .leading, spacing: 0) {
                ForEach(visible.lines, id: \.number) { line in
                    RuneLargeTextLineRow(
                        line: line,
                        searchMatches: searchMatches(in: line),
                        showsLineNumbers: showsLineNumbers,
                        fontSize: fontSize,
                        lineNumberWidth: lineNumberColumnWidth,
                        horizontalContentInset: horizontalContentInset,
                        isActiveSearchLine: line.number == targetLine && activeSearchRange != nil
                    )
                    .frame(height: rowHeight, alignment: .topLeading)
                }
            }
            .padding(.trailing, 12)
            .offset(y: yOffset(for: visible.startLine))
        }
        .frame(minWidth: 1, minHeight: CGFloat(contentHeight), alignment: .topLeading)
    }

    private func scrollAnchor(y: CGFloat, contentHeight: CGFloat, id: String) -> some View {
        VStack(alignment: .leading, spacing: 0) {
            Color.clear
                .frame(width: 1, height: max(0, y))
            Color.clear
                .frame(width: 1, height: 1)
                .id(id)
            Spacer(minLength: 0)
        }
        .frame(width: 1, height: contentHeight, alignment: .topLeading)
    }

    private func visibleViewport(viewportHeight: CGFloat) -> RuneLargeTextViewport {
        let overscan = 24
        let visibleLimit = max(1, Int(ceil(max(1, viewportHeight) / rowHeight)))
        let startLine = layout(viewportHeight: viewportHeight).viewportStartLine(
            verticalOffset: Double(verticalOffset),
            overscan: overscan
        )
        return index.viewport(startLine: startLine, lineLimit: visibleLimit + overscan * 2)
    }

    private func layout(viewportHeight: CGFloat) -> RuneLargeTextViewportLayout {
        RuneLargeTextViewportLayout(
            lineCount: index.lineCount,
            rowHeight: Double(rowHeight),
            verticalPadding: Double(verticalPadding),
            viewportHeight: Double(viewportHeight)
        )
    }

    private func yOffset(for line: Int) -> CGFloat {
        verticalPadding + CGFloat(max(0, line - 1)) * rowHeight
    }

    private func isNearBottom(verticalOffset: CGFloat, viewportHeight: CGFloat) -> Bool {
        let contentHeight = max(rowHeight, CGFloat(index.lineCount) * rowHeight + verticalPadding * 2)
        let maxOffset = max(0, contentHeight - max(1, viewportHeight))
        return maxOffset - verticalOffset < rowHeight * 2
    }

    private var activeSearchRange: NSRange? {
        guard !searchMatchRanges.isEmpty else { return nil }
        let activeIndex = min(max(selectedSearchMatchIndex, 0), searchMatchRanges.count - 1)
        return searchMatchRanges[activeIndex]
    }

    private func searchMatches(in line: RuneLargeTextLine) -> [RuneLargeTextLineSearchMatch] {
        guard !searchMatchRanges.isEmpty, line.contentRange.length > 0 else { return [] }

        let lineStart = line.contentRange.location
        let lineEnd = NSMaxRange(line.contentRange)
        var lower = 0
        var upper = searchMatchRanges.count
        while lower < upper {
            let middle = lower + (upper - lower) / 2
            if NSMaxRange(searchMatchRanges[middle]) <= lineStart {
                lower = middle + 1
            } else {
                upper = middle
            }
        }

        let activeIndex = min(max(selectedSearchMatchIndex, 0), searchMatchRanges.count - 1)
        var index = lower
        var matches: [RuneLargeTextLineSearchMatch] = []
        while index < searchMatchRanges.count {
            let match = searchMatchRanges[index]
            guard match.location < lineEnd else { break }
            let intersection = NSIntersectionRange(match, line.contentRange)
            if intersection.length > 0 {
                matches.append(RuneLargeTextLineSearchMatch(
                    range: NSRange(
                        location: intersection.location - lineStart,
                        length: intersection.length
                    ),
                    isActive: index == activeIndex
                ))
            }
            index += 1
        }
        return matches
    }

    private func scheduleScrollToActiveSearchTarget(proxy: ScrollViewProxy) {
        pendingSearchScrollTask?.cancel()
        guard let line = scrollTargetLine, line > 0, line <= index.lineCount else { return }
        let targetID = matchScrollTargetID(for: line)
        let shouldAnimate = activeSearchRange != nil && !accessibilityReduceMotion
        pendingSearchScrollTask = Task { @MainActor in
            await Task.yield()
            guard !Task.isCancelled else { return }
            if shouldAnimate {
                withAnimation(.easeOut(duration: 0.12)) {
                    proxy.scrollTo(targetID, anchor: .center)
                }
            } else {
                proxy.scrollTo(targetID, anchor: .center)
            }
        }
    }

    private func clampScrollIfNeeded(proxy: ScrollViewProxy, viewportHeight: CGFloat) {
        guard let line = layout(viewportHeight: viewportHeight).scrollTargetLineForClampedOffset(Double(verticalOffset)) else {
            return
        }
        proxy.scrollTo(bottomScrollTargetID(for: line), anchor: .bottom)
    }

    /// ScrollViewReader ignores repeated `scrollTo` calls when the identity is unchanged; include the navigation
    /// revision so stepping search matches on the same line still recenters the viewport.
    private func matchScrollTargetID(for line: Int) -> String {
        if let scrollTargetRevision {
            return "line-anchor-\(line)-r\(scrollTargetRevision)"
        }
        return "line-anchor-\(line)"
    }

    private func bottomScrollTargetID(for line: Int) -> String {
        "line-anchor-\(line)"
    }
}

private struct RuneLargeTextLineSearchMatch {
    let range: NSRange
    let isActive: Bool
}

private struct RuneLargeTextLineRow: View {
    let line: RuneLargeTextLine
    let searchMatches: [RuneLargeTextLineSearchMatch]
    let showsLineNumbers: Bool
    let fontSize: CGFloat
    let lineNumberWidth: CGFloat
    let horizontalContentInset: CGFloat
    let isActiveSearchLine: Bool
    @Environment(\.colorScheme) private var colorScheme

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: 10) {
            if showsLineNumbers {
                Text("\(line.number)")
                    .font(.system(size: max(10, fontSize - 1), weight: .regular, design: .monospaced))
                    .foregroundStyle(.tertiary)
                    .frame(width: lineNumberWidth, alignment: .trailing)
                    .textSelection(.disabled)
            }

            Text(highlightedText)
                .font(.system(size: fontSize, weight: .regular, design: .monospaced))
                .foregroundStyle(.primary)
                .textSelection(.enabled)
                .fixedSize(horizontal: true, vertical: false)
        }
        .padding(.horizontal, max(0, horizontalContentInset))
        .frame(maxWidth: .infinity, alignment: .topLeading)
        .background {
            if isActiveSearchLine {
                RoundedRectangle(cornerRadius: 3, style: .continuous)
                    .fill(Color.accentColor.opacity(colorScheme == .dark ? 0.20 : 0.14))
            }
        }
        .overlay {
            if isActiveSearchLine {
                RoundedRectangle(cornerRadius: 3, style: .continuous)
                    .stroke(Color.accentColor.opacity(0.86), lineWidth: 1)
            }
        }
    }

    private var highlightedText: AttributedString {
        let source = line.text.isEmpty ? " " : line.text
        var attributed = AttributedString(source)
        guard !line.text.isEmpty else { return attributed }

        for match in searchMatches {
            guard let stringRange = Range(match.range, in: line.text),
                  let lower = AttributedString.Index(stringRange.lowerBound, within: attributed),
                  let upper = AttributedString.Index(stringRange.upperBound, within: attributed)
            else {
                continue
            }

            let range = lower..<upper
            if match.isActive {
                attributed[range].backgroundColor = Color.accentColor.opacity(
                    colorScheme == .dark ? 0.64 : 0.42
                )
                attributed[range].underlineStyle = Text.LineStyle(
                    pattern: .solid,
                    color: Color.accentColor
                )
            } else {
                attributed[range].backgroundColor = Color(nsColor: .findHighlightColor).opacity(
                    colorScheme == .dark ? 0.52 : 0.62
                )
                attributed[range].underlineStyle = Text.LineStyle(
                    pattern: .solid,
                    color: Color(nsColor: .systemOrange)
                )
            }
        }
        return attributed
    }
}

private struct RuneLargeTextVerticalOffsetPreferenceKey: PreferenceKey {
    static let defaultValue: CGFloat = 0

    static func reduce(value: inout CGFloat, nextValue: () -> CGFloat) {
        value = nextValue()
    }
}
