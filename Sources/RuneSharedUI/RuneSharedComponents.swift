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
    private let showsLineNumbers: Bool
    private let fontSize: CGFloat
    private let onNearBottomChange: (Bool) -> Void
    @State private var verticalOffset: CGFloat = 0

    public init(
        text: String,
        placeholder: String = "No output",
        scrollTargetLine: Int? = nil,
        showsLineNumbers: Bool = true,
        fontSize: CGFloat = 12,
        onNearBottomChange: @escaping (Bool) -> Void = { _ in }
    ) {
        self.index = RuneLargeTextIndex(text: text)
        self.placeholder = placeholder
        self.scrollTargetLine = scrollTargetLine
        self.showsLineNumbers = showsLineNumbers
        self.fontSize = fontSize
        self.onNearBottomChange = onNearBottomChange
    }

    public init(
        index: RuneLargeTextIndex,
        placeholder: String = "No output",
        scrollTargetLine: Int? = nil,
        showsLineNumbers: Bool = true,
        fontSize: CGFloat = 12,
        onNearBottomChange: @escaping (Bool) -> Void = { _ in }
    ) {
        self.index = index
        self.placeholder = placeholder
        self.scrollTargetLine = scrollTargetLine
        self.showsLineNumbers = showsLineNumbers
        self.fontSize = fontSize
        self.onNearBottomChange = onNearBottomChange
    }

    public var body: some View {
        ScrollViewReader { proxy in
            GeometryReader { outerProxy in
                ScrollView([.vertical, .horizontal]) {
                    if index.lineCount == 0 {
                        Text(placeholder)
                            .font(.system(size: fontSize, weight: .regular, design: .monospaced))
                            .foregroundStyle(.secondary)
                            .frame(maxWidth: .infinity, alignment: .topLeading)
                            .padding(10)
                    } else {
                        sparseContent(viewportHeight: outerProxy.size.height)
                    }
                }
                .coordinateSpace(name: coordinateSpaceName)
                .onPreferenceChange(RuneLargeTextVerticalOffsetPreferenceKey.self) { offset in
                    let normalizedOffset = max(0, offset)
                    verticalOffset = normalizedOffset
                    onNearBottomChange(isNearBottom(verticalOffset: normalizedOffset, viewportHeight: outerProxy.size.height))
                }
            }
            .background(.secondary.opacity(0.08), in: RoundedRectangle(cornerRadius: 8))
            .onAppear {
                scroll(proxy: proxy, to: scrollTargetLine)
            }
            .onChange(of: scrollTargetLine) { _, line in
                scroll(proxy: proxy, to: line)
            }
        }
    }

    private var coordinateSpaceName: String {
        "RuneLargeTextSurfaceScroll"
    }

    private var rowHeight: CGFloat {
        ceil(fontSize * 1.45)
    }

    private var verticalPadding: CGFloat {
        8
    }

    private var lineNumberColumnWidth: CGFloat {
        guard showsLineNumbers else { return 0 }
        return CGFloat(max(3, String(max(1, index.lineCount)).count)) * max(7, fontSize * 0.66)
    }

    private func sparseContent(viewportHeight: CGFloat) -> some View {
        let visible = visibleViewport(viewportHeight: viewportHeight)
        let contentHeight = max(rowHeight, CGFloat(index.lineCount) * rowHeight + verticalPadding * 2)
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
                Color.clear
                    .frame(width: 1, height: 1)
                    .offset(y: yOffset(for: targetLine))
                    .id(scrollTargetID(for: targetLine))
            }

            VStack(alignment: .leading, spacing: 0) {
                ForEach(visible.lines, id: \.number) { line in
                    RuneLargeTextLineRow(
                        line: line,
                        showsLineNumbers: showsLineNumbers,
                        fontSize: fontSize,
                        lineNumberWidth: lineNumberColumnWidth,
                        isTargetLine: line.number == targetLine
                    )
                    .frame(height: rowHeight, alignment: .topLeading)
                }
            }
            .padding(.trailing, 12)
            .offset(y: yOffset(for: visible.startLine))
        }
        .frame(minWidth: 1, minHeight: contentHeight, alignment: .topLeading)
    }

    private func visibleViewport(viewportHeight: CGFloat) -> RuneLargeTextViewport {
        let overscan = 24
        let visibleLimit = max(1, Int(ceil(max(1, viewportHeight) / rowHeight)))
        let startLine = max(1, Int(floor(max(0, verticalOffset - verticalPadding) / rowHeight)) + 1 - overscan)
        return index.viewport(startLine: startLine, lineLimit: visibleLimit + overscan * 2)
    }

    private func yOffset(for line: Int) -> CGFloat {
        verticalPadding + CGFloat(max(0, line - 1)) * rowHeight
    }

    private func isNearBottom(verticalOffset: CGFloat, viewportHeight: CGFloat) -> Bool {
        let contentHeight = max(rowHeight, CGFloat(index.lineCount) * rowHeight + verticalPadding * 2)
        let maxOffset = max(0, contentHeight - max(1, viewportHeight))
        return maxOffset - verticalOffset < rowHeight * 2
    }

    private func scroll(proxy: ScrollViewProxy, to line: Int?) {
        guard let line, line > 0, line <= index.lineCount else { return }
        proxy.scrollTo(scrollTargetID(for: line), anchor: .center)
    }

    private func scrollTargetID(for line: Int) -> String {
        "line-anchor-\(line)"
    }
}

private struct RuneLargeTextLineRow: View {
    let line: RuneLargeTextLine
    let showsLineNumbers: Bool
    let fontSize: CGFloat
    let lineNumberWidth: CGFloat
    let isTargetLine: Bool

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: 10) {
            if showsLineNumbers {
                Text("\(line.number)")
                    .font(.system(size: max(10, fontSize - 1), weight: .regular, design: .monospaced))
                    .foregroundStyle(.tertiary)
                    .frame(width: lineNumberWidth, alignment: .trailing)
                    .textSelection(.disabled)
            }

            Text(line.text.isEmpty ? " " : line.text)
                .font(.system(size: fontSize, weight: .regular, design: .monospaced))
                .foregroundStyle(.primary)
                .textSelection(.enabled)
                .fixedSize(horizontal: true, vertical: false)
        }
        .padding(.horizontal, 10)
        .frame(maxWidth: .infinity, alignment: .topLeading)
        .background(isTargetLine ? Color.accentColor.opacity(0.16) : Color.clear)
    }
}

private struct RuneLargeTextVerticalOffsetPreferenceKey: PreferenceKey {
    static let defaultValue: CGFloat = 0

    static func reduce(value: inout CGFloat, nextValue: () -> CGFloat) {
        value = nextValue()
    }
}
