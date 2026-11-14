import AppKit
import SwiftUI

/// The one tab bar in Rune: resource kinds, every inspector, Helm, terminal
/// and form choices. It is the native AppKit segmented control, with the
/// system's own chrome and accent-filled selection, at the regular size
/// (24 pt, one step below the 28 pt buttons, 13 pt labels; large at bigger
/// interface font sizes). Segments fit their labels
/// (`.fit` distribution), so every segment has the same padding instead of
/// short labels sitting in stretched segments.
///
/// Every tab stays a tab: when they don't fit, the bar scrolls horizontally
/// with faded edges and keeps the selected tab in view. It never collapses
/// into a dropdown.
struct RuneTabBar<Value: Hashable>: View {
    private let title: String
    @Binding private var selection: Value
    private let tabs: [Value]
    private let label: (Value) -> String

    init(
        _ title: String,
        selection: Binding<Value>,
        tabs: [Value],
        label: @escaping (Value) -> String
    ) {
        self.title = title
        _selection = selection
        self.tabs = tabs
        self.label = label
    }

    var body: some View {
        ViewThatFits(in: .horizontal) {
            segments
            ScrollView(.horizontal, showsIndicators: true) {
                segments
            }
            .mask(edgeFade)
            .accessibilityHint("Scroll horizontally to reveal additional choices")
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityIdentifier("segmented-picker-tabs")
    }

    private var segments: some View {
        RuneSegmentedControl(
            title: title,
            labels: tabs.map(label),
            selectedIndex: tabs.firstIndex(of: selection),
            onSelect: { index in
                guard tabs.indices.contains(index), tabs[index] != selection else { return }
                selection = tabs[index]
            }
        )
        .fixedSize()
    }

    private var edgeFade: some View {
        HStack(spacing: 0) {
            LinearGradient(colors: [.clear, .black], startPoint: .leading, endPoint: .trailing)
                .frame(width: RuneSpacing.sm)
            Rectangle().fill(.black)
            LinearGradient(colors: [.black, .clear], startPoint: .leading, endPoint: .trailing)
                .frame(width: RuneSpacing.sm)
        }
    }
}

/// Native segmented control behind `RuneTabBar`.
struct RuneSegmentedControl: NSViewRepresentable {
    let title: String
    let labels: [String]
    let selectedIndex: Int?
    let onSelect: (Int) -> Void
    @Environment(\.runeInterfaceFontSize) private var fontSize
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    func makeCoordinator() -> Coordinator {
        Coordinator(onSelect: onSelect)
    }

    func makeNSView(context: Context) -> NSSegmentedControl {
        let control = NSSegmentedControl(
            labels: labels,
            trackingMode: .selectOne,
            target: context.coordinator,
            action: #selector(Coordinator.changed(_:))
        )
        control.segmentDistribution = .fit
        control.setContentHuggingPriority(.required, for: .horizontal)
        if !title.isEmpty { control.setAccessibilityLabel(title) }
        apply(to: control)
        return control
    }

    func updateNSView(_ control: NSSegmentedControl, context: Context) {
        context.coordinator.onSelect = onSelect
        apply(to: control)
    }

    private func apply(to control: NSSegmentedControl) {
        if control.segmentCount != labels.count {
            control.segmentCount = labels.count
        }
        for (index, text) in labels.enumerated() where control.label(forSegment: index) != text {
            control.setLabel(text, forSegment: index)
        }
        let size = RuneControlSize.regular.nativeControlSize(
            interfaceFontSize: fontSize,
            isAccessibilitySize: dynamicTypeSize.isAccessibilitySize
        )
        let nsSize: NSControl.ControlSize = size == .large ? .large : .regular
        if control.controlSize != nsSize { control.controlSize = nsSize }
        let font = NSFont.systemFont(ofSize: fontSize + RuneControlSize.regular.labelRelativeSize)
        if control.font != font { control.font = font }
        let target = selectedIndex ?? -1
        if control.selectedSegment != target {
            control.selectedSegment = target
            if target >= 0 { scrollSelectionIntoView(control, index: target) }
        }
    }

    /// Keeps the selected segment visible when the bar scrolls; a no-op when
    /// everything fits.
    private func scrollSelectionIntoView(_ control: NSSegmentedControl, index: Int) {
        DispatchQueue.main.async {
            guard control.enclosingScrollView != nil, control.segmentCount > 0 else { return }
            let width = control.bounds.width / CGFloat(control.segmentCount)
            control.scrollToVisible(NSRect(x: width * CGFloat(index), y: 0, width: width, height: control.bounds.height))
        }
    }

    @MainActor
    final class Coordinator: NSObject {
        var onSelect: (Int) -> Void

        init(onSelect: @escaping (Int) -> Void) {
            self.onSelect = onSelect
        }

        @objc func changed(_ sender: NSSegmentedControl) {
            onSelect(sender.selectedSegment)
        }
    }
}
