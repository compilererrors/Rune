import AppKit
import SwiftUI

enum RuneCursorIntent: Equatable {
    case pointer
    case textInput

    var cursor: NSCursor {
        switch self {
        case .pointer:
            return .arrow
        case .textInput:
            return .iBeam
        }
    }
}

private struct RuneCursorScopeActiveKey: EnvironmentKey {
    static let defaultValue = false
}

private extension EnvironmentValues {
    var runeCursorScopeIsActive: Bool {
        get { self[RuneCursorScopeActiveKey.self] }
        set { self[RuneCursorScopeActiveKey.self] = newValue }
    }
}

private struct RuneCursorRegionAnchor {
    let intent: RuneCursorIntent
    let bounds: Anchor<CGRect>
}

private struct RuneCursorRegionPreferenceKey: PreferenceKey {
    static let defaultValue: [RuneCursorRegionAnchor] = []

    static func reduce(
        value: inout [RuneCursorRegionAnchor],
        nextValue: () -> [RuneCursorRegionAnchor]
    ) {
        value.append(contentsOf: nextValue())
    }
}

private struct RuneCursorRectModifier: ViewModifier {
    let intent: RuneCursorIntent
    @Environment(\.runeCursorScopeIsActive) private var cursorScopeIsActive

    func body(content: Content) -> some View {
        content
            .transformAnchorPreference(
                key: RuneCursorRegionPreferenceKey.self,
                value: .bounds
            ) { regions, bounds in
                regions.append(RuneCursorRegionAnchor(
                    intent: intent,
                    bounds: bounds
                ))
            }
            .background {
                // A cursor scope owns routing for all of its descendants.
                if !cursorScopeIsActive {
                    RuneCursorRectRepresentable(intent: intent)
                        .allowsHitTesting(false)
                }
            }
    }
}

private struct RuneCursorScopeModifier: ViewModifier {
    let defaultIntent: RuneCursorIntent

    func body(content: Content) -> some View {
        content
            .environment(\.runeCursorScopeIsActive, true)
            .overlayPreferenceValue(RuneCursorRegionPreferenceKey.self) { anchors in
                GeometryReader { proxy in
                    RuneCursorScopeRepresentable(
                        defaultIntent: defaultIntent,
                        regions: anchors.map {
                            RuneResolvedCursorRegion(
                                intent: $0.intent,
                                rect: proxy[$0.bounds]
                            )
                        }
                    )
                    .allowsHitTesting(false)
                }
            }
    }
}

private struct RuneResolvedCursorRegion: Equatable {
    let intent: RuneCursorIntent
    let rect: CGRect
}

private struct RuneCursorRectRepresentable: NSViewRepresentable {
    let intent: RuneCursorIntent

    func makeNSView(context: Context) -> RuneCursorRectView {
        RuneCursorRectView(intent: intent)
    }

    func updateNSView(_ view: RuneCursorRectView, context: Context) {
        view.intent = intent
    }
}

private struct RuneCursorScopeRepresentable: NSViewRepresentable {
    let defaultIntent: RuneCursorIntent
    let regions: [RuneResolvedCursorRegion]

    func makeNSView(context: Context) -> RuneCursorScopeView {
        RuneCursorScopeView(
            defaultIntent: defaultIntent,
            regions: regions
        )
    }

    func updateNSView(_ view: RuneCursorScopeView, context: Context) {
        view.update(
            defaultIntent: defaultIntent,
            regions: regions
        )
    }
}

final class RuneCursorRectView: NSView {
    private var cursorTrackingArea: NSTrackingArea?

    var intent: RuneCursorIntent {
        didSet {
            guard intent != oldValue else { return }
            updateCursorIfPointerIsInside()
        }
    }

    init(intent: RuneCursorIntent) {
        self.intent = intent
        super.init(frame: .zero)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        nil
    }

    override func updateTrackingAreas() {
        super.updateTrackingAreas()

        if let cursorTrackingArea {
            removeTrackingArea(cursorTrackingArea)
        }

        let trackingArea = NSTrackingArea(
            rect: .zero,
            options: [.cursorUpdate, .activeInKeyWindow, .inVisibleRect],
            owner: self,
            userInfo: nil
        )
        addTrackingArea(trackingArea)
        cursorTrackingArea = trackingArea
    }

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        updateCursorIfPointerIsInside()
    }

    override func cursorUpdate(with event: NSEvent) {
        intent.cursor.set()
    }

    override func hitTest(_ point: NSPoint) -> NSView? {
        nil
    }

    private func updateCursorIfPointerIsInside() {
        guard let window, window.isKeyWindow else { return }
        let pointer = convert(window.mouseLocationOutsideOfEventStream, from: nil)
        guard visibleRect.contains(pointer) else { return }
        intent.cursor.set()
    }
}

final class RuneCursorScopeView: NSView {
    private var cursorTrackingArea: NSTrackingArea?
    private var defaultIntent: RuneCursorIntent
    private var regions: [RuneResolvedCursorRegion]

    override var isFlipped: Bool { true }
    override var acceptsFirstResponder: Bool { false }

    fileprivate init(
        defaultIntent: RuneCursorIntent,
        regions: [RuneResolvedCursorRegion]
    ) {
        self.defaultIntent = defaultIntent
        self.regions = regions
        super.init(frame: .zero)
    }

    @available(*, unavailable)
    required init?(coder: NSCoder) {
        nil
    }

    fileprivate func update(
        defaultIntent: RuneCursorIntent,
        regions: [RuneResolvedCursorRegion]
    ) {
        guard self.defaultIntent != defaultIntent || self.regions != regions else {
            return
        }
        self.defaultIntent = defaultIntent
        self.regions = regions
        updateCursorIfPointerIsInside()
    }

    override func updateTrackingAreas() {
        super.updateTrackingAreas()

        if let cursorTrackingArea {
            removeTrackingArea(cursorTrackingArea)
        }

        let trackingArea = NSTrackingArea(
            rect: .zero,
            options: [
                .cursorUpdate,
                .mouseMoved,
                .mouseEnteredAndExited,
                .activeInKeyWindow,
                .inVisibleRect
            ],
            owner: self,
            userInfo: nil
        )
        addTrackingArea(trackingArea)
        cursorTrackingArea = trackingArea
    }

    override func cursorUpdate(with event: NSEvent) {
        updateCursor(for: event)
    }

    override func mouseEntered(with event: NSEvent) {
        updateCursor(for: event)
    }

    override func mouseMoved(with event: NSEvent) {
        updateCursor(for: event)
    }

    func cursorIntent(at point: NSPoint) -> RuneCursorIntent? {
        let cursorBounds = bounds.intersection(visibleRect)
        guard cursorBounds.contains(point) else { return nil }

        return disjointExplicitRegions(in: cursorBounds)
            .first { $0.rect.contains(point) }?
            .intent
            ?? defaultIntent
    }

    func cursorRegionRects(for intent: RuneCursorIntent) -> [CGRect] {
        regions
            .filter { $0.intent == intent }
            .map(\.rect)
    }

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        updateCursorIfPointerIsInside()
    }

    override func setFrameSize(_ newSize: NSSize) {
        let sizeChanged = frame.size != newSize
        super.setFrameSize(newSize)
        if sizeChanged {
            updateCursorIfPointerIsInside()
        }
    }

    override func hitTest(_ point: NSPoint) -> NSView? {
        nil
    }

    private func updateCursor(for event: NSEvent) {
        let point = convert(event.locationInWindow, from: nil)
        cursorIntent(at: point)?.cursor.set()
    }

    private func updateCursorIfPointerIsInside() {
        guard let window, window.isKeyWindow else { return }
        let point = convert(window.mouseLocationOutsideOfEventStream, from: nil)
        cursorIntent(at: point)?.cursor.set()
    }

    private func disjointExplicitRegions(
        in cursorBounds: CGRect
    ) -> [RuneResolvedCursorRegion] {
        var result: [RuneResolvedCursorRegion] = []

        for region in regions where region.intent != defaultIntent {
            let clippedRect = region.rect.intersection(cursorBounds)
            guard !clippedRect.isNull, !clippedRect.isEmpty else { continue }

            var remainingRects = [clippedRect]
            for claimedRegion in result {
                remainingRects = remainingRects.flatMap {
                    Self.subtract(claimedRegion.rect, from: $0)
                }
            }
            result.append(contentsOf: remainingRects.map {
                RuneResolvedCursorRegion(intent: region.intent, rect: $0)
            })
        }

        return result
    }

    private static func subtract(_ excludedRect: CGRect, from sourceRect: CGRect) -> [CGRect] {
        let intersection = sourceRect.intersection(excludedRect)
        guard !intersection.isNull, !intersection.isEmpty else {
            return [sourceRect]
        }

        var result: [CGRect] = []

        if intersection.minY > sourceRect.minY {
            result.append(CGRect(
                x: sourceRect.minX,
                y: sourceRect.minY,
                width: sourceRect.width,
                height: intersection.minY - sourceRect.minY
            ))
        }
        if intersection.maxY < sourceRect.maxY {
            result.append(CGRect(
                x: sourceRect.minX,
                y: intersection.maxY,
                width: sourceRect.width,
                height: sourceRect.maxY - intersection.maxY
            ))
        }
        if intersection.minX > sourceRect.minX {
            result.append(CGRect(
                x: sourceRect.minX,
                y: intersection.minY,
                width: intersection.minX - sourceRect.minX,
                height: intersection.height
            ))
        }
        if intersection.maxX < sourceRect.maxX {
            result.append(CGRect(
                x: intersection.maxX,
                y: intersection.minY,
                width: sourceRect.maxX - intersection.maxX,
                height: intersection.height
            ))
        }

        return result.filter { !$0.isEmpty }
    }
}

extension View {
    func runeCursorScope(default defaultIntent: RuneCursorIntent) -> some View {
        modifier(RuneCursorScopeModifier(defaultIntent: defaultIntent))
    }

    func runePointerCursor() -> some View {
        modifier(RuneCursorRectModifier(intent: .pointer))
    }

    func runeTextInputCursor() -> some View {
        modifier(RuneCursorRectModifier(intent: .textInput))
    }
}
