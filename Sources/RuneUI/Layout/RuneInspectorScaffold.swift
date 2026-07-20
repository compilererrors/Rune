import SwiftUI

enum RuneInspectorBodyScrollBehavior: Sendable, Equatable {
    /// The scaffold owns vertical scrolling for ordinary inspector content.
    case vertical
    /// The body owns its viewport, for example logs, editors, tables, or terminal panels.
    case selfManaged
}

enum RuneInspectorScaffoldMetrics {
    static let supportedMinimumWidth: CGFloat = 320
    static let sectionSpacing: CGFloat = 12
}

enum RuneInspectorScaffoldRegion: Hashable, Sendable {
    case identity
    case info
    case tabs
    case actions
    case body
}

struct RuneInspectorScaffoldLayoutSnapshot: Equatable, Sendable {
    let frames: [RuneInspectorScaffoldRegion: CGRect]

    subscript(_ region: RuneInspectorScaffoldRegion) -> CGRect? {
        frames[region]
    }
}

/// Stable inspector hierarchy: identity and primary utilities, compact core info,
/// tabs, optional actions, then either a vertically scrolling or self-managed body.
struct RuneInspectorScaffold<Info: View, Tabs: View, Actions: View, Body: View>: View {
    let title: String
    let copyAccessibilityLabel: String
    let bodyScrollBehavior: RuneInspectorBodyScrollBehavior
    let showsTabs: Bool
    let showsActions: Bool
    let onCopy: () -> Void
    let onRefresh: () -> Void
    let onLayoutSnapshotChange: ((RuneInspectorScaffoldLayoutSnapshot) -> Void)?
    @ViewBuilder let info: Info
    @ViewBuilder let tabs: Tabs
    @ViewBuilder let actions: Actions
    @ViewBuilder let content: Body

    init(
        title: String,
        copyAccessibilityLabel: String,
        bodyScrollBehavior: RuneInspectorBodyScrollBehavior,
        showsTabs: Bool = true,
        showsActions: Bool = true,
        onCopy: @escaping () -> Void,
        onRefresh: @escaping () -> Void,
        onLayoutSnapshotChange: ((RuneInspectorScaffoldLayoutSnapshot) -> Void)? = nil,
        @ViewBuilder info: () -> Info,
        @ViewBuilder tabs: () -> Tabs,
        @ViewBuilder actions: () -> Actions,
        @ViewBuilder content: () -> Body
    ) {
        self.title = title
        self.copyAccessibilityLabel = copyAccessibilityLabel
        self.bodyScrollBehavior = bodyScrollBehavior
        self.showsTabs = showsTabs
        self.showsActions = showsActions
        self.onCopy = onCopy
        self.onRefresh = onRefresh
        self.onLayoutSnapshotChange = onLayoutSnapshotChange
        self.info = info()
        self.tabs = tabs()
        self.actions = actions()
        self.content = content()
    }

    var body: some View {
        VStack(alignment: .leading, spacing: RuneInspectorScaffoldMetrics.sectionSpacing) {
            identityHeader
                .accessibilityIdentifier("rune.inspector.identity")
                .runeInspectorScaffoldLayoutProbe(.identity, enabled: onLayoutSnapshotChange != nil)

            info
                .frame(maxWidth: .infinity, alignment: .leading)
                .accessibilityIdentifier("rune.inspector.info")
                .runeInspectorScaffoldLayoutProbe(.info, enabled: onLayoutSnapshotChange != nil)

            if showsTabs {
                tabs
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .accessibilityIdentifier("rune.inspector.tabs")
                    .runeInspectorScaffoldLayoutProbe(.tabs, enabled: onLayoutSnapshotChange != nil)
            }

            if showsActions {
                RuneInspectorActionRow {
                    actions
                }
                .accessibilityIdentifier("rune.inspector.actions")
                .runeInspectorScaffoldLayoutProbe(.actions, enabled: onLayoutSnapshotChange != nil)
            }

            bodyContainer
                .accessibilityIdentifier("rune.inspector.body")
                .runeInspectorScaffoldLayoutProbe(.body, enabled: onLayoutSnapshotChange != nil)
        }
        .frame(
            minWidth: 0,
            maxWidth: .infinity,
            maxHeight: .infinity,
            alignment: .topLeading
        )
        .runeInspectorScaffoldLayoutReporting(onLayoutSnapshotChange)
        .accessibilityElement(children: .contain)
    }

    private var identityHeader: some View {
        HStack(alignment: .center, spacing: 6) {
            Text(title)
                .font(.title2.weight(.bold))
                .lineLimit(1)
                .truncationMode(.middle)
                .textSelection(.enabled)
                .help(title)

            Spacer(minLength: 4)

            RuneIconButton(
                copyAccessibilityLabel,
                systemImage: "doc.on.doc",
                action: onCopy
            )
            RuneIconButton(
                "Refresh inspector",
                systemImage: "arrow.clockwise",
                action: onRefresh
            )
        }
        .contextMenu {
            Button(copyAccessibilityLabel, action: onCopy)
        }
        .accessibilityElement(children: .contain)
    }

    @ViewBuilder
    private var bodyContainer: some View {
        switch bodyScrollBehavior {
        case .vertical:
            ScrollView(.vertical) {
                content
                    .frame(maxWidth: .infinity, alignment: .topLeading)
            }
            .scrollContentBackground(.hidden)
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        case .selfManaged:
            content
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                .layoutPriority(1)
        }
    }
}

private enum RuneInspectorScaffoldLayout {
    static let coordinateSpaceName = "RuneInspectorScaffoldLayoutSpace"
}

private struct RuneInspectorScaffoldFramePreferenceKey: PreferenceKey {
    static let defaultValue: [RuneInspectorScaffoldRegion: CGRect] = [:]

    static func reduce(
        value: inout [RuneInspectorScaffoldRegion: CGRect],
        nextValue: () -> [RuneInspectorScaffoldRegion: CGRect]
    ) {
        value.merge(nextValue(), uniquingKeysWith: { _, new in new })
    }
}

private extension View {
    @ViewBuilder
    func runeInspectorScaffoldLayoutProbe(
        _ region: RuneInspectorScaffoldRegion,
        enabled: Bool
    ) -> some View {
        if enabled {
            overlay {
                GeometryReader { proxy in
                    Color.clear.preference(
                        key: RuneInspectorScaffoldFramePreferenceKey.self,
                        value: [
                            region: proxy.frame(
                                in: .named(RuneInspectorScaffoldLayout.coordinateSpaceName)
                            )
                        ]
                    )
                }
                .allowsHitTesting(false)
                .accessibilityHidden(true)
            }
        } else {
            self
        }
    }

    @ViewBuilder
    func runeInspectorScaffoldLayoutReporting(
        _ onChange: ((RuneInspectorScaffoldLayoutSnapshot) -> Void)?
    ) -> some View {
        if let onChange {
            coordinateSpace(name: RuneInspectorScaffoldLayout.coordinateSpaceName)
                .onPreferenceChange(RuneInspectorScaffoldFramePreferenceKey.self) { frames in
                    onChange(RuneInspectorScaffoldLayoutSnapshot(frames: frames))
                }
        } else {
            self
        }
    }
}
