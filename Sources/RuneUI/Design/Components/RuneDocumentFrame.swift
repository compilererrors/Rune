import SwiftUI

private struct RuneDocumentFrameEmbeddedKey: EnvironmentKey {
    static let defaultValue = false
}

private struct RuneSurfaceTabRoleKey: EnvironmentKey {
    static let defaultValue: RuneSurfaceRole = .document
}

private struct RuneSurfaceTabsKey: EnvironmentKey {
    static var defaultValue: AnyView? { nil }
}

extension EnvironmentValues {
    /// True inside a `RuneDocumentFrame`: toolbar groups, status strips and
    /// text surfaces drop their own cards and frames, so the frame is the one
    /// container level.
    var runeDocumentFrameEmbedded: Bool {
        get { self[RuneDocumentFrameEmbeddedKey.self] }
        set { self[RuneDocumentFrameEmbeddedKey.self] = newValue }
    }

    /// The role of the surface a tab strip sits on, so the selected tab takes
    /// that surface's fill and merges into it.
    var runeSurfaceTabRole: RuneSurfaceRole {
        get { self[RuneSurfaceTabRoleKey.self] }
        set { self[RuneSurfaceTabRoleKey.self] = newValue }
    }

    /// Tabs a container hands to the document view it hosts (for example
    /// the Terminal's pod log tabs), drawn on that document's top edge.
    var runeSurfaceTabs: AnyView? {
        get { self[RuneSurfaceTabsKey.self] }
        set { self[RuneSurfaceTabsKey.self] = newValue }
    }
}

/// One framed document (YAML, Describe, Logs, the shell transcript) with
/// the actions that operate on it in a header bar attached to its top edge,
/// optional tabs that switch it sitting on its top edge (the selected tab
/// merges into the surface), and an optional slim status line attached to
/// its bottom edge, as in Xcode's editor, Terminal and Console. Hairlines
/// separate the bars; there are no cards inside the frame.
struct RuneDocumentFrame<Toolbar: View, Tabs: View, Content: View, Status: View>: View {
    let role: RuneSurfaceRole
    let showsTabs: Bool
    let showsStatus: Bool
    @ViewBuilder let toolbar: Toolbar
    @ViewBuilder let tabs: Tabs
    @ViewBuilder let content: Content
    @ViewBuilder let status: Status
    @Environment(\.runeThemePalette) private var palette
    @Environment(\.colorScheme) private var colorScheme

    init(
        role: RuneSurfaceRole = .document,
        showsTabs: Bool = true,
        showsStatus: Bool = true,
        @ViewBuilder toolbar: () -> Toolbar,
        @ViewBuilder tabs: () -> Tabs,
        @ViewBuilder content: () -> Content,
        @ViewBuilder status: () -> Status
    ) {
        self.role = role
        self.showsTabs = showsTabs
        self.showsStatus = showsStatus
        self.toolbar = toolbar()
        self.tabs = tabs()
        self.content = content()
        self.status = status()
    }

    var body: some View {
        let shape = RoundedRectangle(cornerRadius: RuneUILayoutMetrics.documentFrameCornerRadius, style: .continuous)
        let frameColor = role.frame(palette: palette)
        let hasToolbar = Toolbar.self != EmptyView.self
        let hasTabs = showsTabs && Tabs.self != EmptyView.self
        VStack(spacing: 0) {
            if hasToolbar {
                toolbar
                    .padding(.horizontal, RuneUILayoutMetrics.documentFrameBarInset)
                    .padding(.vertical, RuneUILayoutMetrics.documentFrameBarVerticalInset)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(RuneSurfaceRole.raised.fill(palette: palette, colorScheme: colorScheme))
                    .accessibilityIdentifier("rune.document.toolbar")
                frameColor.frame(height: 1)
            }

            if hasTabs {
                // No hairline below: the selected tab opens into the surface.
                // Native tab bars run edge to edge; the surface starts right under them.
                tabs
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(role.tabStripFill(palette: palette, colorScheme: colorScheme))
                    .environment(\.runeSurfaceTabRole, role)
                    .accessibilityIdentifier("rune.document.tabs")
            }

            content
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                .layoutPriority(1)

            if showsStatus, Status.self != EmptyView.self {
                frameColor.frame(height: 1)
                status
                    .padding(.horizontal, RuneUILayoutMetrics.documentFrameBarInset)
                    .padding(.vertical, RuneUILayoutMetrics.documentFrameStatusVerticalInset)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(RuneSurfaceRole.raised.fill(palette: palette, colorScheme: colorScheme))
                    .accessibilityIdentifier("rune.document.status")
            }
        }
        .background(role.fill(palette: palette, colorScheme: colorScheme))
        .clipShape(shape)
        .overlay {
            shape
                .strokeBorder(frameColor, lineWidth: 1)
                .allowsHitTesting(false)
        }
        .environment(\.runeDocumentFrameEmbedded, true)
    }
}

extension RuneDocumentFrame where Tabs == EmptyView {
    init(
        role: RuneSurfaceRole = .document,
        showsStatus: Bool = true,
        @ViewBuilder toolbar: () -> Toolbar,
        @ViewBuilder content: () -> Content,
        @ViewBuilder status: () -> Status
    ) {
        self.init(role: role, showsTabs: false, showsStatus: showsStatus, toolbar: toolbar, tabs: { EmptyView() }, content: content, status: status)
    }
}

extension RuneDocumentFrame where Tabs == EmptyView, Status == EmptyView {
    init(
        role: RuneSurfaceRole = .document,
        @ViewBuilder toolbar: () -> Toolbar,
        @ViewBuilder content: () -> Content
    ) {
        self.init(role: role, showsTabs: false, showsStatus: false, toolbar: toolbar, tabs: { EmptyView() }, content: content) { EmptyView() }
    }
}

extension RuneDocumentFrame where Status == EmptyView {
    init(
        role: RuneSurfaceRole = .document,
        showsTabs: Bool = true,
        @ViewBuilder toolbar: () -> Toolbar,
        @ViewBuilder tabs: () -> Tabs,
        @ViewBuilder content: () -> Content
    ) {
        self.init(role: role, showsTabs: showsTabs, showsStatus: false, toolbar: toolbar, tabs: tabs, content: content) { EmptyView() }
    }
}
