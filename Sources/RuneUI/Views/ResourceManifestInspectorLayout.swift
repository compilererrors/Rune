import SwiftUI

/// The manifest inspectors (YAML, Describe): an optional header line, then
/// one framed document whose actions sit in a bar attached to its top edge
/// and whose status is a slim line attached to its bottom edge.
struct ResourceManifestInspectorLayout<Header: View, Toolbar: View, Status: View, Surface: View, Footer: View>: View {
    let showsStatus: Bool
    @ViewBuilder var header: Header
    @ViewBuilder var toolbar: Toolbar
    @ViewBuilder var status: Status
    @ViewBuilder var surface: Surface
    @ViewBuilder var footer: Footer

    init(
        showsStatus: Bool = true,
        @ViewBuilder header: () -> Header,
        @ViewBuilder toolbar: () -> Toolbar,
        @ViewBuilder status: () -> Status,
        @ViewBuilder surface: () -> Surface,
        @ViewBuilder footer: () -> Footer
    ) {
        self.showsStatus = showsStatus
        self.header = header()
        self.toolbar = toolbar()
        self.status = status()
        self.surface = surface()
        self.footer = footer()
    }

    var body: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorSectionSpacing) {
            if Header.self != EmptyView.self {
                header
                    .frame(maxWidth: .infinity, alignment: .leading)
            }

            RuneDocumentFrame(showsStatus: showsStatus) {
                toolbar
            } content: {
                surface
            } status: {
                status
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .layoutPriority(1)

            if Footer.self != EmptyView.self {
                footer
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
}
