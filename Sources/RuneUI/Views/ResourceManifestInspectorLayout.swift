import SwiftUI

struct ResourceManifestInspectorLayout<Header: View, Toolbar: View, Status: View, Surface: View, Footer: View>: View {
    @ViewBuilder var header: Header
    @ViewBuilder var toolbar: Toolbar
    @ViewBuilder var status: Status
    @ViewBuilder var surface: Surface
    @ViewBuilder var footer: Footer

    init(
        @ViewBuilder header: () -> Header,
        @ViewBuilder toolbar: () -> Toolbar,
        @ViewBuilder status: () -> Status,
        @ViewBuilder surface: () -> Surface,
        @ViewBuilder footer: () -> Footer
    ) {
        self.header = header()
        self.toolbar = toolbar()
        self.status = status()
        self.surface = surface()
        self.footer = footer()
    }

    var body: some View {
        VStack(alignment: .leading, spacing: RuneUILayoutMetrics.inspectorSectionSpacing) {
            header
                .frame(maxWidth: .infinity, alignment: .leading)

            toolbar
                .frame(maxWidth: .infinity, alignment: .leading)

            status
                .frame(maxWidth: .infinity, alignment: .leading)

            surface
                .frame(maxWidth: .infinity, alignment: .leading)
                .layoutPriority(1)

            footer
                .frame(maxWidth: .infinity, alignment: .leading)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
}
