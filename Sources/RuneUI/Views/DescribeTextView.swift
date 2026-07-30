import SwiftUI

struct DescribeTextSurface: View {
    let text: String
    let minHeight: CGFloat
    let resetID: String
    var searchQuery = ""
    var searchMatchCase = false
    var selectedSearchMatchIndex = 0
    var searchIndex: InspectorFindIndex?
    var searchNavigationRevision = 0

    var body: some View {
        InspectorTextSurface(minHeight: minHeight) {
            InspectorReadOnlyTextView(
                text: text,
                resetID: resetID,
                contentStyle: .describe,
                searchQuery: searchQuery,
                searchMatchCase: searchMatchCase,
                selectedSearchMatchIndex: selectedSearchMatchIndex,
                searchIndex: searchIndex,
                searchMatchRanges: searchIndex?.ranges ?? [],
                searchNavigationRevision: searchNavigationRevision
            )
        }
    }
}
