import SwiftUI

struct DescribeTextSurface: View {
    let text: String
    let minHeight: CGFloat
    let resetID: String

    var body: some View {
        InspectorReadOnlyTextSurface(
            text: text,
            minHeight: minHeight,
            resetID: resetID,
            contentStyle: .plainText
        )
    }
}
