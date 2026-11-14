import SwiftUI

/// Scroll-backed AppKit representables always fill the size SwiftUI proposes.
/// Returning it directly keeps SwiftUI from asking AppKit to measure the whole
/// scroll view subtree, which otherwise happens on every scroll frame as rows
/// are added and removed.
enum RuneRepresentableSizing {
    static func filling(_ proposal: ProposedViewSize) -> CGSize? {
        guard let width = proposal.width, let height = proposal.height,
              width.isFinite, height.isFinite else { return nil }
        return CGSize(width: width, height: height)
    }
}
