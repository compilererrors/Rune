import AppKit
import OSLog
import RuneCore
import RuneSecurity
import SwiftUI

enum PodTableLayout {
    static let metricsSpacing: CGFloat = 10
    static let rowHorizontalPadding: CGFloat = 10
    static let rowVerticalPadding: CGFloat = 5
    static let listRowEdgeInset: CGFloat = 4
    static let nameColumnDefaultWidth: CGFloat = 260
    static let nameColumnMinimumWidth: CGFloat = 104
    static let nameColumnMaximumWidth: CGFloat = 820
    static let nameColumnResizeHandleWidth: CGFloat = 14
    static let cpuWidth: CGFloat = 74
    static let memoryWidth: CGFloat = 80
    static let restartsWidth: CGFloat = 100
    static let ageWidth: CGFloat = 74
    static let statusTextWidth: CGFloat = 94
    static let statusHorizontalPadding: CGFloat = 8
    static let statusTotalWidth: CGFloat = statusTextWidth + (statusHorizontalPadding * 2)
    static let headerHorizontalInset: CGFloat = rowHorizontalPadding
    static let selectionColumnWidth: CGFloat = 30
    static let favoriteColumnWidth: CGFloat = 34
    static let minimumScrollableWidth: CGFloat = 620
    /// Space between column headers and first row — enough to avoid a cramped look without excess air.
    static let headerBottomSpacing: CGFloat = 10

    static func clampedNameColumnWidth(_ width: CGFloat) -> CGFloat {
        let pixelAlignedWidth = width.rounded(.toNearestOrAwayFromZero)
        return min(nameColumnMaximumWidth, max(nameColumnMinimumWidth, pixelAlignedWidth))
    }

    static func minimumScrollableWidth(nameColumnWidth: CGFloat) -> CGFloat {
        max(
            minimumScrollableWidth,
            rowHorizontalPadding * 2
                + selectionColumnWidth
                + nameColumnFrameWidth(nameColumnWidth)
                + metricsColumnGroupWidth
                + statusTotalWidth
                + favoriteColumnWidth
                + (metricsSpacing * 4)
        )
    }

    static func nameColumnFrameWidth(_ width: CGFloat) -> CGFloat {
        clampedNameColumnWidth(width) + nameColumnResizeHandleWidth
    }

    static var metricsColumnGroupWidth: CGFloat {
        cpuWidth
            + memoryWidth
            + restartsWidth
            + ageWidth
            + (metricsSpacing * 3)
    }

    static var defaultAppKitTableWidth: CGFloat {
        selectionColumnWidth
            + nameColumnDefaultWidth
            + cpuWidth
            + memoryWidth
            + restartsWidth
            + ageWidth
            + statusTotalWidth
            + favoriteColumnWidth
    }
}
