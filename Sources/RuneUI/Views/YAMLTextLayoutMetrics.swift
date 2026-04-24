import Foundation

struct YAMLIndentGuideMetrics {
    let indentWidth: Int

    init(indentWidth: Int = 2) {
        self.indentWidth = max(1, indentWidth)
    }

    func guideColumn(forLevel level: Int) -> Int {
        max(0, level * indentWidth - 1)
    }

    func guideXPosition(forLevel level: Int, columnWidth: CGFloat, insetX: CGFloat) -> CGFloat {
        insetX + CGFloat(guideColumn(forLevel: level)) * columnWidth
    }

    func guideLevels(forIndentColumns indentColumns: Int) -> [Int] {
        guard indentColumns >= indentWidth else { return [] }
        return Array(1...(indentColumns / indentWidth))
    }
}

struct YAMLTabStopMetrics {
    let indentWidth: Int

    init(indentWidth: Int = 2) {
        self.indentWidth = max(1, indentWidth)
    }

    func defaultInterval(spaceWidth: CGFloat) -> CGFloat {
        max(1, CGFloat(indentWidth) * spaceWidth)
    }
}

struct YAMLTabMarkerMetrics {
    let maxWidth: CGFloat = 16
    let minWidth: CGFloat = 12
    let height: CGFloat = 11

    func markerRect(glyphRect: CGRect, lineRect: CGRect) -> CGRect {
        let width = min(maxWidth, max(minWidth, glyphRect.width - 4))
        return CGRect(
            x: glyphRect.midX - width / 2,
            y: lineRect.minY + (lineRect.height - height) / 2,
            width: width,
            height: height
        )
    }
}
