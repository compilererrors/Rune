import AppKit
import Foundation

enum ResourceLogANSIFormatter {
    static func attributedString(from text: String, font: NSFont) -> NSAttributedString {
        let output = NSMutableAttributedString()
        var attributes = baseAttributes(font: font)
        var index = text.startIndex

        while index < text.endIndex {
            if text[index] == "\u{1B}",
               let parsed = parseSGRSequence(in: text, from: index)
            {
                attributes = applying(parsed.codes, to: attributes, font: font)
                index = parsed.endIndex
                continue
            }

            let nextEscape = text[index...].firstIndex(of: "\u{1B}") ?? text.endIndex
            output.append(NSAttributedString(
                string: String(text[index..<nextEscape]),
                attributes: attributes
            ))
            index = nextEscape
        }

        return output
    }

    static func plainText(from text: String) -> String {
        var output = ""
        var index = text.startIndex
        while index < text.endIndex {
            if text[index] == "\u{1B}",
               let parsed = parseSGRSequence(in: text, from: index)
            {
                index = parsed.endIndex
                continue
            }

            output.append(text[index])
            index = text.index(after: index)
        }
        return output
    }

    private static func parseSGRSequence(in text: String, from start: String.Index) -> (codes: [Int], endIndex: String.Index)? {
        var index = text.index(after: start)
        guard index < text.endIndex, text[index] == "[" else { return nil }
        index = text.index(after: index)

        let codesStart = index
        while index < text.endIndex, text[index] != "m" {
            let scalar = text[index].unicodeScalars.first?.value ?? 0
            guard (48...57).contains(scalar) || scalar == 59 else { return nil }
            index = text.index(after: index)
        }

        guard index < text.endIndex else { return nil }
        let rawCodes = text[codesStart..<index]
        let codes = rawCodes.isEmpty
            ? [0]
            : rawCodes.split(separator: ";", omittingEmptySubsequences: false).map { Int($0) ?? 0 }
        return (codes, text.index(after: index))
    }

    private static func applying(
        _ codes: [Int],
        to current: [NSAttributedString.Key: Any],
        font: NSFont
    ) -> [NSAttributedString.Key: Any] {
        var attributes = current
        for code in codes {
            switch code {
            case 0:
                attributes = baseAttributes(font: font)
            case 1:
                attributes[.font] = NSFont.monospacedSystemFont(ofSize: font.pointSize, weight: .semibold)
            case 22:
                attributes[.font] = font
            case 30...37:
                attributes[.foregroundColor] = color(for: code - 30, bright: false)
            case 39:
                attributes[.foregroundColor] = NSColor.labelColor
            case 90...97:
                attributes[.foregroundColor] = color(for: code - 90, bright: true)
            default:
                continue
            }
        }
        return attributes
    }

    private static func baseAttributes(font: NSFont) -> [NSAttributedString.Key: Any] {
        [
            .font: font,
            .foregroundColor: NSColor.labelColor,
            .paragraphStyle: paragraphStyle(font: font)
        ]
    }

    private static func paragraphStyle(font: NSFont) -> NSParagraphStyle {
        let style = NSMutableParagraphStyle()
        style.lineBreakMode = .byClipping
        style.defaultTabInterval = font.maximumAdvancement.width * 4
        return style
    }

    private static func color(for index: Int, bright: Bool) -> NSColor {
        switch (index, bright) {
        case (0, false): return .secondaryLabelColor
        case (0, true): return .tertiaryLabelColor
        case (1, false): return .systemRed
        case (1, true): return .systemPink
        case (2, false): return .systemGreen
        case (2, true): return .systemMint
        case (3, false): return .systemOrange
        case (3, true): return .systemYellow
        case (4, false): return .systemBlue
        case (4, true): return .systemCyan
        case (5, false): return .systemPurple
        case (5, true): return .systemPurple
        case (6, false): return .systemTeal
        case (6, true): return .systemCyan
        default: return .labelColor
        }
    }
}
