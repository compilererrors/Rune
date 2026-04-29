import Foundation

public enum TerminalTranscriptSanitizer {
    public static func sanitize(_ text: String) -> String {
        var pendingEscape = ""
        return sanitize(text, pendingEscape: &pendingEscape)
    }

    public static func sanitize(_ text: String, pendingEscape: inout String) -> String {
        let combined = pendingEscape + text
        pendingEscape = ""

        var output = String.UnicodeScalarView()
        var index = combined.unicodeScalars.startIndex

        while index < combined.unicodeScalars.endIndex {
            let scalar = combined.unicodeScalars[index]

            if scalar.value == 0x1B {
                let escapeStart = index
                combined.unicodeScalars.formIndex(after: &index)
                guard index < combined.unicodeScalars.endIndex else {
                    pendingEscape = String(combined.unicodeScalars[escapeStart...])
                    break
                }

                let introducer = combined.unicodeScalars[index]
                combined.unicodeScalars.formIndex(after: &index)

                if introducer == "[" {
                    var foundTerminator = false
                    while index < combined.unicodeScalars.endIndex {
                        let value = combined.unicodeScalars[index].value
                        combined.unicodeScalars.formIndex(after: &index)
                        if (0x40...0x7E).contains(value) {
                            foundTerminator = true
                            break
                        }
                    }
                    if !foundTerminator {
                        pendingEscape = String(combined.unicodeScalars[escapeStart...])
                        break
                    }
                    continue
                }

                if introducer == "]" {
                    var foundTerminator = false
                    while index < combined.unicodeScalars.endIndex {
                        let value = combined.unicodeScalars[index].value
                        if value == 0x07 {
                            combined.unicodeScalars.formIndex(after: &index)
                            foundTerminator = true
                            break
                        }
                        if value == 0x1B {
                            let maybeTerminator = combined.unicodeScalars.index(after: index)
                            if maybeTerminator < combined.unicodeScalars.endIndex,
                               combined.unicodeScalars[maybeTerminator] == "\\" {
                                index = combined.unicodeScalars.index(after: maybeTerminator)
                                foundTerminator = true
                                break
                            }
                        }
                        combined.unicodeScalars.formIndex(after: &index)
                    }
                    if !foundTerminator {
                        pendingEscape = String(combined.unicodeScalars[escapeStart...])
                        break
                    }
                    continue
                }

                continue
            }

            switch scalar.value {
            case 0x08:
                if !output.isEmpty {
                    output.removeLast()
                }
            case 0x09, 0x0A:
                output.append(scalar)
            case 0x0D:
                break
            case 0x00..<0x20:
                break
            default:
                output.append(scalar)
            }

            combined.unicodeScalars.formIndex(after: &index)
        }

        return String(output)
    }
}
