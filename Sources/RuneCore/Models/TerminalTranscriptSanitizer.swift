import Foundation

public enum TerminalTranscriptSanitizer {
    public static func sanitize(_ text: String) -> String {
        var pendingEscape = ""
        return sanitize(text, pendingEscape: &pendingEscape)
    }

    public static func sanitize(_ text: String, pendingEscape: inout String) -> String {
        // Avoid copying the complete transcript for the overwhelmingly common case where
        // the previous chunk did not end halfway through an escape sequence.
        let combined = pendingEscape.isEmpty ? text : pendingEscape + text
        pendingEscape = ""

        // Terminal control syntax is byte-oriented. Walking UTF-8 directly avoids the
        // grapheme/scalar indexing overhead that otherwise dominates large transcripts,
        // while copying non-control UTF-8 bytes without changing their Unicode content.
        let input = Array(combined.utf8)
        var output: [UInt8] = []
        output.reserveCapacity(input.count)
        var index = 0

        while index < input.count {
            let byte = input[index]

            if byte == 0x1B {
                let escapeStart = index
                index += 1
                guard index < input.count else {
                    pendingEscape = String(decoding: input[escapeStart...], as: UTF8.self)
                    break
                }

                let introducer = input[index]
                index += 1

                if introducer == 0x5B { // [ — Control Sequence Introducer
                    var foundTerminator = false
                    var terminator: UInt8?
                    let parameterStart = index
                    while index < input.count {
                        let current = input[index]
                        index += 1
                        if (0x40...0x7E).contains(current) {
                            terminator = current
                            foundTerminator = true
                            break
                        }
                    }
                    if !foundTerminator {
                        pendingEscape = String(decoding: input[escapeStart...], as: UTF8.self)
                        break
                    }
                    if terminator == 0x4B { // K — erase line
                        removeCurrentLine(from: &output)
                    } else if terminator == 0x4A { // J — erase display
                        output.removeAll(keepingCapacity: true)
                    } else if terminator == 0x44 { // D — cursor left
                        let parameterEnd = max(parameterStart, index - 1)
                        let count = parseFirstPositiveInteger(input[parameterStart..<parameterEnd]) ?? 1
                        removeScalarsFromCurrentLine(count, from: &output)
                    }
                    continue
                }

                if introducer == 0x5D { // ] — Operating System Command
                    var foundTerminator = false
                    while index < input.count {
                        let current = input[index]
                        if current == 0x07 {
                            index += 1
                            foundTerminator = true
                            break
                        }
                        if current == 0x1B,
                           index + 1 < input.count,
                           input[index + 1] == 0x5C {
                                index += 2
                                foundTerminator = true
                                break
                        }
                        index += 1
                    }
                    if !foundTerminator {
                        pendingEscape = String(decoding: input[escapeStart...], as: UTF8.self)
                        break
                    }
                    continue
                }

                if Self.isCharsetEscapeIntroducer(introducer) {
                    guard index < input.count else {
                        pendingEscape = String(decoding: input[escapeStart...], as: UTF8.self)
                        break
                    }
                    index += 1
                    continue
                }

                continue
            }

            switch byte {
            case 0x08:
                removeLastUnicodeScalar(from: &output)
            case 0x09, 0x0A:
                output.append(byte)
            case 0x0D:
                if index + 1 < input.count, input[index + 1] == 0x0A {
                    break
                } else {
                    removeCurrentLine(from: &output)
                }
            case 0x00..<0x20:
                break
            default:
                output.append(byte)
            }

            index += 1
        }

        return String(decoding: output, as: UTF8.self)
    }

    private static func isCharsetEscapeIntroducer(_ byte: UInt8) -> Bool {
        switch byte {
        case 0x28, 0x29, 0x2A, 0x2B, 0x2D, 0x2E, 0x2F:
            return true
        default:
            return false
        }
    }

    private static func removeCurrentLine(from output: inout [UInt8]) {
        while output.last != 0x0A, !output.isEmpty {
            output.removeLast()
        }
    }

    private static func removeScalarsFromCurrentLine(_ count: Int, from output: inout [UInt8]) {
        guard count > 0 else { return }
        var remaining = count
        while remaining > 0, output.last != 0x0A, !output.isEmpty {
            removeLastUnicodeScalar(from: &output)
            remaining -= 1
        }
    }

    private static func removeLastUnicodeScalar(from output: inout [UInt8]) {
        guard let last = output.popLast() else { return }
        guard last & 0xC0 == 0x80 else { return }
        while let continuation = output.last, continuation & 0xC0 == 0x80 {
            output.removeLast()
        }
        if output.last != 0x0A {
            _ = output.popLast()
        }
    }

    private static func parseFirstPositiveInteger(_ bytes: ArraySlice<UInt8>) -> Int? {
        var value = 0
        var foundDigit = false
        for byte in bytes {
            if byte == 0x3B { break }
            guard (0x30...0x39).contains(byte) else { return nil }
            foundDigit = true
            value = value * 10 + Int(byte - 0x30)
        }
        guard foundDigit, value > 0 else { return nil }
        return value
    }
}
