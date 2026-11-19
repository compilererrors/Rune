import Foundation

/// Decodes UTF-8 that arrives in pieces, such as shell output in WebSocket
/// frames. A character split between two pieces is held back until the rest
/// of it arrives; decoding each piece on its own would show a replacement
/// character on both sides of the split.
public struct UTF8StreamDecoder: Sendable {
    /// The start of a character whose remaining bytes have not arrived: at
    /// most three bytes.
    private var pending = Data()

    public init() {}

    public mutating func decode(_ data: Data) -> String {
        guard !data.isEmpty else { return "" }
        var input: Data
        if pending.isEmpty {
            input = data
        } else {
            input = pending
            input.append(data)
            pending = Data()
        }
        let incomplete = Self.incompleteSuffixLength(input)
        if incomplete > 0 {
            pending = Data(input.suffix(incomplete))
            input = input.dropLast(incomplete)
        }
        return String(decoding: input, as: UTF8.self)
    }

    /// Ends the stream: a character that never completed becomes a
    /// replacement character.
    public mutating func finish() -> String {
        guard !pending.isEmpty else { return "" }
        defer { pending = Data() }
        return String(decoding: pending, as: UTF8.self)
    }

    /// How many bytes at the end begin a character without completing it.
    /// Bytes that are not valid UTF-8 are not held back: the decoder
    /// replaces them where they stand.
    static func incompleteSuffixLength(_ data: Data) -> Int {
        let tail = Array(data.suffix(4))
        var continuationCount = 0
        for byte in tail.reversed() {
            if byte & 0xC0 == 0x80 {
                continuationCount += 1
                continue
            }
            let length: Int
            if byte & 0x80 == 0 {
                length = 1
            } else if byte & 0xE0 == 0xC0 {
                length = 2
            } else if byte & 0xF0 == 0xE0 {
                length = 3
            } else if byte & 0xF8 == 0xF0 {
                length = 4
            } else {
                return 0
            }
            let present = continuationCount + 1
            return present < length ? present : 0
        }
        return 0
    }
}
