import Compression
import Foundation
import zlib

public struct ZipArchiveEntry: Sendable {
    public let path: String
    public let data: Data

    public init(path: String, data: Data) {
        self.path = path
        self.data = data
    }
}

public enum ZipArchiveError: LocalizedError, Equatable, Sendable {
    case tooManyEntries(Int)
    case tooLarge

    public var errorDescription: String? {
        switch self {
        case let .tooManyEntries(count):
            return "A ZIP archive holds at most 65,535 files; this export has \(count)."
        case .tooLarge:
            return "The export is larger than the 4 GB a ZIP archive can hold."
        }
    }
}

public enum ZipArchiveBuilder {
    /// Entries are deflated when that makes them smaller and stored
    /// otherwise. `modificationDate` is the time each entry is unpacked with.
    public static func build(
        entries rawEntries: [ZipArchiveEntry],
        modificationDate: Date = Date()
    ) throws -> Data {
        let entries = rawEntries.filter { !$0.path.isEmpty }
        // Past 65,535 entries or 4 GB the format needs ZIP64, which Rune
        // does not write.
        guard entries.count <= Int(UInt16.max) else {
            throw ZipArchiveError.tooManyEntries(entries.count)
        }
        let timestamp = dosTimestamp(for: modificationDate)
        var output = Data()
        var centralDirectory = Data()

        for entry in entries {
            let nameData = Data(sanitizedPath(entry.path).utf8.prefix(Int(UInt16.max)))
            let stored = try fitting32Bits(entry.data.count)
            let crc = CRC32.checksum(entry.data)
            let deflated = deflate(entry.data)
            let body = deflated ?? entry.data
            let method: UInt16 = deflated == nil ? 0 : 8
            let offset = try fitting32Bits(output.count)
            let size = try fitting32Bits(body.count)

            output.appendUInt32LE(0x04034b50)
            output.appendUInt16LE(20)
            output.appendUInt16LE(0)
            output.appendUInt16LE(method)
            output.appendUInt16LE(timestamp.time)
            output.appendUInt16LE(timestamp.date)
            output.appendUInt32LE(crc)
            output.appendUInt32LE(size)
            output.appendUInt32LE(stored)
            output.appendUInt16LE(UInt16(nameData.count))
            output.appendUInt16LE(0)
            output.append(nameData)
            output.append(body)

            centralDirectory.appendUInt32LE(0x02014b50)
            centralDirectory.appendUInt16LE(20)
            centralDirectory.appendUInt16LE(20)
            centralDirectory.appendUInt16LE(0)
            centralDirectory.appendUInt16LE(method)
            centralDirectory.appendUInt16LE(timestamp.time)
            centralDirectory.appendUInt16LE(timestamp.date)
            centralDirectory.appendUInt32LE(crc)
            centralDirectory.appendUInt32LE(size)
            centralDirectory.appendUInt32LE(stored)
            centralDirectory.appendUInt16LE(UInt16(nameData.count))
            centralDirectory.appendUInt16LE(0)
            centralDirectory.appendUInt16LE(0)
            centralDirectory.appendUInt16LE(0)
            centralDirectory.appendUInt16LE(0)
            centralDirectory.appendUInt32LE(0)
            centralDirectory.appendUInt32LE(offset)
            centralDirectory.append(nameData)
        }

        let centralDirectoryOffset = try fitting32Bits(output.count)
        let centralDirectorySize = try fitting32Bits(centralDirectory.count)
        _ = try fitting32Bits(output.count + centralDirectory.count)
        output.append(centralDirectory)
        output.appendUInt32LE(0x06054b50)
        output.appendUInt16LE(0)
        output.appendUInt16LE(0)
        output.appendUInt16LE(UInt16(entries.count))
        output.appendUInt16LE(UInt16(entries.count))
        output.appendUInt32LE(centralDirectorySize)
        output.appendUInt32LE(centralDirectoryOffset)
        output.appendUInt16LE(0)
        return output
    }

    private static func fitting32Bits(_ value: Int) throws -> UInt32 {
        guard let fitted = UInt32(exactly: value) else { throw ZipArchiveError.tooLarge }
        return fitted
    }

    /// The raw deflate stream of `data`, or nil when storing it is no larger.
    private static func deflate(_ data: Data) -> Data? {
        let sourceCount = data.count
        guard sourceCount >= 64 else { return nil }
        var destination = Data(count: sourceCount)
        let written = destination.withUnsafeMutableBytes { destinationBuffer -> Int in
            data.withUnsafeBytes { sourceBuffer -> Int in
                guard let destinationBase = destinationBuffer.bindMemory(to: UInt8.self).baseAddress,
                      let sourceBase = sourceBuffer.bindMemory(to: UInt8.self).baseAddress else { return 0 }
                return compression_encode_buffer(
                    destinationBase, sourceCount, sourceBase, sourceCount, nil, COMPRESSION_ZLIB
                )
            }
        }
        guard written > 0, written < sourceCount else { return nil }
        destination.removeSubrange(written...)
        return destination
    }

    /// MS-DOS time and date in local time, as the format stores them.
    private static func dosTimestamp(for date: Date) -> (time: UInt16, date: UInt16) {
        let parts = Calendar(identifier: .gregorian).dateComponents(
            [.year, .month, .day, .hour, .minute, .second],
            from: date
        )
        let year = min(max((parts.year ?? 1980) - 1980, 0), 127)
        let dosDate = (year << 9) | ((parts.month ?? 1) << 5) | (parts.day ?? 1)
        let dosTime = ((parts.hour ?? 0) << 11) | ((parts.minute ?? 0) << 5) | ((parts.second ?? 0) / 2)
        return (UInt16(truncatingIfNeeded: dosTime), UInt16(truncatingIfNeeded: dosDate))
    }

    private static func sanitizedPath(_ path: String) -> String {
        let allowed = CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_ .")
        // Without "." and "..", an entry never points outside the folder
        // the archive is unpacked into.
        let components = path.components(separatedBy: "/").filter { !$0.isEmpty && $0 != "." && $0 != ".." }
        let sanitized = components.map { component in
            String(component.unicodeScalars.map { scalar in
                allowed.contains(scalar) ? Character(scalar) : "-"
            })
        }
        return sanitized.joined(separator: "/")
    }
}

private enum CRC32 {
    static func checksum(_ data: Data) -> UInt32 {
        data.withUnsafeBytes { buffer -> UInt32 in
            var crc = crc32(0, nil, 0)
            var remaining = buffer.count
            var cursor = buffer.bindMemory(to: Bytef.self).baseAddress
            // zlib takes the length as 32 bits.
            while remaining > 0, let base = cursor {
                let chunk = min(remaining, 1 << 30)
                crc = crc32(crc, base, uInt(chunk))
                cursor = base + chunk
                remaining -= chunk
            }
            return UInt32(truncatingIfNeeded: crc)
        }
    }
}

private extension Data {
    mutating func appendUInt16LE(_ value: UInt16) {
        append(UInt8(value & 0xff))
        append(UInt8((value >> 8) & 0xff))
    }

    mutating func appendUInt32LE(_ value: UInt32) {
        append(UInt8(value & 0xff))
        append(UInt8((value >> 8) & 0xff))
        append(UInt8((value >> 16) & 0xff))
        append(UInt8((value >> 24) & 0xff))
    }
}
