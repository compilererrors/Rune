import Foundation

public struct ZipArchiveEntry: Sendable {
    public let path: String
    public let data: Data

    public init(path: String, data: Data) {
        self.path = path
        self.data = data
    }
}

public enum ZipArchiveBuilder {
    public static func build(entries rawEntries: [ZipArchiveEntry]) throws -> Data {
        let entries = rawEntries.filter { !$0.path.isEmpty }
        var output = Data()
        var centralDirectory = Data()

        for entry in entries {
            let nameData = Data(sanitizedPath(entry.path).utf8)
            let crc = CRC32.checksum(entry.data)
            let offset = UInt32(output.count)
            let size = UInt32(entry.data.count)

            output.appendUInt32LE(0x04034b50)
            output.appendUInt16LE(20)
            output.appendUInt16LE(0)
            output.appendUInt16LE(0)
            output.appendUInt16LE(0)
            output.appendUInt16LE(0)
            output.appendUInt32LE(crc)
            output.appendUInt32LE(size)
            output.appendUInt32LE(size)
            output.appendUInt16LE(UInt16(nameData.count))
            output.appendUInt16LE(0)
            output.append(nameData)
            output.append(entry.data)

            centralDirectory.appendUInt32LE(0x02014b50)
            centralDirectory.appendUInt16LE(20)
            centralDirectory.appendUInt16LE(20)
            centralDirectory.appendUInt16LE(0)
            centralDirectory.appendUInt16LE(0)
            centralDirectory.appendUInt16LE(0)
            centralDirectory.appendUInt16LE(0)
            centralDirectory.appendUInt32LE(crc)
            centralDirectory.appendUInt32LE(size)
            centralDirectory.appendUInt32LE(size)
            centralDirectory.appendUInt16LE(UInt16(nameData.count))
            centralDirectory.appendUInt16LE(0)
            centralDirectory.appendUInt16LE(0)
            centralDirectory.appendUInt16LE(0)
            centralDirectory.appendUInt16LE(0)
            centralDirectory.appendUInt32LE(0)
            centralDirectory.appendUInt32LE(offset)
            centralDirectory.append(nameData)
        }

        let centralDirectoryOffset = UInt32(output.count)
        output.append(centralDirectory)
        output.appendUInt32LE(0x06054b50)
        output.appendUInt16LE(0)
        output.appendUInt16LE(0)
        output.appendUInt16LE(UInt16(entries.count))
        output.appendUInt16LE(UInt16(entries.count))
        output.appendUInt32LE(UInt32(centralDirectory.count))
        output.appendUInt32LE(centralDirectoryOffset)
        output.appendUInt16LE(0)
        return output
    }

    private static func sanitizedPath(_ path: String) -> String {
        let allowed = CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_ .")
        let components = path.components(separatedBy: "/").filter { !$0.isEmpty }
        let sanitized = components.map { component in
            String(component.unicodeScalars.map { scalar in
                allowed.contains(scalar) ? Character(scalar) : "-"
            })
        }
        return sanitized.joined(separator: "/")
    }
}

private enum CRC32 {
    private static let table: [UInt32] = (0..<256).map { value in
        var crc = UInt32(value)
        for _ in 0..<8 {
            crc = (crc & 1) == 1 ? 0xedb88320 ^ (crc >> 1) : crc >> 1
        }
        return crc
    }

    static func checksum(_ data: Data) -> UInt32 {
        var crc: UInt32 = 0xffffffff
        for byte in data {
            crc = table[Int((crc ^ UInt32(byte)) & 0xff)] ^ (crc >> 8)
        }
        return crc ^ 0xffffffff
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
