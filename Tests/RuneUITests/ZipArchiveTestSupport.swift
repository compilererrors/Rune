import Compression
import Foundation
import XCTest

enum ZipArchiveTestSupport {
    static func entries(from data: Data, file: StaticString = #filePath, line: UInt = #line) throws -> [String: Data] {
        var offset = 0
        var entries: [String: Data] = [:]

        while offset + 4 <= data.count {
            let signature = try data.uint32LE(at: offset)
            if signature == 0x02014b50 || signature == 0x06054b50 {
                break
            }
            guard signature == 0x04034b50 else {
                XCTFail("Unexpected ZIP local header signature at offset \(offset)", file: file, line: line)
                break
            }

            let method = try data.uint16LE(at: offset + 8)
            XCTAssertTrue(method == 0 || method == 8, "Rune archives store or deflate their entries", file: file, line: line)
            let compressedSize = Int(try data.uint32LE(at: offset + 18))
            let uncompressedSize = Int(try data.uint32LE(at: offset + 22))
            if method == 0 {
                XCTAssertEqual(compressedSize, uncompressedSize, file: file, line: line)
            }
            let nameLength = Int(try data.uint16LE(at: offset + 26))
            let extraLength = Int(try data.uint16LE(at: offset + 28))
            let nameStart = offset + 30
            let nameEnd = nameStart + nameLength
            let dataStart = nameEnd + extraLength
            let dataEnd = dataStart + compressedSize

            guard nameEnd <= data.count, dataEnd <= data.count else {
                throw NSError(domain: "ZipArchiveTestSupport", code: 1, userInfo: [NSLocalizedDescriptionKey: "ZIP entry extends beyond archive data"])
            }
            let name = String(decoding: data[nameStart..<nameEnd], as: UTF8.self)
            let body = data[dataStart..<dataEnd]
            entries[name] = method == 8
                ? try inflate(Data(body), uncompressedSize: uncompressedSize)
                : body
            offset = dataEnd
        }

        XCTAssertFalse(entries.isEmpty, "Expected at least one ZIP entry", file: file, line: line)
        return entries
    }

    private static func inflate(_ deflated: Data, uncompressedSize: Int) throws -> Data {
        guard uncompressedSize > 0 else { return Data() }
        var output = Data(count: uncompressedSize)
        let written = output.withUnsafeMutableBytes { destination -> Int in
            deflated.withUnsafeBytes { source -> Int in
                guard let destinationBase = destination.bindMemory(to: UInt8.self).baseAddress,
                      let sourceBase = source.bindMemory(to: UInt8.self).baseAddress else { return 0 }
                return compression_decode_buffer(
                    destinationBase, uncompressedSize, sourceBase, deflated.count, nil, COMPRESSION_ZLIB
                )
            }
        }
        guard written == uncompressedSize else {
            throw NSError(domain: "ZipArchiveTestSupport", code: 4, userInfo: [NSLocalizedDescriptionKey: "ZIP entry did not inflate to its recorded size"])
        }
        return output
    }
}

private extension Data {
    func uint16LE(at offset: Int) throws -> UInt16 {
        guard offset + 2 <= count else {
            throw NSError(domain: "ZipArchiveTestSupport", code: 2, userInfo: [NSLocalizedDescriptionKey: "Missing UInt16 at offset \(offset)"])
        }
        return UInt16(self[offset]) | (UInt16(self[offset + 1]) << 8)
    }

    func uint32LE(at offset: Int) throws -> UInt32 {
        guard offset + 4 <= count else {
            throw NSError(domain: "ZipArchiveTestSupport", code: 3, userInfo: [NSLocalizedDescriptionKey: "Missing UInt32 at offset \(offset)"])
        }
        return UInt32(self[offset])
            | (UInt32(self[offset + 1]) << 8)
            | (UInt32(self[offset + 2]) << 16)
            | (UInt32(self[offset + 3]) << 24)
    }
}
