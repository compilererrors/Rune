import Foundation
import RuneCore

/// Shared helpers for JSON **List** responses from the cluster: tolerate missing `items`, skip malformed elements.
enum KubectlListJSON {
    struct CollectionPageInfo: Sendable, Equatable {
        let itemsCount: Int
        let remainingItemCount: Int?
        let continueToken: String?
    }

    static func utf8DataTrimmed(_ raw: String) -> Data {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        return Data(trimmed.utf8)
    }

    /// Decodes each `items[]` entry with `T` independently; skips entries that fail `Decodable`.
    static func decodeLenientListItems<T: Decodable>(
        _ type: T.Type,
        from raw: String,
        decoder: JSONDecoder = JSONDecoder(),
        invalidJSONMessage: String,
        invalidStructureMessage: String
    ) throws -> [T] {
        let data = utf8DataTrimmed(raw)
        guard !data.isEmpty else { return [] }

        let rootObject: Any
        do {
            rootObject = try JSONSerialization.jsonObject(with: data)
        } catch {
            throw RuneError.parseError(message: invalidJSONMessage)
        }
        guard let root = rootObject as? [String: Any] else {
            throw RuneError.parseError(message: invalidStructureMessage)
        }
        let rawItems = root["items"] as? [Any] ?? []
        var decoded: [T] = []
        decoded.reserveCapacity(rawItems.count)
        for item in rawItems {
            guard let dict = item as? [String: Any],
                  let itemData = try? JSONSerialization.data(withJSONObject: dict),
                  let value = try? decoder.decode(T.self, from: itemData)
            else {
                continue
            }
            decoded.append(value)
        }
        return decoded
    }

    /// Total collection size from a single List response (e.g. raw GET with `limit=1` on a collection path).
    /// Uses `metadata.remainingItemCount` + `items.count` when present; returns `nil` when pagination needs legacy counting.
    static func collectionListTotal(from raw: String) -> Int? {
        guard let page = collectionPageInfo(from: raw) else { return nil }
        if let r = page.remainingItemCount {
            return page.itemsCount + r
        }
        if page.continueToken != nil {
            return nil
        }
        return page.itemsCount
    }

    /// List-page metadata for paged counting (`items.count`, `metadata.continue`, optional `metadata.remainingItemCount`).
    static func collectionPageInfo(from raw: String) -> CollectionPageInfo? {
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty,
              let data = trimmed.data(using: .utf8),
              let root = try? JSONSerialization.jsonObject(with: data) as? [String: Any]
        else {
            return nil
        }
        let items = root["items"] as? [Any] ?? []
        let meta = root["metadata"] as? [String: Any]
        let remaining: Int? = {
            guard let meta else { return nil }
            if let n = meta["remainingItemCount"] as? Int { return n }
            if let n = meta["remainingItemCount"] as? Int64 { return Int(n) }
            if let n = meta["remainingItemCount"] as? NSNumber { return n.intValue }
            return nil
        }()
        let continueToken = (meta?["continue"] as? String)?
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let normalizedContinue = (continueToken?.isEmpty == false) ? continueToken : nil
        return CollectionPageInfo(
            itemsCount: items.count,
            remainingItemCount: remaining,
            continueToken: normalizedContinue
        )
    }
}
