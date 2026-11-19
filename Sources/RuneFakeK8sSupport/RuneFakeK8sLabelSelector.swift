import Foundation

/// A `labelSelector` query as the API server reads it: `key=value`,
/// `key==value`, `key!=value`, `key in (a,b)`, `key notin (a,b)`, `key` and
/// `!key`, joined by commas outside parentheses. A requirement it cannot
/// read matches nothing, as the API server rejects it.
struct RuneFakeK8sLabelSelector {
    private enum Requirement {
        case equals(String, String)
        case notEquals(String, String)
        case isIn(String, Set<String>)
        case notIn(String, Set<String>)
        case exists(String)
        case doesNotExist(String)
        case invalid
    }

    private let requirements: [Requirement]

    init(_ raw: String) {
        var parts: [String] = []
        var current = ""
        var depth = 0
        for character in raw {
            switch character {
            case "(": depth += 1; current.append(character)
            case ")": depth -= 1; current.append(character)
            case "," where depth == 0: parts.append(current); current = ""
            default: current.append(character)
            }
        }
        parts.append(current)
        requirements = parts
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { !$0.isEmpty }
            .map(Self.requirement)
    }

    func matches(_ labels: [String: String]) -> Bool {
        requirements.allSatisfy { requirement in
            switch requirement {
            case let .equals(key, value): labels[key] == value
            case let .notEquals(key, value): labels[key] != value
            case let .isIn(key, values): labels[key].map(values.contains) ?? false
            case let .notIn(key, values): labels[key].map { !values.contains($0) } ?? true
            case let .exists(key): labels[key] != nil
            case let .doesNotExist(key): labels[key] == nil
            case .invalid: false
            }
        }
    }

    private static func requirement(_ text: String) -> Requirement {
        if text.hasPrefix("!") {
            return .doesNotExist(String(text.dropFirst()).trimmingCharacters(in: .whitespaces))
        }
        if let range = text.range(of: "!=") {
            return .notEquals(trim(text[..<range.lowerBound]), trim(text[range.upperBound...]))
        }
        if let range = text.range(of: "==") ?? text.range(of: "=") {
            return .equals(trim(text[..<range.lowerBound]), trim(text[range.upperBound...]))
        }
        for (word, isIn) in [(" notin ", false), (" in ", true)] {
            if let range = text.range(of: word) {
                let key = trim(text[..<range.lowerBound])
                var list = trim(text[range.upperBound...])
                guard list.hasPrefix("("), list.hasSuffix(")") else { return .invalid }
                list = String(list.dropFirst().dropLast())
                let values = Set(list.split(separator: ",").map { trim($0) })
                return isIn ? .isIn(key, values) : .notIn(key, values)
            }
        }
        return text.contains(" ") ? .invalid : .exists(text)
    }

    private static func trim(_ text: some StringProtocol) -> String {
        text.trimmingCharacters(in: .whitespaces)
    }
}
