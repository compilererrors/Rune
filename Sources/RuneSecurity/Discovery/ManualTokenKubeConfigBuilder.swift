import Foundation
import RuneCore

public struct ManualTokenKubeConfigRequest: Equatable, Sendable {
    public let name: String
    public let server: String
    public let namespace: String
    public let token: String

    public init(name: String, server: String, namespace: String, token: String) {
        self.name = name
        self.server = server
        self.namespace = namespace
        self.token = token
    }
}

public enum ManualTokenKubeConfigBuilder {
    public static func buildYAML(for request: ManualTokenKubeConfigRequest) throws -> String {
        let server = request.server.trimmingCharacters(in: .whitespacesAndNewlines)
        let token = request.token.trimmingCharacters(in: .whitespacesAndNewlines)
        let namespace = request.namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let components = URLComponents(string: server),
              let scheme = components.scheme?.lowercased(),
              ["http", "https"].contains(scheme),
              components.host?.isEmpty == false else {
            throw RuneError.invalidInput(message: "Manual cluster server must be a valid HTTP or HTTPS URL.")
        }
        guard !token.isEmpty else {
            throw RuneError.invalidInput(message: "Manual cluster token is required.")
        }

        let contextName = normalizedName(from: request.name, fallback: components.host ?? "manual-cluster")
        let clusterName = "\(contextName)-cluster"
        let userName = "\(contextName)-user"
        let namespaceLine = namespace.isEmpty ? "" : "\n    namespace: \(yamlQuoted(namespace))"

        return """
        apiVersion: v1
        kind: Config
        current-context: \(yamlQuoted(contextName))
        clusters:
        - name: \(yamlQuoted(clusterName))
          cluster:
            server: \(yamlQuoted(server))
        contexts:
        - name: \(yamlQuoted(contextName))
          context:
            cluster: \(yamlQuoted(clusterName))
            user: \(yamlQuoted(userName))\(namespaceLine)
        users:
        - name: \(yamlQuoted(userName))
          user:
            token: \(yamlQuoted(token))
        """
    }

    static func normalizedName(from raw: String, fallback: String) -> String {
        let candidate = raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? fallback : raw
        let normalized = candidate
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .map { character -> Character in
                if character.isLetter || character.isNumber || character == "-" || character == "_" || character == "." {
                    return character
                }
                return "-"
            }
            .reduce(into: "") { partial, character in
                if character == "-", partial.last == "-" { return }
                partial.append(character)
            }
            .trimmingCharacters(in: CharacterSet(charactersIn: "-._"))
        return normalized.isEmpty ? "manual-cluster" : normalized
    }

    private static func yamlQuoted(_ value: String) -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.withoutEscapingSlashes]
        guard let data = try? encoder.encode(value),
              let encoded = String(data: data, encoding: .utf8) else {
            return "\"\""
        }
        return encoded
    }
}
