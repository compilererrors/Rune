import Foundation

struct AuthDoctorKubeconfigProjection: Sendable {
    struct Context: Sendable {
        var name = ""
        var cluster = ""
        var user = ""
    }

    struct Cluster: Sendable {
        var name = ""
        var server = ""
        var hasProxyURL = false
        var hasCustomCA = false
    }

    struct User: Sendable {
        struct Exec: Sendable {
            var command = ""
            var apiVersion: String?
            var interactiveMode: String?
            var installHint: String?
            var hasEKSRoleArgument = false
            var sourceDirectory = ""
        }

        var name = ""
        var exec: Exec?
        var authProviderName: String?
        var authExpiry: String?
    }

    var currentContext: String?
    var contexts: [Context] = []
    var clusters: [Cluster] = []
    var users: [User] = []

    static func parse(_ source: String, sourceURL: URL) -> Self {
        Parser(source: source, sourceURL: sourceURL).parse()
    }
}

private extension AuthDoctorKubeconfigProjection {
    struct Parser {
        private enum Section {
            case none
            case clusters
            case contexts
            case users
        }

        private struct MutableContext {
            var value = Context()
            var entryIndent = 0
            var bodyIndent: Int?
        }

        private struct MutableCluster {
            var value = Cluster()
            var entryIndent = 0
            var bodyIndent: Int?
        }

        private struct MutableUser {
            var value = User()
            var entryIndent = 0
            var execIndent: Int?
            var execFieldIndent: Int?
            var argsIndent: Int?
            var authProviderIndent: Int?
            var authProviderFieldIndent: Int?
            var authConfigIndent: Int?
        }

        let source: String
        let sourceURL: URL

        func parse() -> AuthDoctorKubeconfigProjection {
            var projection = AuthDoctorKubeconfigProjection()
            var section = Section.none
            var cluster: MutableCluster?
            var context: MutableContext?
            var user: MutableUser?
            let lines = Self.normalizedLines(source)
            var index = 0

            func flushCluster() {
                if let value = cluster?.value, !value.name.isEmpty {
                    projection.clusters.append(value)
                }
                cluster = nil
            }

            func flushContext() {
                if let value = context?.value, !value.name.isEmpty {
                    projection.contexts.append(value)
                }
                context = nil
            }

            func flushUser() {
                if var value = user?.value, !value.name.isEmpty {
                    if value.exec?.command.isEmpty == true {
                        value.exec = nil
                    }
                    projection.users.append(value)
                }
                user = nil
            }

            func flushAll() {
                flushCluster()
                flushContext()
                flushUser()
            }

            while index < lines.count {
                let originalLine = lines[index]
                let line = Self.stripInlineComment(originalLine)
                let trimmed = line.trimmingCharacters(in: CharacterSet.whitespaces)
                guard !trimmed.isEmpty else {
                    index += 1
                    continue
                }
                let indent = Self.indentation(of: line)

                if indent == 0, !trimmed.hasPrefix("-") {
                    if let value = Self.scalarValue(trimmed, key: "current-context") {
                        projection.currentContext = value.isEmpty ? nil : value
                        index += 1
                        continue
                    }

                    let nextSection: Section?
                    switch trimmed {
                    case "clusters:", "clusters: []": nextSection = .clusters
                    case "contexts:", "contexts: []": nextSection = .contexts
                    case "users:", "users: []": nextSection = .users
                    default: nextSection = nil
                    }
                    if let nextSection {
                        flushAll()
                        section = nextSection
                        index += 1
                        continue
                    }
                }

                switch section {
                case .clusters:
                    if trimmed.hasPrefix("- "), indent <= 2 {
                        flushCluster()
                        cluster = MutableCluster(entryIndent: indent)
                        Self.applyClusterLine(String(trimmed.dropFirst(2)), indent: indent, cluster: &cluster)
                    } else {
                        Self.applyClusterLine(trimmed, indent: indent, cluster: &cluster)
                    }

                case .contexts:
                    if trimmed.hasPrefix("- "), indent <= 2 {
                        flushContext()
                        context = MutableContext(entryIndent: indent)
                        Self.applyContextLine(String(trimmed.dropFirst(2)), indent: indent, context: &context)
                    } else {
                        Self.applyContextLine(trimmed, indent: indent, context: &context)
                    }

                case .users:
                    if trimmed.hasPrefix("- "), indent <= 2 {
                        flushUser()
                        user = MutableUser(entryIndent: indent)
                        Self.applyUserLine(
                            String(trimmed.dropFirst(2)),
                            indent: indent,
                            lines: lines,
                            index: &index,
                            sourceDirectory: sourceURL.deletingLastPathComponent().path,
                            user: &user
                        )
                    } else {
                        Self.applyUserLine(
                            trimmed,
                            indent: indent,
                            lines: lines,
                            index: &index,
                            sourceDirectory: sourceURL.deletingLastPathComponent().path,
                            user: &user
                        )
                    }

                case .none:
                    break
                }

                index += 1
            }

            flushAll()
            return projection
        }

        private static func applyClusterLine(
            _ line: String,
            indent: Int,
            cluster: inout MutableCluster?
        ) {
            if cluster == nil { cluster = MutableCluster(entryIndent: indent) }
            guard var mutable = cluster else { return }

            if let value = scalarValue(line, key: "name"), indent <= mutable.entryIndent + 2 {
                mutable.value.name = value
            } else if line == "cluster:" {
                mutable.bodyIndent = indent
            } else if let bodyIndent = mutable.bodyIndent, indent > bodyIndent {
                if let value = scalarValue(line, key: "server") {
                    mutable.value.server = value
                } else if scalarValue(line, key: "proxy-url") != nil {
                    mutable.value.hasProxyURL = true
                } else if scalarValue(line, key: "certificate-authority") != nil
                    || scalarValue(line, key: "certificate-authority-data") != nil {
                    mutable.value.hasCustomCA = true
                }
            }
            cluster = mutable
        }

        private static func applyContextLine(
            _ line: String,
            indent: Int,
            context: inout MutableContext?
        ) {
            if context == nil { context = MutableContext(entryIndent: indent) }
            guard var mutable = context else { return }

            if let value = scalarValue(line, key: "name"), indent <= mutable.entryIndent + 2 {
                mutable.value.name = value
            } else if line == "context:" {
                mutable.bodyIndent = indent
            } else if let bodyIndent = mutable.bodyIndent, indent > bodyIndent {
                if let value = scalarValue(line, key: "cluster") {
                    mutable.value.cluster = value
                } else if let value = scalarValue(line, key: "user") {
                    mutable.value.user = value
                }
            }
            context = mutable
        }

        private static func applyUserLine(
            _ line: String,
            indent: Int,
            lines: [String],
            index: inout Int,
            sourceDirectory: String,
            user: inout MutableUser?
        ) {
            if user == nil { user = MutableUser(entryIndent: indent) }
            guard var mutable = user else { return }

            if let execIndent = mutable.execIndent, indent <= execIndent, line != "exec:" {
                mutable.execIndent = nil
                mutable.execFieldIndent = nil
                mutable.argsIndent = nil
            }
            if let providerIndent = mutable.authProviderIndent,
               indent <= providerIndent,
               line != "auth-provider:" {
                mutable.authProviderIndent = nil
                mutable.authProviderFieldIndent = nil
                mutable.authConfigIndent = nil
            }

            if let value = scalarValue(line, key: "name"), indent <= mutable.entryIndent + 2 {
                mutable.value.name = value
                user = mutable
                return
            }

            if line == "exec:" {
                mutable.execIndent = indent
                mutable.execFieldIndent = nil
                mutable.argsIndent = nil
                mutable.value.exec = User.Exec(sourceDirectory: sourceDirectory)
                user = mutable
                return
            }

            if line == "auth-provider:" {
                mutable.authProviderIndent = indent
                mutable.authProviderFieldIndent = nil
                mutable.authConfigIndent = nil
                user = mutable
                return
            }

            if let execIndent = mutable.execIndent, indent > execIndent {
                if line == "args:" {
                    mutable.execFieldIndent = mutable.execFieldIndent ?? indent
                    if indent == mutable.execFieldIndent {
                        mutable.argsIndent = indent
                    }
                    user = mutable
                    return
                }

                // YAML permits an "indentless" sequence where argument list items line up with `args:`.
                if let argsIndent = mutable.argsIndent, indent >= argsIndent, line.hasPrefix("- ") {
                    let argument = parseScalar(String(line.dropFirst(2)))
                    if argument == "--role-arn" || argument.hasPrefix("--role-arn=") {
                        mutable.value.exec?.hasEKSRoleArgument = true
                    }
                    user = mutable
                    return
                }

                let recognizedExecField = scalarValue(line, key: "command") != nil
                    || scalarValue(line, key: "apiVersion") != nil
                    || scalarValue(line, key: "interactiveMode") != nil
                    || scalarValue(line, key: "installHint") != nil
                    || line == "env:"
                    || line == "provideClusterInfo:"
                if recognizedExecField {
                    mutable.execFieldIndent = mutable.execFieldIndent ?? indent
                }

                if indent == mutable.execFieldIndent {
                    if let value = scalarValue(line, key: "command") {
                        mutable.value.exec?.command = value
                    } else if let value = scalarValue(line, key: "apiVersion") {
                        mutable.value.exec?.apiVersion = value
                    } else if let value = scalarValue(line, key: "interactiveMode") {
                        mutable.value.exec?.interactiveMode = value
                    } else if let value = scalarValue(line, key: "installHint") {
                        if isBlockScalarMarker(value) {
                            let block = blockScalar(lines: lines, after: index, parentIndent: indent)
                            mutable.value.exec?.installHint = block.value
                            index = block.lastConsumedIndex
                        } else if !value.isEmpty {
                            mutable.value.exec?.installHint = value
                        }
                    }
                }
                user = mutable
                return
            }

            if let providerIndent = mutable.authProviderIndent, indent > providerIndent {
                if line == "config:" {
                    mutable.authConfigIndent = indent
                    user = mutable
                    return
                }

                if mutable.authConfigIndent == nil {
                    mutable.authProviderFieldIndent = mutable.authProviderFieldIndent ?? indent
                    if indent == mutable.authProviderFieldIndent,
                       let value = scalarValue(line, key: "name") {
                        mutable.value.authProviderName = value
                    }
                } else if let configIndent = mutable.authConfigIndent, indent > configIndent {
                    if let value = scalarValue(line, key: "expiry")
                        ?? scalarValue(line, key: "expiration") {
                        mutable.value.authExpiry = value
                    }
                }
            }
            user = mutable
        }

        private static func blockScalar(
            lines: [String],
            after parentIndex: Int,
            parentIndent: Int
        ) -> (value: String?, lastConsumedIndex: Int) {
            var index = parentIndex + 1
            var collected: [(indent: Int, value: String)] = []
            while index < lines.count {
                let line = lines[index]
                let trimmed = line.trimmingCharacters(in: .whitespaces)
                let indent = indentation(of: line)
                if !trimmed.isEmpty, indent <= parentIndent { break }
                collected.append((indent, line))
                index += 1
            }

            let contentIndent = collected
                .filter { !$0.value.trimmingCharacters(in: .whitespaces).isEmpty }
                .map(\.indent)
                .min() ?? parentIndent + 1
            let value = collected.map { entry -> String in
                guard entry.value.count >= contentIndent else { return "" }
                return String(entry.value.dropFirst(contentIndent))
            }.joined(separator: "\n").trimmingCharacters(in: .whitespacesAndNewlines)
            return (value.isEmpty ? nil : value, max(parentIndex, index - 1))
        }

        private static func isBlockScalarMarker(_ value: String) -> Bool {
            value == "|" || value == "|-" || value == "|+"
                || value == ">" || value == ">-" || value == ">+"
        }

        private static func normalizedLines(_ source: String) -> [String] {
            var normalized = source
            if normalized.hasPrefix("\u{FEFF}") { normalized.removeFirst() }
            normalized = normalized.replacingOccurrences(of: "\r\n", with: "\n")
            normalized = normalized.replacingOccurrences(of: "\r", with: "\n")
            return normalized.split(omittingEmptySubsequences: false, whereSeparator: \.isNewline).map(String.init)
        }

        private static func indentation(of line: String) -> Int {
            line.prefix { $0 == " " }.count
        }

        private static func scalarValue(_ line: String, key: String) -> String? {
            let prefix = "\(key):"
            guard line.hasPrefix(prefix) else { return nil }
            return parseScalar(String(line.dropFirst(prefix.count)))
        }

        private static func parseScalar(_ raw: String) -> String {
            let trimmed = raw.trimmingCharacters(in: .whitespaces)
            if trimmed.count >= 2,
               let first = trimmed.first,
               let last = trimmed.last,
               (first == "\"" && last == "\"") || (first == "'" && last == "'") {
                return String(trimmed.dropFirst().dropLast())
            }
            return trimmed
        }

        private static func stripInlineComment(_ line: String) -> String {
            var inSingle = false
            var inDouble = false
            var escaped = false
            for index in line.indices {
                let character = line[index]
                if character == "\\", inDouble {
                    escaped.toggle()
                    continue
                }
                if character == "'", !inDouble { inSingle.toggle() }
                if character == "\"", !inSingle, !escaped { inDouble.toggle() }
                if character == "#", !inSingle, !inDouble,
                   (index == line.startIndex || line[line.index(before: index)].isWhitespace) {
                    return String(line[..<index])
                }
                escaped = false
            }
            return line
        }
    }
}
