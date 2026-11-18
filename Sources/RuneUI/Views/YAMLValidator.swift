import Foundation
import RuneCore
import Yams

/// The resource a YAML draft is meant to update, so the validator can say
/// when the document names a different object.
struct YAMLValidationTarget: Equatable, Sendable {
    let kind: String
    let name: String
    let namespace: String?
}

/// Validates a YAML draft with a real YAML 1.2 parser (libyaml through Yams)
/// plus Kubernetes-aware checks, and reports every problem at an exact line,
/// column and UTF-16 range in the text that was validated.
///
/// Pure and thread-safe: the view model runs it off the main thread after a
/// short debounce and only publishes results for the text that is current.
enum YAMLValidator {
    static func validate(_ text: String, target: YAMLValidationTarget? = nil) -> [YAMLValidationIssue] {
        guard !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return [] }
        let index = YAMLValidationLineIndex(text)
        var issues: [YAMLValidationIssue] = []

        var roots: [Node] = []
        do {
            let parser = try Parser(yaml: text)
            while let root = try parser.nextRoot() {
                // Superseded by a newer edit: its result is not shown.
                if Task.isCancelled { return [] }
                roots.append(root)
            }
        } catch let error as YamlError {
            // A tab is an error only where the parser stops on it. Inside a
            // block scalar (a Makefile or script in a ConfigMap) it is
            // content, and the document parses.
            let syntax = syntaxIssue(for: error, index: index)
            if let line = syntax?.line, let tab = tabIndentationIssue(onLine: line, index: index) {
                issues.append(tab)
            } else if let syntax {
                issues.append(syntax)
            }
        } catch {
            issues.append(YAMLValidationIssue(source: .syntax, severity: .error, message: error.localizedDescription))
        }

        let documents = roots.filter { !$0.isEmptyDocument }
        for (documentIndex, root) in documents.enumerated() {
            if Task.isCancelled { return [] }
            issues += kubernetesIssues(
                in: root,
                target: documentIndex == 0 && documents.count == 1 ? target : nil,
                index: index
            )
        }

        return issues.sorted {
            ($0.line ?? .max, $0.column ?? .max, $0.message) < ($1.line ?? .max, $1.column ?? .max, $1.message)
        }
    }

    // MARK: Syntax

    private static func syntaxIssue(for error: YamlError, index: YAMLValidationLineIndex) -> YAMLValidationIssue? {
        switch error {
        case let .scanner(context, problem, mark, _),
             let .parser(context, problem, mark, _),
             let .composer(context, problem, mark, _):
            var message = sentence(problem)
            if let context, context.mark.line != mark.line {
                message += " (\(context.text) on line \(context.mark.line))"
            }
            return issue(.syntax, .error, message, line: mark.line, column: mark.column, index: index)
        case let .duplicatedKeysInMapping(duplicates, context):
            let names = duplicates.map { "“\($0)”" }.joined(separator: ", ")
            let message = duplicates.count == 1
                ? "Duplicate key \(names); each key may appear once in a mapping"
                : "Duplicate keys \(names); each key may appear once in a mapping"
            return issue(.syntax, .error, message, line: context.mark.line, column: context.mark.column, index: index)
        case let .reader(problem, offset, _, _):
            if let offset, let position = index.position(forUTF8Offset: offset) {
                return issue(.syntax, .error, sentence(problem), line: position.line, column: position.column, index: index)
            }
            return YAMLValidationIssue(source: .syntax, severity: .error, message: sentence(problem))
        default:
            return YAMLValidationIssue(source: .syntax, severity: .error, message: String(describing: error))
        }
    }

    /// libyaml rejects a tab in indentation with "found character that cannot
    /// start any token"; say what is wrong and where, at the tab itself. Nil
    /// when the line's indentation holds no tab.
    private static func tabIndentationIssue(onLine line: Int, index: YAMLValidationLineIndex) -> YAMLValidationIssue? {
        var column = 1
        for scalar in index.lineText(line).unicodeScalars {
            if scalar == "\t" {
                return issue(.syntax, .error, "Tabs are not allowed in YAML indentation.", line: line, column: column, index: index, length: 1)
            }
            if scalar != " " { return nil }
            column += 1
        }
        return nil
    }

    // MARK: Kubernetes

    private static func kubernetesIssues(in root: Node, target: YAMLValidationTarget?, index: YAMLValidationLineIndex) -> [YAMLValidationIssue] {
        guard let mapping = root.mapping else {
            return [issue(.kubernetes, .error, "A Kubernetes manifest must be a mapping with apiVersion, kind and metadata", at: root.mark, index: index)]
        }
        var issues: [YAMLValidationIssue] = []
        let start = root.mark

        func requireString(_ key: String, example: String) -> Node? {
            guard let node = mapping[key] else {
                issues.append(issue(.kubernetes, .error, "Missing \(key)", at: start, index: index))
                return nil
            }
            guard node.isPlainStringLike else {
                issues.append(issue(.kubernetes, .error, "\(key) must be a string, for example \(example)", at: node.mark, index: index))
                return nil
            }
            return node
        }

        _ = requireString("apiVersion", example: "v1 or apps/v1")
        let kindNode = requireString("kind", example: "Deployment")

        var nameNode: Node?
        var namespaceNode: Node?
        if let metadata = mapping["metadata"] {
            if let metadataMapping = metadata.mapping {
                nameNode = metadataMapping["name"]
                namespaceNode = metadataMapping["namespace"]
                if nameNode == nil, metadataMapping["generateName"] == nil {
                    issues.append(issue(.kubernetes, .error, "Missing metadata.name", at: metadata.mark, index: index))
                } else if let nameNode, !nameNode.isPlainStringLike {
                    issues.append(issue(.kubernetes, .error, "metadata.name must be a string", at: nameNode.mark, index: index))
                }
                for field in ["labels", "annotations"] {
                    guard let node = metadataMapping[field] else { continue }
                    guard let values = node.mapping else {
                        issues.append(issue(.kubernetes, .error, "metadata.\(field) must be a mapping of strings", at: node.mark, index: index))
                        continue
                    }
                    for (key, value) in values where !value.isPlainStringLike {
                        let keyText = key.string ?? "?"
                        // An empty value is accepted (and is how a patch
                        // removes a key), so it only gets a note.
                        if value.null != nil {
                            issues.append(issue(
                                .kubernetes, .warning,
                                "metadata.\(field).\(keyText) has no value; write \"\" for an empty string",
                                at: key.mark, index: index
                            ))
                        } else {
                            issues.append(issue(
                                .kubernetes, .error,
                                "metadata.\(field).\(keyText) must be a string; quote the value",
                                at: value.mark, index: index
                            ))
                        }
                    }
                }
            } else {
                issues.append(issue(.kubernetes, .error, "metadata must be a mapping", at: metadata.mark, index: index))
            }
        } else {
            issues.append(issue(.kubernetes, .error, "Missing metadata", at: start, index: index))
        }

        if let spec = mapping["spec"]?.mapping {
            issues += specIssues(spec, path: "spec", index: index)
        }

        if let target {
            if let kindNode, let kind = kindNode.string, kind != target.kind {
                issues.append(issue(.kubernetes, .error, "kind is \(kind), but this editor updates a \(target.kind)", at: kindNode.mark, index: index))
            }
            if let nameNode, let name = nameNode.string, name != target.name {
                issues.append(issue(.kubernetes, .error, "metadata.name is \(name), but this editor updates \(target.name)", at: nameNode.mark, index: index))
            }
            if let expected = target.namespace, let namespaceNode, let namespace = namespaceNode.string, namespace != expected {
                issues.append(issue(.kubernetes, .error, "metadata.namespace is \(namespace), but this resource is in \(expected)", at: namespaceNode.mark, index: index))
            }
        }
        return issues
    }

    /// Type checks on fields people edit most: replica counts, container
    /// lists and ports, through pod templates and job templates.
    private static func specIssues(_ spec: Node.Mapping, path: String, index: YAMLValidationLineIndex) -> [YAMLValidationIssue] {
        var issues: [YAMLValidationIssue] = []
        if let replicas = spec["replicas"], replicas.int == nil {
            issues.append(issue(.kubernetes, .error, "\(path).replicas must be a whole number", at: replicas.mark, index: index))
        }
        for field in ["containers", "initContainers"] {
            guard let node = spec[field] else { continue }
            guard let containers = node.sequence else {
                issues.append(issue(.kubernetes, .error, "\(path).\(field) must be a list", at: node.mark, index: index))
                continue
            }
            for (position, container) in containers.enumerated() {
                let itemPath = "\(path).\(field)[\(position)]"
                guard let containerMapping = container.mapping else {
                    issues.append(issue(.kubernetes, .error, "\(itemPath) must be a mapping with name and image", at: container.mark, index: index))
                    continue
                }
                if containerMapping["name"] == nil {
                    issues.append(issue(.kubernetes, .error, "\(itemPath) is missing name", at: container.mark, index: index))
                }
                if containerMapping["image"] == nil {
                    issues.append(issue(.kubernetes, .warning, "\(itemPath) has no image", at: container.mark, index: index))
                }
                if let ports = containerMapping["ports"] {
                    guard let portList = ports.sequence else {
                        issues.append(issue(.kubernetes, .error, "\(itemPath).ports must be a list", at: ports.mark, index: index))
                        continue
                    }
                    for (portPosition, port) in portList.enumerated() {
                        if let containerPort = port.mapping?["containerPort"], containerPort.int == nil {
                            issues.append(issue(.kubernetes, .error, "\(itemPath).ports[\(portPosition)].containerPort must be a number", at: containerPort.mark, index: index))
                        }
                    }
                }
            }
        }
        if let template = spec["template"]?.mapping?["spec"]?.mapping {
            issues += specIssues(template, path: "\(path).template.spec", index: index)
        }
        if let jobTemplate = spec["jobTemplate"]?.mapping?["spec"]?.mapping {
            issues += specIssues(jobTemplate, path: "\(path).jobTemplate.spec", index: index)
        }
        return issues
    }

    // MARK: Issue construction

    private static func issue(
        _ source: YAMLValidationIssue.Source,
        _ severity: YAMLValidationIssue.Severity,
        _ message: String,
        at mark: Mark?,
        index: YAMLValidationLineIndex
    ) -> YAMLValidationIssue {
        guard let mark else {
            return YAMLValidationIssue(source: source, severity: severity, message: message, line: 1, column: 1, range: index.tokenRange(line: 1, column: 1))
        }
        return issue(source, severity, message, line: mark.line, column: mark.column, index: index)
    }

    private static func issue(
        _ source: YAMLValidationIssue.Source,
        _ severity: YAMLValidationIssue.Severity,
        _ message: String,
        line: Int,
        column: Int,
        index: YAMLValidationLineIndex,
        length: Int? = nil
    ) -> YAMLValidationIssue {
        let line = min(max(1, line), max(1, index.lineCount))
        let column = max(1, column)
        let range = length.map { index.range(line: line, column: column, length: $0) } ?? index.tokenRange(line: line, column: column)
        return YAMLValidationIssue(source: source, severity: severity, message: message, line: line, column: column, range: range)
    }

    private static func sentence(_ text: String) -> String {
        guard let first = text.first else { return text }
        return first.uppercased() + text.dropFirst()
    }
}

/// Maps 1-based lines and Unicode-scalar columns (libyaml's units) to UTF-16
/// ranges (NSTextView's units). Handles LF, CRLF and CR line breaks.
struct YAMLValidationLineIndex {
    private let text: NSString
    private let lineStarts: [Int]

    init(_ source: String) {
        text = source as NSString
        var starts = [0]
        let length = text.length
        var position = 0
        while position < length {
            let character = text.character(at: position)
            if character == 0x0A {
                starts.append(position + 1)
            } else if character == 0x0D {
                if position + 1 < length, text.character(at: position + 1) == 0x0A {
                    position += 1
                }
                starts.append(position + 1)
            }
            position += 1
        }
        lineStarts = starts
    }

    var lineCount: Int { lineStarts.count }

    /// The line's content without its line break.
    func lineText(_ line: Int) -> String {
        guard line >= 1, line <= lineStarts.count else { return "" }
        let start = lineStarts[line - 1]
        var end = line < lineStarts.count ? lineStarts[line] : text.length
        while end > start, [0x0A, 0x0D].contains(text.character(at: end - 1)) {
            end -= 1
        }
        return text.substring(with: NSRange(location: start, length: end - start))
    }

    func utf16Offset(line: Int, column: Int) -> Int {
        let line = min(max(1, line), lineStarts.count)
        let content = lineText(line)
        var utf16 = 0
        var scalars = 0
        for scalar in content.unicodeScalars {
            guard scalars < column - 1 else { break }
            utf16 += scalar.utf16.count
            scalars += 1
        }
        return lineStarts[line - 1] + utf16
    }

    func range(line: Int, column: Int, length: Int) -> YAMLValidationRange {
        let start = utf16Offset(line: line, column: column)
        let lineEnd = lineStarts[min(max(1, line), lineStarts.count) - 1] + (lineText(line) as NSString).length
        return YAMLValidationRange(location: start, length: max(1, min(length, max(1, lineEnd - start))))
    }

    /// The token at a position: up to the next whitespace, ":" or line end,
    /// at least one character, so underlines cover what the message names.
    func tokenRange(line: Int, column: Int) -> YAMLValidationRange {
        let start = utf16Offset(line: line, column: column)
        let lineStart = lineStarts[min(max(1, line), lineStarts.count) - 1]
        let lineEnd = lineStart + (lineText(line) as NSString).length
        var end = start
        while end < lineEnd {
            let character = text.character(at: end)
            if character == 0x20 || character == 0x09 || (character == 0x3A && end > start) { break }
            end += 1
        }
        if end == start {
            // At a line end (an unterminated construct): mark the last character.
            return YAMLValidationRange(location: max(lineStart, min(start, lineEnd) - (lineEnd > lineStart && start >= lineEnd ? 1 : 0)), length: 1)
        }
        return YAMLValidationRange(location: start, length: end - start)
    }

    func position(forUTF8Offset offset: Int) -> (line: Int, column: Int)? {
        let prefix = String(decoding: Array((text as String).utf8.prefix(offset)), as: UTF8.self) as NSString
        let utf16Offset = prefix.length
        guard let lineIndex = lineStarts.lastIndex(where: { $0 <= utf16Offset }) else { return nil }
        let column = text.substring(with: NSRange(location: lineStarts[lineIndex], length: utf16Offset - lineStarts[lineIndex])).unicodeScalars.count + 1
        return (lineIndex + 1, column)
    }
}

private extension Node {
    /// True for `---` documents with nothing in them.
    var isEmptyDocument: Bool {
        if case let .scalar(scalar) = self {
            return scalar.string.isEmpty && scalar.style == .plain
        }
        return false
    }

    /// A scalar that Kubernetes reads as a string: quoted, or plain text that
    /// does not resolve to a number, boolean or null.
    var isPlainStringLike: Bool {
        guard case let .scalar(scalar) = self else { return false }
        if scalar.style != .plain { return true }
        // Plain scalars resolve like Kubernetes' YAML decoder does.
        return bool == nil && int == nil && float == nil && null == nil
    }
}
