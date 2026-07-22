public enum KubernetesAPIPathSanitizer {
    private static let safeQueryParameterNames: Set<String> = [
        "allowWatchBookmarks",
        "command",
        "container",
        "continue",
        "dryRun",
        "fieldManager",
        "fieldSelector",
        "fieldValidation",
        "follow",
        "force",
        "gracePeriodSeconds",
        "insecureSkipTLSVerifyBackend",
        "labelSelector",
        "limit",
        "limitBytes",
        "orphanDependents",
        "path",
        "ports",
        "pretty",
        "previous",
        "propagationPolicy",
        "resourceVersion",
        "resourceVersionMatch",
        "sendInitialEvents",
        "sinceSeconds",
        "sinceTime",
        "stderr",
        "stdin",
        "stdout",
        "tailLines",
        "timeoutSeconds",
        "timestamps",
        "tty",
        "watch"
    ]

    public static func sanitizedPath(_ apiPath: String) -> String {
        let pieces = apiPath.split(separator: "?", maxSplits: 1, omittingEmptySubsequences: false)
        let redactedPath = redactedPathSegments(String(pieces.first ?? ""))
        guard pieces.count > 1 else { return redactedPath }

        let query = pieces[1]
            .split(separator: "&", omittingEmptySubsequences: false)
            .map { item -> String in
                let pair = item.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
                guard pair.count == 2,
                      let name = pair.first,
                      safeQueryParameterNames.contains(String(name)) else {
                    return "<redacted>"
                }
                return "\(name)=<redacted>"
            }
            .joined(separator: "&")
        return query.isEmpty ? redactedPath : "\(redactedPath)?\(query)"
    }

    private static func redactedPathSegments(_ path: String) -> String {
        let hasLeadingSlash = path.hasPrefix("/")
        var segments = path.split(separator: "/", omittingEmptySubsequences: true).map(String.init)

        if let namespaceIndex = segments.firstIndex(of: "namespaces"),
           namespaceIndex + 1 < segments.count {
            segments[namespaceIndex + 1] = "<namespace>"
        }

        if let nameIndex = objectNameIndex(in: segments) {
            segments[nameIndex] = "<name>"
            redactProxyTail(in: &segments, after: nameIndex)
        }

        let joined = segments.joined(separator: "/")
        return hasLeadingSlash ? "/" + joined : joined
    }

    private static func objectNameIndex(in segments: [String]) -> Int? {
        if let namespaceIndex = segments.firstIndex(of: "namespaces") {
            let resourceIndex = namespaceIndex + 2
            let nameIndex = resourceIndex + 1
            return nameIndex < segments.count ? nameIndex : nil
        }

        if segments.first == "api" {
            let resourceIndex = segments.indices.contains(2) && segments[2] == "watch" ? 3 : 2
            let nameIndex = resourceIndex + 1
            return nameIndex < segments.count ? nameIndex : nil
        }

        if segments.first == "apis" {
            let resourceIndex = segments.indices.contains(3) && segments[3] == "watch" ? 4 : 3
            let nameIndex = resourceIndex + 1
            return nameIndex < segments.count ? nameIndex : nil
        }

        return nil
    }

    private static func redactProxyTail(in segments: inout [String], after nameIndex: Int) {
        let proxyIndex = nameIndex + 1
        let tailIndex = proxyIndex + 1
        guard segments.indices.contains(proxyIndex),
              segments[proxyIndex] == "proxy",
              segments.indices.contains(tailIndex) else {
            return
        }
        segments.replaceSubrange(tailIndex..<segments.endIndex, with: ["<path>"])
    }
}
