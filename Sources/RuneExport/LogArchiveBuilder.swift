import Foundation

public struct PodLogArchiveRecord: Sendable {
    public let podName: String
    public let containerName: String?
    public let logs: String

    public init(podName: String, containerName: String?, logs: String) {
        self.podName = podName
        self.containerName = containerName
        self.logs = logs
    }
}

public enum LogArchiveBuilder {
    public static func buildPodContainerZip(
        records: [PodLogArchiveRecord],
        baseName: String,
        generatedAt: String
    ) throws -> Data {
        var entries: [ZipArchiveEntry] = []
        entries.reserveCapacity(records.count + 1)
        var mergedText = String()
        mergedText.reserveCapacity(records.reduce(0) { partial, record in
            partial + record.logs.utf8.count + (record.podName.utf8.count + (record.containerName?.utf8.count ?? 0) + 8) * 8
        })
        var wroteMergedLine = false

        for record in records.sorted(by: podContainerOrder) {
            let label = record.containerName.map { "\(record.podName)/\($0)" } ?? record.podName
            if record.logs.isEmpty {
                appendMergedLogLine(label: label, line: nil, to: &mergedText, wroteLine: &wroteMergedLine)
            } else {
                for line in record.logs.split(separator: "\n", omittingEmptySubsequences: false) {
                    appendMergedLogLine(label: label, line: line, to: &mergedText, wroteLine: &wroteMergedLine)
                }
            }

            let fileName = record.containerName.map { "\($0)-\(generatedAt).log" } ?? "\(record.podName)-\(generatedAt).log"
            entries.append(
                ZipArchiveEntry(
                    path: "\(baseName)/pods/\(record.podName)/\(fileName)",
                    data: Data(record.logs.utf8)
                )
            )
        }

        entries.insert(
            ZipArchiveEntry(
                path: "\(baseName)/merged-\(generatedAt).log",
                data: Data(mergedText.utf8)
            ),
            at: 0
        )

        return try ZipArchiveBuilder.build(entries: entries)
    }

    public static func buildZip(
        mergedText: String,
        podNames: [String],
        baseName: String,
        generatedAt: String
    ) throws -> Data {
        var entries = [
            ZipArchiveEntry(
                path: "\(baseName)/merged-\(generatedAt).log",
                data: Data(mergedText.utf8)
            )
        ]

        var byPod = splitMergedLogsByPod(mergedText: mergedText, podNames: podNames)
        if podNames.count == 1, byPod[podNames[0], default: ""].isEmpty {
            byPod[podNames[0]] = mergedText
        }
        for podName in podNames.sorted() {
            let text = byPod[podName] ?? ""
            entries.append(
                ZipArchiveEntry(
                    path: "\(baseName)/pods/\(podName)-\(generatedAt).log",
                    data: Data(text.utf8)
                )
            )
        }

        return try ZipArchiveBuilder.build(entries: entries)
    }

    public static func splitMergedLogsByPod(mergedText: String, podNames: [String]) -> [String: String] {
        let knownPods = Set(podNames)
        var output: [String: [String]] = [:]
        for podName in podNames {
            output[podName] = []
        }

        for line in mergedText.split(separator: "\n", omittingEmptySubsequences: false).map(String.init) {
            guard line.first == "[",
                  let close = line.firstIndex(of: "]")
            else { continue }
            let podName = String(line[line.index(after: line.startIndex)..<close])
            guard knownPods.contains(podName) else { continue }
            let textStart = line.index(after: close)
            let text = String(line[textStart...]).trimmingCharacters(in: .whitespaces)
            output[podName, default: []].append(text)
        }

        return output.mapValues { lines in
            lines.joined(separator: "\n")
        }
    }

    private static func podContainerOrder(_ lhs: PodLogArchiveRecord, _ rhs: PodLogArchiveRecord) -> Bool {
        if lhs.podName != rhs.podName {
            return lhs.podName.localizedCaseInsensitiveCompare(rhs.podName) == .orderedAscending
        }
        return (lhs.containerName ?? "").localizedCaseInsensitiveCompare(rhs.containerName ?? "") == .orderedAscending
    }

    private static func appendMergedLogLine(
        label: String,
        line: Substring?,
        to output: inout String,
        wroteLine: inout Bool
    ) {
        if wroteLine {
            output.append("\n")
        }
        output.append("[")
        output.append(label)
        output.append("]")
        if let line {
            output.append(" ")
            output.append(contentsOf: line)
        }
        wroteLine = true
    }
}
