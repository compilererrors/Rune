import Foundation
import RuneCore

public struct HelmOutputParser {
    public init() {}

    public func parseReleases(from raw: String) throws -> [HelmReleaseSummary] {
        let data = Data(raw.utf8)
        let decoder = JSONDecoder()
        let payload = try decoder.decode([ReleasePayload].self, from: data)

        return payload.map { release in
            HelmReleaseSummary(
                name: release.name,
                namespace: release.namespace,
                revision: Int(release.revision) ?? 0,
                updated: release.updated,
                status: release.status,
                chart: release.chart,
                appVersion: release.appVersion
            )
        }
    }

    public func parseHistory(from raw: String) throws -> [HelmReleaseRevision] {
        let data = Data(raw.utf8)
        let decoder = JSONDecoder()
        let payload = try decoder.decode([HistoryPayload].self, from: data)

        return payload.map { entry in
            HelmReleaseRevision(
                revision: entry.revision,
                updated: entry.updated,
                status: entry.status,
                chart: entry.chart,
                appVersion: entry.appVersion,
                description: entry.description
            )
        }
    }
}

private struct ReleasePayload: Decodable {
    let name: String
    let namespace: String
    let revision: String
    let updated: String
    let status: String
    let chart: String
    let appVersion: String

    private enum CodingKeys: String, CodingKey {
        case name
        case namespace
        case revision
        case updated
        case status
        case chart
        case appVersion = "app_version"
    }
}

private struct HistoryPayload: Decodable {
    let revision: Int
    let updated: String
    let status: String
    let chart: String
    let appVersion: String
    let description: String

    private enum CodingKeys: String, CodingKey {
        case revision
        case updated
        case status
        case chart
        case appVersion = "app_version"
        case description
    }
}
