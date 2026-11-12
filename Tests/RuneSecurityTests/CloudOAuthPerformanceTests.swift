import Foundation
import XCTest
@testable import RuneSecurity

final class CloudOAuthPerformanceTests: XCTestCase {
    func testAuthorizationRoundTripBenchmarkKPI() async throws {
        let configuration = try configuration()
        let iterations = 500
        var samples: [Double] = []
        for _ in 0..<7 {
            let coordinator = CloudOAuthAuthorizationCoordinator()
            let sheetID = CloudOAuthSheetID()
            var completed = 0
            let started = ContinuousClock.now
            for _ in 0..<iterations {
                let attempt = try await coordinator.begin(sheetID: sheetID, configuration: configuration)
                let query = URLComponents(url: attempt.authorizationURL, resolvingAgainstBaseURL: false)?.queryItems
                let state = try XCTUnwrap(query?.first(where: { $0.name == "state" })?.value)
                var callback = URLComponents(url: configuration.redirectURI, resolvingAgainstBaseURL: false)!
                callback.queryItems = [
                    URLQueryItem(name: "state", value: state),
                    URLQueryItem(name: "code", value: "synthetic-code")
                ]
                let exchange = try await coordinator.consumeCallback(
                    try XCTUnwrap(callback.url), sheetID: sheetID,
                    attemptID: attempt.id, generation: attempt.generation
                )
                if exchange.authorizationCode == "synthetic-code" { completed += 1 }
            }
            samples.append(seconds(started.duration(to: .now)))
            XCTAssertEqual(completed, iterations)
            let active = await coordinator.hasActiveAttempt(sheetID: sheetID)
            XCTAssertFalse(active)
        }
        try reportAndAssert(name: "oauth_round_trip", samples: samples, iterations: iterations)
    }

    func testSheetChurnBenchmarkKPI() async throws {
        let configuration = try configuration()
        let iterations = 500
        var samples: [Double] = []
        for _ in 0..<7 {
            let coordinator = CloudOAuthAuthorizationCoordinator()
            let sheetIDs = (0..<iterations).map { _ in CloudOAuthSheetID() }
            let started = ContinuousClock.now
            for sheetID in sheetIDs {
                _ = try await coordinator.begin(sheetID: sheetID, configuration: configuration)
                await coordinator.cancel(sheetID: sheetID)
            }
            samples.append(seconds(started.duration(to: .now)))
            for sheetID in sheetIDs {
                let active = await coordinator.hasActiveAttempt(sheetID: sheetID)
                XCTAssertFalse(active)
            }
        }
        try reportAndAssert(name: "oauth_sheet_churn", samples: samples, iterations: iterations)
    }

    private func reportAndAssert(name: String, samples: [Double], iterations: Int) throws {
        #if DEBUG
        let configuration = "debug"
        let maximumP95Seconds = 1.0
        #else
        let configuration = "release"
        let maximumP95Seconds = 0.5
        #endif
        let data = try JSONSerialization.data(withJSONObject: [
            "name": name, "samples_seconds": samples,
            "iterations": iterations, "configuration": configuration
        ], options: [.sortedKeys])
        print("RUNE_BENCHMARK \(String(decoding: data, as: UTF8.self))")
        let p95 = samples.sorted()[Int(ceil(Double(samples.count) * 0.95)) - 1]
        XCTAssertLessThan(p95, maximumP95Seconds,
                          "KPI: 500 local OAuth operations must keep p95 below \(maximumP95Seconds)s; no browser or network work is included.")
    }

    private func seconds(_ duration: Duration) -> Double {
        Double(duration.components.seconds) + Double(duration.components.attoseconds) / 1e18
    }

    private func configuration() throws -> CloudOAuthAuthorizationConfiguration {
        try CloudOAuthAuthorizationConfiguration(
            provider: .azure,
            authorizationEndpoint: URL(string: "https://identity.example.invalid/authorize")!,
            clientID: "synthetic-public-client",
            redirectURI: URL(string: "rune-synthetic://oauth/callback")!,
            scopes: ["openid", "cluster.read"],
            allowedAuthorizationHosts: ["identity.example.invalid"],
            allowedRedirectSchemes: ["rune-synthetic"],
            expectedIssuer: "https://identity.example.invalid/tenant",
            expectedAudience: "synthetic-public-client"
        )
    }
}
