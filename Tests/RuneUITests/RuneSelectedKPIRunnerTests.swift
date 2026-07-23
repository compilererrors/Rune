import Foundation
import XCTest

final class RuneSelectedKPIRunnerTests: XCTestCase {
    func testTrackedRunnerIncludesReleaseRESTNativeAndResourceGates() throws {
        let scriptURL = repoRoot.appendingPathComponent("scripts/run-selected-kpis.sh")
        let source = try String(contentsOf: scriptURL, encoding: .utf8)

        XCTAssertTrue(FileManager.default.isExecutableFile(atPath: scriptURL.path))
        XCTAssertTrue(source.contains("RunePerformanceBenchmarksTests/testRESTRequestMetricsRecordingBenchmarkKPI"))
        XCTAssertTrue(source.contains("RunePerformanceBenchmarksTests/testRESTRequestMetricsRetentionChurnBenchmarkKPI"))
        XCTAssertTrue(source.contains("RunePerformanceBenchmarksTests/testNativeCloudImportDiagnosticProjectionBenchmarkKPI"))
        XCTAssertTrue(source.contains("RunePerformanceBenchmarksTests/testNativeCloudImportAdmissionGuardBenchmarkKPI"))
        XCTAssertTrue(source.contains("NativeCloudImportParityTests/testHeadlessAKSAndGKEImportReviewBindingReleaseKPI"))
        XCTAssertTrue(source.contains("RunePerformanceBenchmarksTests/testResourceListColumnLayoutBenchmarkKPI"))
        XCTAssertTrue(source.contains("AWSEKSNativeAuthTests/testKPISignsOneThousandTokensWithinFiveSeconds"))
        XCTAssertTrue(source.contains("GCPServiceAccountNativeAuthTests/testKPICachedTokenResolvesOneThousandTimesWithinTwoSeconds"))
        XCTAssertTrue(source.contains("AKSServicePrincipalAuthTests/testKubeloginParserBenchmarkKPI"))
        XCTAssertTrue(source.contains("OIDCNativeAuthTests/testJWTParsingBenchmarkKPI"))
        XCTAssertTrue(source.contains("CONFIGURATION:-release"))
        XCTAssertTrue(source.contains("set -euo pipefail"))
        XCTAssertTrue(source.contains("CLANG_MODULE_CACHE_PATH"))
        XCTAssertTrue(source.contains("SWIFTPM_MODULECACHE_OVERRIDE"))
    }

    func testRunnerRecordsUsefulMetadataWithoutPersonalOrClusterIdentity() throws {
        let source = try String(
            contentsOf: repoRoot.appendingPathComponent("scripts/run-selected-kpis.sh"),
            encoding: .utf8
        )

        for field in [
            "generated_at_utc",
            "configuration",
            "git_revision",
            "swift_version",
            "architecture",
            "os_version",
            "logical_cpu_count",
            "physical_memory_bytes",
            "cpu_model"
        ] {
            XCTAssertTrue(source.contains(field), "Missing KPI environment field \(field).")
        }

        XCTAssertFalse(source.contains("hostname"))
        XCTAssertFalse(source.contains("whoami"))
        XCTAssertFalse(source.contains("USER="))
        XCTAssertFalse(source.contains("kubeconfig"))
        XCTAssertFalse(source.contains("selectedContext"))
        XCTAssertTrue(source.contains("test-reports/selected-kpis-"))
    }

    func testBaselineUsesSectionsAndPointsAtTheTrackedRunner() throws {
        let source = try String(
            contentsOf: repoRoot.appendingPathComponent("scripts/SELECTED_KPI_BASELINE.md"),
            encoding: .utf8
        )

        XCTAssertTrue(source.contains("scripts/run-selected-kpis.sh"))
        XCTAssertTrue(source.contains("## Acceptance baseline"))
        XCTAssertFalse(source.contains("| ---"))
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}
