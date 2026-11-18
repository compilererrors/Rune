import Foundation
import XCTest

final class RuneSelectedKPIRunnerTests: XCTestCase {
    func testTrackedRunnerIncludesReleaseRESTNativeAndResourceGates() throws {
        let scriptURL = repoRoot.appendingPathComponent("scripts/run-selected-kpis.sh")
        let source = try String(contentsOf: scriptURL, encoding: .utf8)

        XCTAssertTrue(FileManager.default.isExecutableFile(atPath: scriptURL.path))
        XCTAssertTrue(source.contains("RunePerformanceBenchmarksTests/testRESTRequestMetricsRecordingBenchmarkKPI"))
        XCTAssertTrue(source.contains("RunePerformanceBenchmarksTests/testRESTRequestMetricsRetentionChurnBenchmarkKPI"))
        XCTAssertTrue(source.contains("RunePerformanceBenchmarksTests/testYAMLEditorLocalValidationBoundaryBenchmarkKPI"))
        XCTAssertTrue(source.contains("RunePerformanceBenchmarksTests/testExplicitYAMLServerDryRunBenchmarkKPI"))
        XCTAssertTrue(source.contains("RunePerformanceBenchmarksTests/testKubernetesYAMLValidationErrorParsingBenchmarkKPI"))
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

    /// The fast tier gates every release build on the flows people feel;
    /// the full tier adds everything else.
    func testRunnerHasFastAndFullTiers() throws {
        let source = try String(contentsOf: repoRoot.appendingPathComponent("scripts/run-selected-kpis.sh"), encoding: .utf8)
        XCTAssertTrue(source.contains("KPI_TIER=\"${KPI_TIER:-full}\""))
        XCTAssertTrue(source.contains("kpi_tier=$KPI_TIER"))

        let fast = try filters(named: "fast_kpi_filters", in: source)
        let fullOnly = try filters(named: "full_only_kpi_filters", in: source)
        XCTAssertTrue(Set(fast).isDisjoint(with: fullOnly), "a KPI belongs to one tier")
        for kpi in [
            "RuneTerminalTypingSafeguardTests/testKeystrokesStayFastKPI",
            "RuneTerminalWorkloadTests/testTerminalOutputEndToEndKPI",
            "RuneTerminalWorkloadTests/testShellTabSwitchKPI",
            "RuneTerminalWorkloadTests/testWindowLiveResizeKPI",
            "RuneDocumentFlowKPITests/testLogFollowAppendKPI",
            "RuneDocumentFlowKPITests/testYAMLEditorTypingInALargeManifestKPI",
            "RuneScrollSmoothnessBenchmarkTests/testScrollingLargeTablesAndDocumentsStaysInsideFrameBudgetKPI",
            "RunePerformanceBenchmarksTests/testColdStartLaunchShellInitialMountKPI"
        ] {
            XCTAssertTrue(fast.contains(kpi), "\(kpi) is in the fast tier")
        }
        for kpi in [
            "RuneTerminalWorkloadTests/testIdleCPUAndWakeupsKPI",
            "RuneTerminalWorkloadTests/testMemoryWithManyTabsAndLongLogsKPI",
            "RuneYAMLValidatorTests/testValidationOfALargeDocumentStaysFastKPI",
            "RunePerformanceBenchmarksTests/testCommandPaletteGlobalSearchAndAliasBenchmarkKPI"
        ] {
            XCTAssertTrue(fullOnly.contains(kpi), "\(kpi) is in the full tier")
        }
    }

    private func filters(named name: String, in source: String) throws -> [String] {
        let start = try XCTUnwrap(source.range(of: "\(name)=(\n"))
        let end = try XCTUnwrap(source.range(of: "\n)", range: start.upperBound..<source.endIndex))
        return source[start.upperBound..<end.lowerBound]
            .split(separator: "\n")
            .map { $0.trimmingCharacters(in: .whitespaces).trimmingCharacters(in: CharacterSet(charactersIn: "\"")) }
            .filter { !$0.isEmpty }
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

    /// The baseline is a local release checklist, not part of the repository.
    func testBaselineUsesSectionsAndPointsAtTheTrackedRunner() throws {
        let baseline = repoRoot.appendingPathComponent("scripts/SELECTED_KPI_BASELINE.md")
        guard FileManager.default.fileExists(atPath: baseline.path) else {
            throw XCTSkip("The KPI baseline is kept locally.")
        }
        let source = try String(contentsOf: baseline, encoding: .utf8)

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
