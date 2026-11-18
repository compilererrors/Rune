#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CONFIGURATION="${CONFIGURATION:-release}"
RUNS="${RUNS:-1}"
# fast: the user-facing flows, before every release build. full: everything, nightly.
KPI_TIER="${KPI_TIER:-full}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_DIR="${RESULTS_DIR:-$REPO_ROOT/test-reports/selected-kpis-$TIMESTAMP}"
METADATA_FILE="$RESULTS_DIR/environment.txt"
RESULTS_FILE="$RESULTS_DIR/results.tsv"
SELECTED_KPI_MODULE_CACHE_DIR="${RUNE_SELECTED_KPI_MODULE_CACHE_DIR:-$REPO_ROOT/.build-cache/selected-kpis}"

if [[ "$CONFIGURATION" != "debug" && "$CONFIGURATION" != "release" ]]; then
  echo "CONFIGURATION must be debug or release." >&2
  exit 2
fi

if [[ "$KPI_TIER" != "fast" && "$KPI_TIER" != "full" ]]; then
  echo "KPI_TIER must be fast or full." >&2
  exit 2
fi

if [[ ! "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
  echo "RUNS must be a positive integer." >&2
  exit 2
fi

mkdir -p \
  "$RESULTS_DIR" \
  "$SELECTED_KPI_MODULE_CACHE_DIR/clang" \
  "$SELECTED_KPI_MODULE_CACHE_DIR/swiftpm"

export CLANG_MODULE_CACHE_PATH="${CLANG_MODULE_CACHE_PATH:-$SELECTED_KPI_MODULE_CACHE_DIR/clang}"
export SWIFTPM_MODULECACHE_OVERRIDE="${SWIFTPM_MODULECACHE_OVERRIDE:-$SELECTED_KPI_MODULE_CACHE_DIR/swiftpm}"

git_revision="$(git -C "$REPO_ROOT" rev-parse --short=12 HEAD 2>/dev/null || echo unavailable)"
if [[ -z "$(git -C "$REPO_ROOT" status --porcelain --untracked-files=normal 2>/dev/null)" ]]; then
  git_worktree="clean"
else
  git_worktree="dirty"
fi

{
  echo "generated_at_utc=$TIMESTAMP"
  echo "configuration=$CONFIGURATION"
  echo "runs=$RUNS"
  echo "kpi_tier=$KPI_TIER"
  echo "git_revision=$git_revision"
  echo "git_worktree=$git_worktree"
  echo "architecture=$(uname -m)"
  echo "os_version=$(sw_vers -productVersion 2>/dev/null || echo unavailable)"
  echo "os_build=$(sw_vers -buildVersion 2>/dev/null || echo unavailable)"
  echo "swift_version=$(swift --version 2>&1 | sed -n '1p')"
  echo "logical_cpu_count=$(sysctl -n hw.logicalcpu 2>/dev/null || echo unavailable)"
  echo "physical_memory_bytes=$(sysctl -n hw.memsize 2>/dev/null || echo unavailable)"
  echo "cpu_model=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unavailable)"
} > "$METADATA_FILE"

# Typing, terminal output, tab switching, resizing, scrolling, logs and
# YAML editing: what people feel in every session.
fast_kpi_filters=(
  "RuneTerminalTypingSafeguardTests/testKeystrokesStayFastKPI"
  "TerminalFeaturePerformanceKPITests/testTerminalKeystrokeHandlingKPI"
  "TerminalFeaturePerformanceKPITests/testTerminalEmulatorFullScreenOutputKPI"
  "RuneTerminalWorkloadTests/testTerminalOutputEndToEndKPI"
  "RuneTerminalWorkloadTests/testShellTabSwitchKPI"
  "RuneTerminalWorkloadTests/testDraggingTheInspectorDividerOnlyLaysOut"
  "RuneTerminalWorkloadTests/testWindowLiveResizeKPI"
  "RuneDocumentFlowKPITests/testLogFollowAppendKPI"
  "RuneDocumentFlowKPITests/testYAMLEditorTypingInALargeManifestKPI"
  "TextSizeChangeCostTests/testStepsOnALongLogStayInstant"
  "RuneScrollSmoothnessBenchmarkTests/testScrollingLargeTablesAndDocumentsStaysInsideFrameBudgetKPI"
  "RuneResourceTableScaleBenchmarkTests/testLargePodTableMountFilterAndLiveResizeBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testColdStartLaunchShellInitialMountKPI"
  "RunePerformanceBenchmarksTests/testFakeRESTRapidViewSwitchBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testLargeLogInspectorInitialMountBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testLogSearchBenchmarkKPI"
)

# Auth, cloud import, REST metrics, idle cost, memory and the remaining
# load and search paths.
full_only_kpi_filters=(
  "CloudOAuthPerformanceTests/testAuthorizationRoundTripBenchmarkKPI"
  "CloudOAuthPerformanceTests/testSheetChurnBenchmarkKPI"
  "CloudOAuthAuthorizationCoordinatorTests/testSheetChurnUsesFreshGenerationsAndRejectsCrossSheetCallbacksKPI"
  "RunePerformanceBenchmarksTests/testColdStartViewModelInitializationKPI"
  "RunePerformanceBenchmarksTests/testBridgedRapidAppKitSelectionBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testKubeConfigImportTransactionResolutionMetadataProjectionBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testRESTRequestMetricsRecordingBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testRESTRequestMetricsRetentionChurnBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testRESTRequestMetricsGroupingBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testRESTRequestMetricsDebugHighlightsBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testYAMLEditorLocalValidationBoundaryBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testExplicitYAMLServerDryRunBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testKubernetesYAMLValidationErrorParsingBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testNativeCloudImportDiagnosticProjectionBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testNativeCloudImportAdmissionGuardBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testMockedNativeEKSImportReviewBindingAndCoreLoadBenchmarkKPI"
  "NativeCloudImportParityTests/testHeadlessAKSAndGKEImportReviewBindingReleaseKPI"
  "RunePerformanceBenchmarksTests/testResourceListColumnLayoutBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testAppKitResourceColumnResizePreviewBenchmarkKPI"
  "AWSEKSNativeAuthTests/testKPISignsOneThousandTokensWithinFiveSeconds"
  "GCPServiceAccountNativeAuthTests/testKPICachedTokenResolvesOneThousandTimesWithinTwoSeconds"
  "AKSServicePrincipalAuthTests/testKubeloginParserBenchmarkKPI"
  "OIDCNativeAuthTests/testJWTParsingBenchmarkKPI"
  "ExecCredentialQualityGateTests/testConcurrentExecResolutionIsSingleFlightAndCachedWithinKPI"
  "TextSizeChangeCostTests/testStepsOnATranscriptTextViewStayInstant"
  "TextSizeChangeCostTests/testStepsOnALargeTranscriptSurfaceStayInstant"
  "RuneTerminalWorkloadTests/testIdleCPUAndWakeupsKPI"
  "RuneTerminalWorkloadTests/testMemoryWithManyTabsAndLongLogsKPI"
  "RunePerformanceBenchmarksTests/testFakeRESTPodAndLogLoadBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testContextSwitchNamespaceCarryResolutionBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testViewModelRapidResourceMenuSwitchBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testLargeTerminalTranscriptInitialMountBenchmarkKPI"
  "RuneYAMLValidatorTests/testValidationOfALargeDocumentStaysFastKPI"
  "RunePerformanceBenchmarksTests/testTerminalSessionAppendBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testTerminalTranscriptAppendWhileSearchOpenBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testIncrementalLogSearchWithReusedIndexBenchmarkKPI"
  "RunePerformanceBenchmarksTests/testCommandPaletteGlobalSearchAndAliasBenchmarkKPI"
)

kpi_filters=("${fast_kpi_filters[@]}")
if [[ "$KPI_TIER" == "full" ]]; then
  kpi_filters+=("${full_only_kpi_filters[@]}")
fi

printf "run\tfilter\tstatus\telapsed_seconds\n" > "$RESULTS_FILE"
overall_status=0

for run in $(seq 1 "$RUNS"); do
  for filter in "${kpi_filters[@]}"; do
    safe_name="$(printf "%s" "$filter" | tr -c 'A-Za-z0-9._-' '_')"
    log_file="$RESULTS_DIR/run-${run}-${safe_name}.log"
    start_seconds="$(date +%s)"
    swift_args=(test --filter "$filter")
    if [[ "$CONFIGURATION" == "release" ]]; then
      swift_args=(test -c release --filter "$filter")
    fi

    echo "== Selected KPI $filter · run $run/$RUNS · $CONFIGURATION =="
    if (
      cd "$REPO_ROOT"
      swift "${swift_args[@]}"
    ) 2>&1 | tee "$log_file"; then
      status="pass"
    else
      status="fail"
      overall_status=1
    fi
    elapsed_seconds="$(( $(date +%s) - start_seconds ))"
    printf "%s\t%s\t%s\t%s\n" "$run" "$filter" "$status" "$elapsed_seconds" >> "$RESULTS_FILE"
  done
done

passed="$(awk -F '\t' 'NR > 1 && $3 == "pass" { count += 1 } END { print count + 0 }' "$RESULTS_FILE")"
failed="$(awk -F '\t' 'NR > 1 && $3 == "fail" { count += 1 } END { print count + 0 }' "$RESULTS_FILE")"
echo "Selected KPI result ($KPI_TIER tier): $passed passed, $failed failed."
echo "Environment: $METADATA_FILE"
echo "Results: $RESULTS_FILE"
exit "$overall_status"
