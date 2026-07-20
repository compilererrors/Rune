#if DEBUG
import AppKit
import RuneSecurity
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class RuneUIComponentGalleryTests: XCTestCase {
    func testSyntheticFixturesCoverLongLabelsAndIndependentAccessibilityActions() {
        XCTAssertTrue(RuneUIComponentGalleryFixtures.longContext.hasPrefix("synthetic-"))
        XCTAssertTrue(RuneUIComponentGalleryFixtures.longNamespace.hasPrefix("synthetic-"))
        XCTAssertTrue(RuneUIComponentGalleryFixtures.longResource.hasPrefix("synthetic-"))
        XCTAssertTrue(RuneUIComponentGalleryFixtures.emptyCommandPaletteQuery.hasPrefix("synthetic-"))
        XCTAssertGreaterThan(RuneUIComponentGalleryFixtures.longContext.count, 48)
        XCTAssertGreaterThan(RuneUIComponentGalleryFixtures.longSettingTitle.count, 60)
        XCTAssertEqual(RuneUIComponentGalleryFixtures.portForwardSession.contextName, "synthetic-context")
        XCTAssertEqual(RuneUIComponentGalleryFixtures.importReview.sourceName, "synthetic-kubeconfig.yaml")
        XCTAssertTrue(RuneUIComponentGalleryFixtures.importReview.redactedPreview.contains("synthetic-context"))
        XCTAssertFalse(RuneUIComponentGalleryFixtures.importReview.redactedPreview.contains("token:"))

        let labels = RuneUIComponentGalleryFixtures.independentActionLabels
        XCTAssertEqual(Set(labels).count, labels.count)
        XCTAssertTrue(labels.contains(RuneUIComponentGalleryFixtures.refreshActionLabel))
        XCTAssertTrue(labels.contains(RuneUIComponentGalleryFixtures.favoriteActionLabel))
        XCTAssertTrue(labels.contains(RuneUIComponentGalleryFixtures.disabledActionLabel))
        XCTAssertTrue(labels.contains(RuneUIComponentGalleryFixtures.closeDialogActionLabel))
        XCTAssertTrue(labels.contains("Retry synthetic request"))
        XCTAssertTrue(labels.contains("Remove Log Target Favorite"))
        XCTAssertTrue(labels.contains("Close log tab for \(RuneUIComponentGalleryFixtures.pods[0].name)"))

        for scenario in [
            RuneUIComponentGalleryScenario.compactMatrix,
            .minimumWindow,
        ] {
            XCTAssertTrue(scenario.requiredRegions.contains(.root))
            XCTAssertTrue(scenario.requiredRegions.contains(.contentStates))
            XCTAssertTrue(scenario.requiredRegions.contains(.inspectorScaffold))
            XCTAssertTrue(scenario.requiredRegions.contains(.terminal))
            XCTAssertTrue(scenario.requiredRegions.contains(.settings))
        }

        XCTAssertEqual(
            RuneUIComponentGalleryScenario.workflowSurfaces.requiredRegions,
            [.root, .resourceTable, .addClusterPopover, .portForwardPanel]
        )
        XCTAssertEqual(
            RuneUIComponentGalleryScenario.authenticationSurfaces.requiredRegions,
            [.root, .directProvider, .nativeOnlyProvider]
        )
        XCTAssertEqual(
            RuneUIComponentGalleryScenario.dialogSurfaces.requiredRegions,
            [.root, .commandPalette, .importReview]
        )
    }

    func testCompact320MatrixKeepsTargetsAndRegionsValid() throws {
        let hosted = try hostGallery(
            scenario: .compactMatrix,
            size: CGSize(width: 320, height: 2_200)
        )
        defer { hosted.window.orderOut(nil) }

        let snapshot = hosted.snapshot
        XCTAssertTrue(snapshot.containsRequiredRegions(for: .compactMatrix))

        let root = try frame(.root, in: snapshot)
        XCTAssertEqual(root.width, RuneInspectorScaffoldMetrics.supportedMinimumWidth, accuracy: 0.5)
        XCTAssertEqual(root.height, 2_200, accuracy: 0.5)

        for region in [
            RuneUIComponentGalleryRegion.iconRefresh,
            .iconFavorite,
            .iconDisabled,
            .dialogClose,
        ] {
            let target = try frame(region, in: snapshot)
            XCTAssertGreaterThanOrEqual(target.width, RuneUILayoutMetrics.iconButtonSize - 0.5)
            XCTAssertGreaterThanOrEqual(target.height, RuneUILayoutMetrics.iconButtonSize - 0.5)
            XCTAssertLessThanOrEqual(target.width, RuneUILayoutMetrics.iconButtonSize + 1)
            XCTAssertLessThanOrEqual(target.height, RuneUILayoutMetrics.iconButtonSize + 1)
        }

        for region in [
            RuneUIComponentGalleryRegion.contextCapsule,
            .productionCapsule,
            .readOnlyCapsule,
        ] {
            XCTAssertGreaterThanOrEqual(
                try frame(region, in: snapshot).height,
                RuneUILayoutMetrics.headerCapsuleMinimumHeight - 0.5
            )
        }

        let boundedRegions: [RuneUIComponentGalleryRegion] = [
            .capsules,
            .iconTargets,
            .contentStates,
            .adaptiveForm,
            .tabs,
            .dialog,
            .terminal,
            .settings,
            .inspectorScaffold,
        ]
        for region in boundedRegions {
            let candidate = try frame(region, in: snapshot)
            XCTAssertGreaterThanOrEqual(candidate.minX, root.minX - 0.5, "\(region.rawValue) escaped left")
            XCTAssertLessThanOrEqual(candidate.maxX, root.maxX + 0.5, "\(region.rawValue) escaped right")
        }

        for pair in zip(boundedRegions, boundedRegions.dropFirst()) {
            let first = try frame(pair.0, in: snapshot)
            let second = try frame(pair.1, in: snapshot)
            let intersection = first.intersection(second)
            XCTAssertTrue(
                intersection.isNull || intersection.width * intersection.height <= 1,
                "\(pair.0.rawValue) overlaps \(pair.1.rawValue): \(intersection)"
            )
        }
    }

    func testMinimumWindowMatrixKeepsThreePanesInside980By640Root() throws {
        let hosted = try hostGallery(
            scenario: .minimumWindow,
            size: CGSize(width: RuneWindowLayoutDefaults.minimumWidth, height: RuneWindowLayoutDefaults.minimumHeight)
        )
        defer { hosted.window.orderOut(nil) }

        let snapshot = hosted.snapshot
        XCTAssertTrue(snapshot.containsRequiredRegions(for: .minimumWindow))

        let root = try frame(.root, in: snapshot)
        let sidebar = try frame(.sidebarPane, in: snapshot)
        let content = try frame(.contentPane, in: snapshot)
        let inspector = try frame(.inspectorPane, in: snapshot)

        XCTAssertEqual(root.width, RuneWindowLayoutDefaults.minimumWidth, accuracy: 0.5)
        XCTAssertEqual(root.height, RuneWindowLayoutDefaults.minimumHeight, accuracy: 0.5)
        XCTAssertEqual(sidebar.width, RuneUILayoutMetrics.splitSidebarMinWidth, accuracy: 0.5)
        XCTAssertEqual(inspector.width, RuneUILayoutMetrics.splitDetailColumnMinWidth, accuracy: 0.5)
        XCTAssertGreaterThan(content.width, 400)

        for pane in [sidebar, content, inspector] {
            XCTAssertGreaterThanOrEqual(pane.minX, root.minX - 0.5)
            XCTAssertLessThanOrEqual(pane.maxX, root.maxX + 0.5)
            XCTAssertGreaterThanOrEqual(pane.minY, root.minY - 0.5)
            XCTAssertLessThanOrEqual(pane.maxY, root.maxY + 0.5)
            XCTAssertEqual(pane.height, root.height, accuracy: 0.5)
        }

        XCTAssertLessThanOrEqual(sidebar.maxX, content.minX + 1.5)
        XCTAssertLessThanOrEqual(content.maxX, inspector.minX + 1.5)
        XCTAssertLessThanOrEqual(try frame(.inspectorScaffold, in: snapshot).maxX, inspector.maxX + 0.5)

        let png = try renderedPNG(from: hosted.hostView)
        XCTAssertGreaterThan(png.count, 10_000, "The minimum-window visual smoke render should contain real drawn UI.")
    }

    func testCompactVisualMatrixSupportsEnlargedTextInDarkAppearance() throws {
        let regular = try hostGallery(
            scenario: .compactMatrix,
            size: CGSize(width: 320, height: 2_200),
            dynamicTypeSize: .large,
            colorScheme: .light
        )
        defer { regular.window.orderOut(nil) }

        let enlarged = try hostGallery(
            scenario: .compactMatrix,
            size: CGSize(width: 320, height: 2_800),
            dynamicTypeSize: .accessibility3,
            colorScheme: .dark
        )
        defer { enlarged.window.orderOut(nil) }

        XCTAssertTrue(regular.snapshot.containsRequiredRegions(for: .compactMatrix))
        XCTAssertTrue(enlarged.snapshot.containsRequiredRegions(for: .compactMatrix))
        XCTAssertGreaterThan(
            try frame(.contextCapsule, in: enlarged.snapshot).height,
            try frame(.contextCapsule, in: regular.snapshot).height
        )
        XCTAssertGreaterThan(
            try frame(.settings, in: enlarged.snapshot).height,
            try frame(.settings, in: regular.snapshot).height
        )

        let enlargedRoot = try frame(.root, in: enlarged.snapshot)
        for region in [
            RuneUIComponentGalleryRegion.iconTargets,
            .contentStates,
            .adaptiveForm,
            .tabs,
            .dialog,
            .terminal,
            .settings,
            .inspectorScaffold,
        ] {
            let candidate = try frame(region, in: enlarged.snapshot)
            XCTAssertGreaterThanOrEqual(candidate.minX, enlargedRoot.minX - 0.5)
            XCTAssertLessThanOrEqual(candidate.maxX, enlargedRoot.maxX + 0.5, "\(region.rawValue) clipped at enlarged text")
        }

        XCTAssertGreaterThan(try renderedPNG(from: regular.hostView).count, 10_000)
        XCTAssertGreaterThan(try renderedPNG(from: enlarged.hostView).count, 10_000)
    }

    func testWorkflowSurfaceMatrixRendersRealTablePopoverAndPortForwardInsideBounds() throws {
        let hosted = try hostGallery(
            scenario: .workflowSurfaces,
            size: CGSize(width: 480, height: 1_600)
        )
        defer { hosted.window.orderOut(nil) }

        let snapshot = hosted.snapshot
        XCTAssertTrue(snapshot.containsRequiredRegions(for: .workflowSurfaces))

        let root = try frame(.root, in: snapshot)
        let resourceTable = try frame(.resourceTable, in: snapshot)
        let addClusterPopover = try frame(.addClusterPopover, in: snapshot)
        let portForward = try frame(.portForwardPanel, in: snapshot)

        for surface in [resourceTable, portForward, addClusterPopover] {
            XCTAssertGreaterThanOrEqual(surface.minX, root.minX - 0.5)
            XCTAssertLessThanOrEqual(surface.maxX, root.maxX + 0.5)
        }
        XCTAssertEqual(addClusterPopover.width, RuneUILayoutMetrics.addClusterPopoverWidth, accuracy: 0.5)
        XCTAssertGreaterThanOrEqual(resourceTable.height, 149.5)
        XCTAssertTrue(
            descendants(of: hosted.hostView).contains(where: { $0 is NSTableView }),
            "The workflow gallery should mount the production AppKit resource table, not a drawn placeholder."
        )
        XCTAssertGreaterThan(try renderedPNG(from: hosted.hostView).count, 10_000)
    }

    func testProviderModeScenarioUsesActualDirectAndNativeOnlyPresentationContracts() throws {
        let direct = RuneUIComponentGalleryFixtures.directProviderPresentation
        let nativeOnly = RuneUIComponentGalleryFixtures.nativeOnlyProviderPresentation

        XCTAssertEqual(direct.provider, .eks)
        XCTAssertEqual(direct.executionMode, .externalCLI)
        XCTAssertTrue(direct.allowsExternalCommandExecution)
        XCTAssertTrue(direct.exposesCLIOnlyActions)
        XCTAssertEqual(direct.fields.map(\.id), [.clusterName, .region, .profile, .roleARN])

        XCTAssertEqual(nativeOnly.provider, .eks)
        XCTAssertEqual(nativeOnly.executionMode, .nativeOnly)
        XCTAssertFalse(nativeOnly.allowsExternalCommandExecution)
        XCTAssertFalse(nativeOnly.exposesCLIOnlyActions)
        XCTAssertEqual(nativeOnly.fields.map(\.id), [.awsAccessKeyID, .awsSecretAccessKey, .awsSessionToken])
        XCTAssertEqual(nativeOnly.fields.filter { $0.input == .secureText }.count, 2)

        let fieldIdentifiers = (direct.fields + nativeOnly.fields).map(\.accessibilityIdentifier)
        XCTAssertEqual(Set(fieldIdentifiers).count, fieldIdentifiers.count)

        let hosted = try hostGallery(
            scenario: .authenticationSurfaces,
            size: CGSize(width: 1_360, height: 820),
            dynamicTypeSize: .accessibility2,
            colorScheme: .dark
        )
        defer { hosted.window.orderOut(nil) }

        XCTAssertTrue(hosted.snapshot.containsRequiredRegions(for: .authenticationSurfaces))
        XCTAssertGreaterThanOrEqual(
            descendants(of: hosted.hostView).filter { $0 is NSTextField }.count,
            direct.fields.count + nativeOnly.fields.count
        )
        XCTAssertTrue(descendants(of: hosted.hostView).contains { $0 is NSSecureTextField })
    }

    func testEmptyCommandPaletteFixtureAndImportReviewStaySyntheticAndActionable() {
        let viewModel = RuneUIComponentGalleryFixtures.makeEmptyCommandPaletteViewModel()
        let query = RuneUIComponentGalleryFixtures.emptyCommandPaletteQuery

        XCTAssertTrue(viewModel.commandPaletteItems(query: query).isEmpty)
        XCTAssertEqual(viewModel.consumeCommandPalettePrefillQuery(), query)
        XCTAssertTrue(RuneUIComponentGalleryFixtures.importReview.isValid)
        XCTAssertEqual(
            RuneUIComponentGalleryFixtures.importReview.duplicateHandlingChoices,
            KubeConfigDuplicateHandlingChoice.allCases
        )
        XCTAssertTrue(
            RuneUIComponentGalleryFixtures.importReview.contexts.allSatisfy {
                $0.name.hasPrefix("synthetic-") && $0.serverHost == "cluster.example.invalid"
            }
        )
    }

    func testPairwiseWindowAppearanceTextAndThemeMatrixRendersProductionScenarios() throws {
        // Pairwise representatives cover every requested axis without multiplying
        // every size, appearance, text size, theme, and surface into a costly Cartesian suite.
        let configurations = [
            PairwiseScenarioConfiguration(
                name: "minimum-light-normal",
                scenario: .minimumWindow,
                size: CGSize(width: 980, height: 640),
                dynamicTypeSize: .large,
                colorScheme: .light,
                theme: .native
            ),
            PairwiseScenarioConfiguration(
                name: "default-workflows-light-normal",
                scenario: .workflowSurfaces,
                size: CGSize(width: 1_360, height: 820),
                dynamicTypeSize: .large,
                colorScheme: .light,
                theme: .native
            ),
            PairwiseScenarioConfiguration(
                name: "default-auth-dark-enlarged",
                scenario: .authenticationSurfaces,
                size: CGSize(width: 1_360, height: 820),
                dynamicTypeSize: .accessibility2,
                colorScheme: .dark,
                theme: .native
            ),
            PairwiseScenarioConfiguration(
                name: "wide-dialogs-fjord-normal",
                scenario: .dialogSurfaces,
                size: CGSize(width: 1_680, height: 900),
                dynamicTypeSize: .large,
                colorScheme: nil,
                theme: .fjord
            ),
        ]

        var renderedImages: [Data] = []
        for configuration in configurations {
            let hosted = try hostGallery(
                scenario: configuration.scenario,
                size: configuration.size,
                dynamicTypeSize: configuration.dynamicTypeSize,
                colorScheme: configuration.colorScheme,
                theme: configuration.theme
            )
            defer { hosted.window.orderOut(nil) }

            XCTAssertTrue(
                hosted.snapshot.containsRequiredRegions(for: configuration.scenario),
                configuration.name
            )
            let root = try frame(.root, in: hosted.snapshot)
            XCTAssertEqual(root.width, configuration.size.width, accuracy: 0.5, configuration.name)
            XCTAssertEqual(root.height, configuration.size.height, accuracy: 0.5, configuration.name)

            let surfaces = disjointSurfaceRegions(for: configuration.scenario)
            for region in surfaces {
                let candidate = try frame(region, in: hosted.snapshot)
                XCTAssertGreaterThanOrEqual(candidate.minX, root.minX - 0.5, "\(configuration.name): \(region.rawValue)")
                XCTAssertLessThanOrEqual(candidate.maxX, root.maxX + 0.5, "\(configuration.name): \(region.rawValue)")
                XCTAssertGreaterThanOrEqual(candidate.minY, root.minY - 0.5, "\(configuration.name): \(region.rawValue)")
                XCTAssertLessThanOrEqual(candidate.maxY, root.maxY + 0.5, "\(configuration.name): \(region.rawValue)")
            }

            for (index, firstRegion) in surfaces.enumerated() {
                for secondRegion in surfaces.dropFirst(index + 1) {
                    let intersection = try frame(firstRegion, in: hosted.snapshot)
                        .intersection(frame(secondRegion, in: hosted.snapshot))
                    XCTAssertTrue(
                        intersection.isNull || intersection.width * intersection.height <= 1,
                        "\(configuration.name): \(firstRegion.rawValue) overlaps \(secondRegion.rawValue): \(intersection)"
                    )
                }
            }

            let png = try renderedPNG(from: hosted.hostView)
            XCTAssertGreaterThan(png.count, 20_000, "\(configuration.name) should render real UI pixels")
            renderedImages.append(png)

            if configuration.theme == .fjord {
                let resolvedTheme = configuration.theme.resolvedTheme
                XCTAssertEqual(resolvedTheme.preferredColorScheme, .dark)
                XCTAssertNotNil(resolvedTheme.palette)
                XCTAssertNotNil(resolvedTheme.appKitPalette)
            }
        }

        XCTAssertEqual(renderedImages.count, configurations.count)
        XCTAssertEqual(Set(renderedImages).count, configurations.count, "Each pairwise scenario should produce a distinct PNG.")
    }

    func testProductionScenarioAccessibilityActionsRemainIndependent() throws {
        let gallerySource = try String(contentsOfFile: gallerySourcePath, encoding: .utf8)
        let portForwardSource = try String(contentsOfFile: portForwardSourcePath, encoding: .utf8)
        let providerFieldSource = try String(contentsOfFile: providerFieldSourcePath, encoding: .utf8)
        let commandPaletteSource = try String(contentsOfFile: commandPaletteSourcePath, encoding: .utf8)
        let importReviewSource = try String(contentsOfFile: importReviewSourcePath, encoding: .utf8)

        XCTAssertTrue(gallerySource.contains("TerminalPortForwardPanelView("))
        XCTAssertTrue(gallerySource.contains("AddClusterProviderCredentialTextInput(field: field"))
        XCTAssertTrue(gallerySource.contains("CommandPaletteView(viewModel: viewModel)"))
        XCTAssertTrue(gallerySource.contains("KubeConfigImportReviewPanel("))
        XCTAssertTrue(portForwardSource.contains(".accessibilityIdentifier(\"rune.port-forward.primary-action\")"))
        XCTAssertTrue(providerFieldSource.contains(".accessibilityIdentifier(field.accessibilityIdentifier)"))
        XCTAssertTrue(commandPaletteSource.contains("RuneDialogCloseButton(\"Close Command Palette\")"))
        XCTAssertTrue(importReviewSource.contains("isConfirmationPending ? \"Cancel kubeconfig import\""))
    }

    func testCompositeAccessibilityContractKeepsActionsDistinctAndStateful() throws {
        let hosted = try hostGallery(
            scenario: .compactMatrix,
            size: CGSize(width: 320, height: 2_200)
        )
        defer { hosted.window.orderOut(nil) }

        let gallerySource = try String(contentsOfFile: gallerySourcePath, encoding: .utf8)
        let iconSource = try String(contentsOfFile: iconSourcePath, encoding: .utf8)
        let logTabSource = try String(contentsOfFile: terminalLogTabSourcePath, encoding: .utf8)
        let labels = Set(RuneUIComponentGalleryFixtures.independentActionLabels)

        XCTAssertEqual(labels.count, RuneUIComponentGalleryFixtures.independentActionLabels.count)
        XCTAssertTrue(RuneUIComponentGalleryFixtures.selectedActionLabels.isSubset(of: labels))
        XCTAssertTrue(RuneUIComponentGalleryFixtures.disabledActionLabels.isSubset(of: labels))
        XCTAssertTrue(gallerySource.contains("RuneUIComponentGalleryFixtures.refreshActionLabel"))
        XCTAssertTrue(gallerySource.contains("RuneUIComponentGalleryFixtures.favoriteActionLabel"))
        XCTAssertTrue(gallerySource.contains("RuneUIComponentGalleryFixtures.disabledActionLabel"))
        XCTAssertTrue(gallerySource.contains("RuneUIComponentGalleryFixtures.closeDialogActionLabel"))
        XCTAssertTrue(gallerySource.contains("TerminalLogTabBar("))
        XCTAssertTrue(iconSource.contains(".accessibilityLabel(accessibilityLabel)"))
        XCTAssertTrue(iconSource.contains(".accessibilityValue(isSelected ? \"Selected\" : \"Not selected\")"))
        XCTAssertTrue(iconSource.contains(".disabled(isDisabled)"))
        XCTAssertTrue(logTabSource.contains(".accessibilityElement(children: .contain)"))
        XCTAssertTrue(logTabSource.contains("isSelected: tab.isFavorite"))

        // SwiftUI exposes its AppKit AX proxy tree only when an accessibility
        // client is active. Assert the live tree as an additional gate when it
        // is available; the deterministic component contract above always runs.
        let nodes = accessibilityNodes(in: hosted.hostView)
        if !nodes.compactMap(\.label).isEmpty {
            let liveLabels = Set(nodes.compactMap(\.label))
            for expected in RuneUIComponentGalleryFixtures.independentActionLabels {
                XCTAssertTrue(liveLabels.contains(expected), "Missing live accessibility action: \(expected)")
            }

            let favorite = try XCTUnwrap(
                nodes.first { $0.label == RuneUIComponentGalleryFixtures.favoriteActionLabel }
            )
            XCTAssertEqual(favorite.value, "Selected")

            let disabled = try XCTUnwrap(
                nodes.first { $0.label == RuneUIComponentGalleryFixtures.disabledActionLabel }
            )
            XCTAssertFalse(disabled.isEnabled)
        }
    }

    func testGalleryConstructionAndInitialLayoutBenchmarkKPI() {
        func workload() -> Int {
            var checksum = 0
            for configuration in [
                (RuneUIComponentGalleryScenario.compactMatrix, CGSize(width: 320, height: 1_600)),
                (.minimumWindow, CGSize(width: 980, height: 640)),
                (.workflowSurfaces, CGSize(width: 1_360, height: 820)),
                (.authenticationSurfaces, CGSize(width: 1_360, height: 820)),
                (.dialogSurfaces, CGSize(width: 1_680, height: 900)),
            ] {
                let controller = NSHostingController(
                    rootView: RuneUIComponentGallery(scenario: configuration.0)
                        .frame(width: configuration.1.width, height: configuration.1.height)
                )
                controller.view.frame = NSRect(origin: .zero, size: configuration.1)
                controller.view.layoutSubtreeIfNeeded()
                checksum &+= Int(controller.view.fittingSize.width.rounded())
                checksum &+= Int(controller.view.fittingSize.height.rounded())
                checksum &+= controller.view.subviews.count
            }
            return checksum
        }

        let expected = workload()
        XCTAssertEqual(workload(), expected, "Synthetic gallery construction and layout must stay deterministic.")

        let options = XCTMeasureOptions()
        options.iterationCount = 3
        var measuredChecksum = 0
        measure(metrics: [XCTClockMetric(), XCTMemoryMetric()], options: options) {
            measuredChecksum = workload()
        }
        XCTAssertEqual(measuredChecksum, expected)

        var fastest = Double.greatestFiniteMagnitude
        for _ in 0..<3 {
            let started = ContinuousClock.now
            let checksum = workload()
            let duration = started.duration(to: .now)
            let seconds = Double(duration.components.seconds)
                + Double(duration.components.attoseconds) / 1_000_000_000_000_000_000
            fastest = min(fastest, seconds)
            XCTAssertEqual(checksum, expected)
        }
        XCTAssertLessThan(
            fastest,
            1.0,
            "KPI: the five representative production-surface galleries should construct and lay out below 1s in debug."
        )
    }

    private struct PairwiseScenarioConfiguration {
        let name: String
        let scenario: RuneUIComponentGalleryScenario
        let size: CGSize
        let dynamicTypeSize: DynamicTypeSize
        let colorScheme: ColorScheme?
        let theme: RuneAppearanceTheme
    }

    private struct HostedGallery {
        let window: NSWindow
        let hostView: NSView
        let snapshot: RuneUIComponentGalleryLayoutSnapshot
    }

    private struct AccessibilityNode {
        let label: String?
        let value: String?
        let isEnabled: Bool
    }

    private func hostGallery(
        scenario: RuneUIComponentGalleryScenario,
        size: CGSize,
        dynamicTypeSize: DynamicTypeSize = .large,
        colorScheme: ColorScheme? = .light,
        theme: RuneAppearanceTheme = .native
    ) throws -> HostedGallery {
        var snapshots: [RuneUIComponentGalleryLayoutSnapshot] = []
        let controller = NSHostingController(
            rootView: RuneUIComponentGallery(
                scenario: scenario,
                onLayoutSnapshotChange: { snapshots.append($0) }
            )
            .dynamicTypeSize(dynamicTypeSize)
            .runeAppearanceTheme(theme.resolvedTheme)
            .preferredColorScheme(colorScheme)
            .frame(width: size.width, height: size.height)
        )
        let window = NSWindow(
            contentRect: NSRect(origin: .zero, size: size),
            styleMask: [.borderless],
            backing: .buffered,
            defer: false
        )
        window.isReleasedWhenClosed = false
        window.contentViewController = controller
        controller.view.frame = NSRect(origin: .zero, size: size)
        window.makeKeyAndOrderFront(nil)

        let deadline = Date().addingTimeInterval(2)
        while Date() < deadline {
            controller.view.layoutSubtreeIfNeeded()
            RunLoop.main.run(until: Date().addingTimeInterval(0.01))
            if let snapshot = snapshots.last(where: { $0.containsRequiredRegions(for: scenario) }) {
                return HostedGallery(window: window, hostView: controller.view, snapshot: snapshot)
            }
        }

        window.orderOut(nil)
        XCTFail("Timed out waiting for \(scenario.rawValue) component-gallery layout probes; snapshots=\(snapshots.count)")
        throw CancellationError()
    }

    private func frame(
        _ region: RuneUIComponentGalleryRegion,
        in snapshot: RuneUIComponentGalleryLayoutSnapshot,
        file: StaticString = #filePath,
        line: UInt = #line
    ) throws -> CGRect {
        try XCTUnwrap(snapshot[region], "Missing \(region.rawValue) frame", file: file, line: line)
    }

    private func disjointSurfaceRegions(
        for scenario: RuneUIComponentGalleryScenario
    ) -> [RuneUIComponentGalleryRegion] {
        switch scenario {
        case .compactMatrix:
            return [.capsules, .iconTargets, .contentStates, .adaptiveForm, .tabs, .dialog, .terminal, .settings, .inspectorScaffold]
        case .minimumWindow:
            return [.sidebarPane, .contentPane, .inspectorPane]
        case .workflowSurfaces:
            return [.resourceTable, .portForwardPanel, .addClusterPopover]
        case .authenticationSurfaces:
            return [.directProvider, .nativeOnlyProvider]
        case .dialogSurfaces:
            return [.commandPalette, .importReview]
        }
    }

    private func renderedPNG(from view: NSView) throws -> Data {
        view.layoutSubtreeIfNeeded()
        let bounds = view.bounds
        let representation = try XCTUnwrap(view.bitmapImageRepForCachingDisplay(in: bounds))
        view.cacheDisplay(in: bounds, to: representation)
        return try XCTUnwrap(representation.representation(using: .png, properties: [:]))
    }

    private func descendants(of view: NSView) -> [NSView] {
        view.subviews.flatMap { [$0] + descendants(of: $0) }
    }

    private func accessibilityNodes(in rootView: NSView) -> [AccessibilityNode] {
        var visited: Set<ObjectIdentifier> = []
        var result: [AccessibilityNode] = []

        func visit(_ candidate: Any) {
            guard let object = candidate as AnyObject? else { return }
            let identifier = ObjectIdentifier(object)
            guard visited.insert(identifier).inserted else { return }

            if let view = candidate as? NSView {
                let label = view.accessibilityLabel()?.trimmingCharacters(in: .whitespacesAndNewlines)
                result.append(AccessibilityNode(
                    label: label?.isEmpty == false ? label : nil,
                    value: view.accessibilityValue() as? String,
                    isEnabled: view.isAccessibilityEnabled()
                ))
                view.accessibilityChildren()?.forEach(visit)
                view.subviews.forEach(visit)
            } else if let element = candidate as? NSAccessibilityElement {
                let label = element.accessibilityLabel()?.trimmingCharacters(in: .whitespacesAndNewlines)
                result.append(AccessibilityNode(
                    label: label?.isEmpty == false ? label : nil,
                    value: element.accessibilityValue() as? String,
                    isEnabled: element.isAccessibilityEnabled()
                ))
                element.accessibilityChildren()?.forEach(visit)
            }
        }

        visit(rootView)
        return result
    }

    private var gallerySourcePath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Debug/RuneUIComponentGallery.swift").path
    }

    private var iconSourcePath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Layout/RuneDesignComponents.swift").path
    }

    private var terminalLogTabSourcePath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/TerminalLogTabBar.swift").path
    }

    private var portForwardSourcePath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/PortForwardControlComponents.swift").path
    }

    private var providerFieldSourcePath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/AddClusterProviderCredentialField.swift").path
    }

    private var commandPaletteSourcePath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/CommandPaletteView.swift").path
    }

    private var importReviewSourcePath: String {
        repoRoot.appendingPathComponent("Sources/RuneUI/Views/KubeConfigImportReviewPanel.swift").path
    }

    private var repoRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}
#endif
