import AppKit
import SwiftUI
import XCTest
@testable import RuneUI

@MainActor
final class AddClusterProviderCredentialFieldTests: XCTestCase {
    func testRequirementProjectionAndInputSecuritySemanticsStayExplicit() throws {
        let directEKS = presentation(.eks, mode: .externalCLI)
        let nativeEKS = presentation(.eks, mode: .nativeOnly)
        let nativeAKS = presentation(.aks, mode: .nativeOnly)
        let nativeGKE = presentation(.gke, mode: .nativeOnly)

        XCTAssertEqual(directEKS.fields.map(\.requirementTitle), ["Required", "Required", "Optional", "Optional"])
        XCTAssertEqual(
            nativeEKS.fields.map(\.requirementTitle),
            [nil, nil, nil, nil, "Optional"]
        )
        XCTAssertEqual(
            nativeAKS.fields.map(\.requirementTitle),
            Array<String?>(repeating: nil, count: 6)
        )
        XCTAssertEqual(
            nativeGKE.fields.map(\.requirementTitle),
            Array<String?>(repeating: nil, count: 4)
        )

        let accessKey = try field(.awsAccessKeyID, in: nativeEKS)
        let secretKey = try field(.awsSecretAccessKey, in: nativeEKS)
        let sessionToken = try field(.awsSessionToken, in: nativeEKS)
        let azureSecret = try field(.azureClientSecret, in: nativeAKS)
        let serviceAccount = try field(.googleServiceAccountJSON, in: nativeGKE)

        XCTAssertEqual(accessKey.input, .text, "AWS access key IDs must remain visible identifiers.")
        XCTAssertEqual(secretKey.input, .secureText)
        XCTAssertEqual(sessionToken.input, .secureText)
        XCTAssertEqual(azureSecret.input, .secureText)
        XCTAssertEqual(serviceAccount.input, .sensitiveJSONFile)

        XCTAssertEqual(
            accessKey.accessibilityRequirementHint,
            "Needed only when using this optional import method"
        )
        XCTAssertEqual(sessionToken.accessibilityRequirementHint, "Optional field")
        XCTAssertEqual(
            accessKey.accessibilityIdentifier,
            "rune.add-cluster.provider-field.awsAccessKeyID"
        )
    }

    func testRootAdoptsPresentationFieldsWithoutPlaceholderOnlyOptionalLabels() throws {
        let rootSource = try source("Sources/RuneUI/Views/RuneRootView.swift")
        let componentSource = try source("Sources/RuneUI/Views/AddClusterProviderCredentialField.swift")
        let providerRegion = try XCTUnwrap(rootSource.slice(
            from: "private func providerCredentialFields(_ fields: [AddClusterProviderField]) -> some View",
            to: "private func providerPrimaryAction("
        ))

        XCTAssertTrue(rootSource.contains("providerCredentialFields(presentation.fields)"))
        XCTAssertTrue(rootSource.contains("presentation.credentialSectionTitle"))
        XCTAssertTrue(rootSource.contains("addClusterProviderAdvancedImportSection("))
        XCTAssertTrue(rootSource.contains("isAddClusterProviderAdvancedImportExpanded"))
        XCTAssertTrue(rootSource.contains("Label(\"Advanced\", systemImage: \"slider.horizontal.3\")"))
        XCTAssertTrue(rootSource.contains("addClusterProviderRecommendedPath("))
        XCTAssertTrue(rootSource.contains("Text(\"Recommended\")"))
        XCTAssertTrue(rootSource.contains("RuneDisclosureSection(\n                                    \"Help & tools\""))
        XCTAssertTrue(rootSource.contains("Text(\"Optional\")"))
        XCTAssertTrue(rootSource.contains("addClusterProviderFormSection(\"Tools\")"))
        XCTAssertTrue(providerRegion.contains("LazyVGrid("))
        XCTAssertTrue(providerRegion.contains("columns: addClusterProviderCredentialColumns"))
        XCTAssertTrue(providerRegion.contains("ForEach(fields) { field in"))
        XCTAssertTrue(providerRegion.contains("providerCredentialInput(field)"))
        XCTAssertTrue(providerRegion.contains("AddClusterProviderCredentialTextInput(field: field"))
        XCTAssertTrue(providerRegion.contains("AddClusterProviderCredentialField(field: field)"))
        XCTAssertFalse(providerRegion.localizedCaseInsensitiveContains("(optional)"))

        for fieldID in AddClusterProviderFieldIdentifier.allCasesForCredentialFieldTests {
            XCTAssertTrue(
                providerRegion.contains(".\(fieldID.rawValue)"),
                "Root must adopt the presentation field \(fieldID.rawValue)."
            )
        }

        XCTAssertTrue(componentSource.contains("titleText = field.title"))
        XCTAssertTrue(componentSource.contains("requirementTitle = field.requirementTitle"))
        XCTAssertTrue(componentSource.contains("Text(titleText)"))
        XCTAssertTrue(componentSource.contains("Text(requirementTitle)"))
        XCTAssertTrue(componentSource.contains("standardLabelRowHeight"))
        XCTAssertTrue(componentSource.contains("HStack(alignment: .firstTextBaseline, spacing: 6)"))
        XCTAssertFalse(componentSource.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertTrue(componentSource.contains("dynamicTypeSize.isAccessibilitySize"))
        XCTAssertTrue(componentSource.contains("TextField(\"\", text: $text)"))
        XCTAssertTrue(componentSource.contains("SecureField(\"\", text: $text)"))
        XCTAssertTrue(componentSource.contains("if field.input == .secureText"))
        XCTAssertTrue(componentSource.contains(".accessibilityLabel(titleText)"))
        XCTAssertTrue(componentSource.contains(".accessibilityHint(accessibilityRequirementHint)"))
        XCTAssertTrue(componentSource.contains(".accessibilityIdentifier(fieldAccessibilityIdentifier)"))
        XCTAssertTrue(componentSource.contains(".accessibilityHidden(true)"))
    }

    func testProviderSheetCredentialGridAdaptsWithoutCompressingFields() throws {
        let rootSource = try source("Sources/RuneUI/Views/RuneRootView.swift")
        let columnRegion = try XCTUnwrap(rootSource.slice(
            from: "private var addClusterProviderCredentialColumns: [GridItem]",
            to: "private var selectedAddClusterNativeContextOption"
        ))

        XCTAssertTrue(columnRegion.contains("if dynamicTypeSize.isAccessibilitySize"))
        XCTAssertTrue(columnRegion.contains("GridItem(.flexible()"))
        XCTAssertTrue(
            columnRegion.contains(
                ".adaptive(minimum: RuneAddClusterProviderActionLayout.minimumCredentialFieldWidth)"
            )
        )
        XCTAssertTrue(columnRegion.contains("alignment: .top"))

        let contentWidth = RuneAddClusterProviderActionLayout.contentWidth()
        let spacing = RuneUILayoutMetrics.dialogControlSpacing
        let twoColumnWidth = (contentWidth - spacing) / 2
        let threeColumnWidth = (contentWidth - spacing * 2) / 3

        XCTAssertGreaterThanOrEqual(
            twoColumnWidth,
            RuneAddClusterProviderActionLayout.minimumCredentialFieldWidth
        )
        XCTAssertLessThan(
            threeColumnWidth,
            RuneAddClusterProviderActionLayout.minimumCredentialFieldWidth,
            "The standard provider sheet should resolve to two readable credential columns, not three compressed fields."
        )
    }

    func testManualTokenFieldsReuseCredentialAccessibilityContract() throws {
        let popoverSource = try source("Sources/RuneUI/Views/AddClusterPopoverView.swift")
        let componentSource = try source("Sources/RuneUI/Views/AddClusterProviderCredentialField.swift")
        let manualRegion = try XCTUnwrap(popoverSource.slice(
            from: "private var manualTokenSection: some View",
            to: "private var gridColumns: [GridItem]"
        ))

        XCTAssertEqual(
            manualRegion.components(separatedBy: "AddClusterProviderCredentialField(").count - 1,
            4
        )
        XCTAssertTrue(manualRegion.contains("title: \"Context name\",\n                    isRequired: true"))
        XCTAssertTrue(manualRegion.contains("title: \"Server URL\",\n                    isRequired: true"))
        XCTAssertTrue(manualRegion.contains("title: \"Namespace\",\n                    isRequired: false"))
        XCTAssertTrue(manualRegion.contains("title: \"Bearer token\",\n                    isRequired: true"))
        let accessibilityIdentifiers = [
            "rune.add-cluster.manual-field.context-name",
            "rune.add-cluster.manual-field.server-url",
            "rune.add-cluster.manual-field.namespace",
            "rune.add-cluster.manual-field.bearer-token",
        ]
        XCTAssertEqual(Set(accessibilityIdentifiers).count, 4)
        for accessibilityIdentifier in accessibilityIdentifiers {
            XCTAssertEqual(
                manualRegion.components(separatedBy: "\"\(accessibilityIdentifier)\"").count - 1,
                accessibilityIdentifier.hasSuffix("bearer-token") ? 2 : 1,
                "Every manual credential control needs one stable accessibility identity; the secure field repeats it directly for AppKit."
            )
        }
        XCTAssertTrue(manualRegion.contains("SecureField(\"Required token\""))
        XCTAssertTrue(manualRegion.contains(".accessibilityLabel(\"Bearer token\")"))
        XCTAssertTrue(manualRegion.contains(".accessibilityHint(\"Required field\")"))
        XCTAssertTrue(
            manualRegion.contains(
                ".accessibilityIdentifier(\"rune.add-cluster.manual-field.bearer-token\")"
            )
        )
        XCTAssertFalse(popoverSource.contains("private func manualField"))

        XCTAssertTrue(componentSource.contains("init(\n        title: String,\n        isRequired: Bool,"))
        XCTAssertTrue(componentSource.contains("isRequired ? \"Required field\" : \"Optional field\""))
        XCTAssertTrue(componentSource.contains("fieldAccessibilityIdentifier = accessibilityIdentifier"))
    }

    func testAllProviderModesRenderAt400PointsWithEnlargedText() throws {
        for mode in [AddClusterProviderExecutionMode.externalCLI, .nativeOnly] {
            for provider in [AddClusterProviderIdentifier.aks, .eks, .gke] {
                let fields = presentation(provider, mode: mode).fields
                let regular = fittingSize(fields: fields, dynamicTypeSize: .large)
                let enlargedHost = renderHost(fields: fields, dynamicTypeSize: .accessibility3)

                XCTAssertEqual(regular.width, AddClusterProviderCredentialFieldMetrics.supportedCompactWidth, accuracy: 0.5)
                XCTAssertEqual(
                    enlargedHost.frame.width,
                    AddClusterProviderCredentialFieldMetrics.supportedCompactWidth,
                    accuracy: 0.5
                )
                XCTAssertGreaterThan(enlargedHost.frame.height, CGFloat(fields.count) * 32)
                XCTAssertFalse(
                    scrollViews(in: enlargedHost).contains(where: \.hasHorizontalScroller),
                    "Provider fields must not require horizontal scrolling at 400pt."
                )

                let png = try renderedPNG(from: enlargedHost)
                XCTAssertGreaterThan(png.count, 2_000)
            }
        }
    }

    func testCredentialControlsExposeStableAccessibilityContract() throws {
        let fields = [AddClusterProviderExecutionMode.externalCLI, .nativeOnly].flatMap { mode in
            [AddClusterProviderIdentifier.aks, .eks, .gke].flatMap {
                presentation($0, mode: mode).fields
            }
        }
        let identifiers = fields.map(\.accessibilityIdentifier)

        XCTAssertEqual(Set(identifiers).count, Set(fields.map(\.id)).count)
        XCTAssertTrue(fields.allSatisfy { !$0.title.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty })
        XCTAssertTrue(fields.allSatisfy { field in
            switch field.requirement {
            case .required:
                return field.accessibilityRequirementHint == "Required field"
            case .optional:
                return field.accessibilityRequirementHint == "Optional field"
            case .requiredForOptionalMethod:
                return field.accessibilityRequirementHint == "Needed only when using this optional import method"
            }
        })

        let componentSource = try source("Sources/RuneUI/Views/AddClusterProviderCredentialField.swift")
        XCTAssertTrue(componentSource.contains(".accessibilityLabel(titleText)"))
        XCTAssertTrue(componentSource.contains(".accessibilityHint(accessibilityRequirementHint)"))
        XCTAssertTrue(componentSource.contains(".accessibilityIdentifier(fieldAccessibilityIdentifier)"))
        XCTAssertTrue(componentSource.contains(".accessibilityHidden(true)"))
    }

    func testCompactProviderFieldLayoutBenchmarkKPI() {
        let fields = presentation(.eks, mode: .externalCLI).fields
            + presentation(.eks, mode: .nativeOnly).fields
        let iterations = 24
        let started = ContinuousClock.now

        for _ in 0..<iterations {
            _ = fittingSize(fields: fields, dynamicTypeSize: .accessibility3)
        }

        let elapsed = started.duration(to: .now).seconds
        print(
            "KPI provider fields: \(iterations) rendered 400pt enlarged-text layouts in "
                + String(format: "%.3f", elapsed) + "s (target < 1.000s debug)."
        )
        XCTAssertLessThan(elapsed, 1.0)
    }

    private func presentation(
        _ provider: AddClusterProviderIdentifier,
        mode: AddClusterProviderExecutionMode
    ) -> AddClusterProviderPresentation {
        AddClusterProviderPresentation.resolve(provider: provider, mode: mode)
    }

    private func field(
        _ id: AddClusterProviderFieldIdentifier,
        in presentation: AddClusterProviderPresentation
    ) throws -> AddClusterProviderField {
        try XCTUnwrap(presentation.fields.first { $0.id == id })
    }

    private func fittingSize(
        fields: [AddClusterProviderField],
        dynamicTypeSize: DynamicTypeSize
    ) -> CGSize {
        let host = NSHostingView(rootView: fixture(fields: fields, dynamicTypeSize: dynamicTypeSize))
        host.layoutSubtreeIfNeeded()
        return host.fittingSize
    }

    private func renderHost(
        fields: [AddClusterProviderField],
        dynamicTypeSize: DynamicTypeSize
    ) -> NSView {
        let host = NSHostingView(rootView: fixture(fields: fields, dynamicTypeSize: dynamicTypeSize))
        host.layoutSubtreeIfNeeded()
        host.frame = NSRect(origin: .zero, size: host.fittingSize)
        host.layoutSubtreeIfNeeded()
        return host
    }

    private func fixture(
        fields: [AddClusterProviderField],
        dynamicTypeSize: DynamicTypeSize
    ) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            ForEach(fields) { field in
                if field.input == .sensitiveJSONFile {
                    AddClusterProviderCredentialField(field: field) {
                        Label("Choose a synthetic JSON document below.", systemImage: "doc.badge.plus")
                    }
                } else {
                    AddClusterProviderCredentialTextInput(
                        field: field,
                        text: .constant("synthetic-value")
                    )
                }
            }
        }
        .padding(12)
        .dynamicTypeSize(dynamicTypeSize)
        .environment(\.locale, Locale(identifier: "en"))
        .frame(width: AddClusterProviderCredentialFieldMetrics.supportedCompactWidth)
    }

    private func scrollViews(in root: NSView) -> [NSScrollView] {
        var result: [NSScrollView] = []
        if let scrollView = root as? NSScrollView {
            result.append(scrollView)
        }
        for child in root.subviews {
            result.append(contentsOf: scrollViews(in: child))
        }
        return result
    }

    private func renderedPNG(from view: NSView) throws -> Data {
        view.layoutSubtreeIfNeeded()
        let representation = try XCTUnwrap(view.bitmapImageRepForCachingDisplay(in: view.bounds))
        view.cacheDisplay(in: view.bounds, to: representation)
        return try XCTUnwrap(representation.representation(using: .png, properties: [:]))
    }

    private func source(_ relativePath: String) throws -> String {
        try String(contentsOf: repositoryRoot.appendingPathComponent(relativePath), encoding: .utf8)
    }

    private var repositoryRoot: URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }
}

private extension AddClusterProviderFieldIdentifier {
    static let allCasesForCredentialFieldTests: [Self] = [
        .clusterName,
        .resourceGroup,
        .subscription,
        .region,
        .profile,
        .roleARN,
        .location,
        .projectID,
        .awsAccessKeyID,
        .awsSecretAccessKey,
        .awsSessionToken,
        .azureTenantID,
        .azureClientID,
        .azureClientSecret,
        .googleServiceAccountJSON,
    ]
}

private extension Duration {
    var seconds: Double {
        let components = self.components
        return Double(components.seconds) + Double(components.attoseconds) / 1e18
    }
}

private extension String {
    func slice(from start: String, to end: String) -> String? {
        guard let startRange = range(of: start),
              let endRange = range(of: end, range: startRange.upperBound..<endIndex) else {
            return nil
        }
        return String(self[startRange.lowerBound..<endRange.lowerBound])
    }
}
