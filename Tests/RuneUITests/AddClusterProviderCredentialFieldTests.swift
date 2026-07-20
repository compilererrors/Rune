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
            ["Required", "Required", "Required", "Required", "Optional"]
        )
        XCTAssertEqual(nativeAKS.fields.map(\.requirementTitle), Array(repeating: "Required", count: 6))
        XCTAssertEqual(nativeGKE.fields.map(\.requirementTitle), Array(repeating: "Required", count: 4))

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

        XCTAssertEqual(accessKey.accessibilityRequirementHint, "Required field")
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

        XCTAssertTrue(componentSource.contains("Text(field.title)"))
        XCTAssertTrue(componentSource.contains("Text(field.requirementTitle)"))
        XCTAssertTrue(componentSource.contains("ViewThatFits(in: .horizontal)"))
        XCTAssertTrue(componentSource.contains("dynamicTypeSize.isAccessibilitySize"))
        XCTAssertTrue(componentSource.contains("TextField(\"\", text: $text)"))
        XCTAssertTrue(componentSource.contains("SecureField(\"\", text: $text)"))
        XCTAssertTrue(componentSource.contains("if field.input == .secureText"))
        XCTAssertTrue(componentSource.contains(".accessibilityLabel(field.title)"))
        XCTAssertTrue(componentSource.contains(".accessibilityHint(field.accessibilityRequirementHint)"))
        XCTAssertTrue(componentSource.contains(".accessibilityIdentifier(field.accessibilityIdentifier)"))
        XCTAssertTrue(componentSource.contains(".accessibilityHidden(true)"))
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
                XCTAssertGreaterThan(enlargedHost.frame.height, regular.height)
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
        XCTAssertTrue(fields.allSatisfy {
            $0.accessibilityRequirementHint == ($0.isRequired ? "Required field" : "Optional field")
        })

        let componentSource = try source("Sources/RuneUI/Views/AddClusterProviderCredentialField.swift")
        XCTAssertTrue(componentSource.contains(".accessibilityLabel(field.title)"))
        XCTAssertTrue(componentSource.contains(".accessibilityHint(field.accessibilityRequirementHint)"))
        XCTAssertTrue(componentSource.contains(".accessibilityIdentifier(field.accessibilityIdentifier)"))
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
