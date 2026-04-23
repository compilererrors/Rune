import SwiftUI
import RuneCore

struct ResourceYAMLEditorSurface: View {
    @Binding var text: String
    let displayText: String
    let readOnlyResetID: String
    let inlineEditing: Bool
    let implementation: ManifestInlineEditorImplementation
    let validationIssues: [YAMLValidationIssue]

    var body: some View {
        let activeImplementation = inlineEditing ? implementation : .readOnlyScroll

        InspectorTextSurface(minHeight: 280) {
            Group {
                switch activeImplementation {
                case .readOnlyScroll:
                    InspectorReadOnlyTextView(
                        text: displayText,
                        resetID: readOnlyResetID,
                        contentStyle: .yaml,
                        externalValidationIssues: validationIssues
                    )
                case .swiftUITextEditor:
                    TextEditor(text: $text)
                        .font(.system(size: 12, weight: .regular, design: .monospaced))
                        .scrollContentBackground(.hidden)
                        .padding(6)
                case .appKitTextView:
                    AppKitManifestTextView(
                        text: $text,
                        isEditable: true,
                        contentStyle: .yaml,
                        externalValidationIssues: validationIssues
                    )
                }
            }
        }
    }
}

struct ResourceYAMLInspectorPane: View {
    let resourceReference: String
    @Binding var yamlText: String
    let yamlDisplayText: String
    let yamlFooterText: String
    let baseline: String
    let hasUnsavedEdits: Bool
    let canApplyMutations: Bool
    let validationIssues: [YAMLValidationIssue]
    let isValidating: Bool
    @Binding var isInlineEditing: Bool
    let inlineEditorImplementation: ManifestInlineEditorImplementation
    let onApply: () -> Void
    let onOpenEditor: () -> Void
    let onRevert: () -> Void
    let onImport: () -> Void
    let onExport: () -> Void
    let readOnlyResetID: String

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(spacing: 8) {
                if hasUnsavedEdits {
                    Text("Unsaved edits")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.orange)
                }
                Spacer(minLength: 0)
            }

            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 8) {
                    Button("Apply YAML", action: onApply)
                        .buttonStyle(.borderedProminent)
                        .disabled(!canApplyMutations || yamlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                        .help("Sends the manifest to the cluster. Closing the editor or this tab does not.")

                    if inlineEditorImplementation.supportsInlineEditing {
                        Button(isInlineEditing ? "Done" : "Quick Edit") {
                            isInlineEditing.toggle()
                        }
                        .buttonStyle(.bordered)
                        .disabled(yamlText.isEmpty)
                    }

                    Button("Edit…", action: onOpenEditor)
                        .buttonStyle(.bordered)
                        .disabled(yamlText.isEmpty)

                    Button("Revert") {
                        onRevert()
                        isInlineEditing = false
                    }
                    .buttonStyle(.bordered)
                    .disabled(!hasUnsavedEdits)

                    Divider()
                        .frame(height: 16)

                    Button("Import…") {
                        onImport()
                        onOpenEditor()
                    }
                    .buttonStyle(.bordered)
                    .help("Replace the editor with the contents of a YAML file")

                    Button("Export…", action: onExport)
                        .buttonStyle(.bordered)
                        .disabled(yamlText.isEmpty)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            YAMLValidationSummaryView(
                issues: validationIssues,
                isValidating: isValidating
            )

            ResourceYAMLEditorSurface(
                text: $yamlText,
                displayText: yamlDisplayText,
                readOnlyResetID: readOnlyResetID,
                inlineEditing: isInlineEditing,
                implementation: inlineEditorImplementation,
                validationIssues: validationIssues
            )

            if yamlText.isEmpty {
                Text(yamlFooterText)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
        .onChange(of: baseline) { _, _ in
            isInlineEditing = false
        }
    }
}

struct ResourceYAMLEditorSheetView: View {
    let resourceReference: String
    @Binding var yamlText: String
    let yamlFooterText: String
    let canApplyMutations: Bool
    let hasUnsavedEdits: Bool
    let validationIssues: [YAMLValidationIssue]
    let isValidating: Bool
    let onApply: () -> Void
    let onRevert: () -> Void
    let onImport: () -> Void
    let onExport: () -> Void
    let onClose: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: 4) {
                    Text("YAML Editor")
                        .font(.title2.weight(.bold))
                    Text(resourceReference)
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Button("Close", action: onClose)
            }

            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 8) {
                    Button("Apply YAML", action: onApply)
                        .buttonStyle(.borderedProminent)
                        .disabled(!canApplyMutations || yamlText.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                        .help("Sends the manifest to the cluster. Closing this sheet does not.")

                    Button("Revert", action: onRevert)
                        .buttonStyle(.bordered)
                        .disabled(!hasUnsavedEdits)

                    Button("Import…", action: onImport)
                        .buttonStyle(.bordered)

                    Button("Export…", action: onExport)
                        .buttonStyle(.bordered)
                        .disabled(yamlText.isEmpty)

                    Spacer(minLength: 0)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            YAMLValidationSummaryView(
                issues: validationIssues,
                isValidating: isValidating
            )

            ResourceYAMLEditorSurface(
                text: $yamlText,
                displayText: yamlText,
                readOnlyResetID: "yaml-sheet:\(resourceReference)",
                inlineEditing: true,
                implementation: .appKitTextView,
                validationIssues: validationIssues
            )

            if yamlText.isEmpty {
                Text(yamlFooterText)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }

            Text("Close dismisses this sheet only. Nothing is sent to the cluster until you tap Apply YAML or Apply on the Describe tab.")
                .font(.footnote)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
        }
        .padding(16)
        .frame(minWidth: 760, minHeight: 560)
        .background(.regularMaterial)
    }
}

private struct YAMLValidationSummaryView: View {
    let issues: [YAMLValidationIssue]
    let isValidating: Bool
    private let maxVisibleIssues = 6
    @State private var isExpanded = false

    private var visibleIssues: [YAMLValidationIssue] {
        Array(issues.prefix(maxVisibleIssues))
    }

    private var errorCount: Int {
        issues.filter { $0.severity == .error }.count
    }

    private var warningCount: Int {
        issues.filter { $0.severity == .warning }.count
    }

    private var issueSignature: String {
        issues.map(\.id).joined(separator: "|")
    }

    var body: some View {
        if isValidating || !issues.isEmpty {
            VStack(alignment: .leading, spacing: 8) {
                Button {
                    if !issues.isEmpty {
                        isExpanded.toggle()
                    }
                } label: {
                    HStack(spacing: 8) {
                        HStack(spacing: 6) {
                            if errorCount > 0 {
                                issueBadge(
                                    systemName: "xmark.circle.fill",
                                    text: "\(errorCount)",
                                    color: .red
                                )
                            }

                            if warningCount > 0 {
                                issueBadge(
                                    systemName: "exclamationmark.triangle.fill",
                                    text: "\(warningCount)",
                                    color: .orange
                                )
                            }

                            if isValidating {
                                ProgressView()
                                    .controlSize(.small)
                            }
                        }

                        Text(summaryTitle)
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(.secondary)

                        Spacer(minLength: 0)

                        if !issues.isEmpty {
                            Image(systemName: isExpanded ? "chevron.up" : "chevron.down")
                                .font(.caption2.weight(.semibold))
                                .foregroundStyle(.tertiary)
                        }
                    }
                }
                .buttonStyle(.plain)

                if isExpanded {
                    VStack(alignment: .leading, spacing: 8) {
                        ForEach(visibleIssues) { issue in
                            HStack(alignment: .top, spacing: 8) {
                                Image(systemName: symbolName(for: issue))
                                    .font(.caption)
                                    .foregroundStyle(color(for: issue))
                                    .frame(width: 14, alignment: .center)

                                VStack(alignment: .leading, spacing: 2) {
                                    Text(issue.message)
                                        .font(.caption)
                                        .foregroundStyle(.primary)
                                    Text(locationText(for: issue))
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                }
                            }
                        }

                        if issues.count > maxVisibleIssues {
                            Text("\(issues.count - maxVisibleIssues) more issues not shown")
                                .font(.caption2)
                                .foregroundStyle(.secondary)
                        }
                    }
                    .padding(.top, 2)
                }
            }
            .padding(.horizontal, 2)
            .onChange(of: issueSignature) { _, _ in
                if issues.isEmpty {
                    isExpanded = false
                } else if isExpanded {
                    isExpanded = true
                }
            }
        }
    }

    private var summaryTitle: String {
        if isValidating, issues.isEmpty {
            return "Validating YAML against Kubernetes…"
        }

        if errorCount > 0, warningCount > 0 {
            return "\(errorCount) errors, \(warningCount) warnings"
        }
        if errorCount > 0 {
            return errorCount == 1 ? "1 YAML error" : "\(errorCount) YAML errors"
        }
        if warningCount > 0 {
            return warningCount == 1 ? "1 validation warning" : "\(warningCount) validation warnings"
        }
        return "Validating YAML against Kubernetes…"
    }

    private var summaryColor: Color {
        if issues.contains(where: { $0.severity == .error }) {
            return .red
        }
        return .orange
    }

    private func symbolName(for issue: YAMLValidationIssue) -> String {
        issue.severity == .error ? "xmark.octagon.fill" : "exclamationmark.triangle.fill"
    }

    private func color(for issue: YAMLValidationIssue) -> Color {
        issue.severity == .error ? .red : .orange
    }

    private func locationText(for issue: YAMLValidationIssue) -> String {
        let source = issue.source.rawValue.capitalized

        switch (issue.line, issue.column) {
        case let (line?, column?):
            return "\(source) • line \(line), column \(column)"
        case let (line?, nil):
            return "\(source) • line \(line)"
        default:
            return source
        }
    }

    private func issueBadge(systemName: String, text: String, color: Color) -> some View {
        HStack(spacing: 4) {
            Image(systemName: systemName)
                .font(.caption2)
            Text(text)
                .font(.caption2.weight(.semibold))
        }
        .foregroundStyle(color)
        .padding(.horizontal, 6)
        .padding(.vertical, 3)
        .background(color.opacity(0.10), in: Capsule())
    }
}
