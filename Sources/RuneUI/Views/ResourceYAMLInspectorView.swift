import SwiftUI

struct ResourceYAMLEditorSurface: View {
    @Binding var text: String
    let displayText: String
    let readOnlyResetID: String
    let inlineEditing: Bool
    let implementation: ManifestInlineEditorImplementation

    var body: some View {
        let activeImplementation = inlineEditing ? implementation : .readOnlyScroll

        InspectorTextSurface(minHeight: 280) {
            Group {
                switch activeImplementation {
                case .readOnlyScroll:
                    InspectorReadOnlyTextView(
                        text: displayText,
                        resetID: readOnlyResetID,
                        contentStyle: .yaml
                    )
                case .swiftUITextEditor:
                    TextEditor(text: $text)
                        .font(.system(size: 12, weight: .regular, design: .monospaced))
                        .scrollContentBackground(.hidden)
                        .padding(6)
                case .appKitTextView:
                    AppKitManifestTextView(text: $text, isEditable: true, contentStyle: .yaml)
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

            ResourceYAMLEditorSurface(
                text: $yamlText,
                displayText: yamlDisplayText,
                readOnlyResetID: readOnlyResetID,
                inlineEditing: isInlineEditing,
                implementation: inlineEditorImplementation
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

            ResourceYAMLEditorSurface(
                text: $yamlText,
                displayText: yamlText,
                readOnlyResetID: "yaml-sheet:\(resourceReference)",
                inlineEditing: true,
                implementation: .appKitTextView
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
