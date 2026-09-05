import SwiftUI

enum AddClusterProviderCredentialFieldMetrics {
    static let labelToControlSpacing: CGFloat = 4
    static let stackedLabelSpacing: CGFloat = 2
    static let standardLabelRowHeight: CGFloat = 18
    static let supportedCompactWidth: CGFloat = 400
}

extension AddClusterProviderField {
    var requirementTitle: String? {
        switch requirement {
        case .required:
            return "Required"
        case .optional:
            return "Optional"
        case .requiredForOptionalMethod:
            return nil
        }
    }

    var accessibilityRequirementHint: String {
        let requirementHint: String
        switch requirement {
        case .required:
            requirementHint = "Required field"
        case .optional:
            requirementHint = "Optional field"
        case .requiredForOptionalMethod:
            requirementHint = "Needed only when using this optional import method"
        }
        return [requirementHint, helpText].compactMap { $0 }.joined(separator: ". ")
    }

    var accessibilityIdentifier: String {
        "rune.add-cluster.provider-field.\(id.rawValue)"
    }
}

/// Keeps provider field names visible after a value is entered and reserves one
/// consistent label block for aligned controls. Conditional requirements are
/// explained once by their optional method instead of repeated on every field.
struct AddClusterProviderCredentialField<Content: View>: View {
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @Environment(\.runeInterfaceFontSize) private var interfaceFontSize

    private let titleText: String
    private let requirementTitle: String?
    private let accessibilityRequirementHint: String
    private let fieldAccessibilityIdentifier: String
    private let helpText: String?
    private let content: Content

    init(
        field: AddClusterProviderField,
        @ViewBuilder content: () -> Content
    ) {
        titleText = field.title
        requirementTitle = field.requirementTitle
        accessibilityRequirementHint = field.accessibilityRequirementHint
        fieldAccessibilityIdentifier = field.accessibilityIdentifier
        helpText = field.helpText
        self.content = content()
    }

    init(
        title: String,
        isRequired: Bool,
        accessibilityIdentifier: String,
        @ViewBuilder content: () -> Content
    ) {
        titleText = title
        requirementTitle = isRequired ? "Required" : "Optional"
        accessibilityRequirementHint = isRequired ? "Required field" : "Optional field"
        fieldAccessibilityIdentifier = accessibilityIdentifier
        helpText = nil
        self.content = content()
    }

    var body: some View {
        VStack(alignment: .leading, spacing: AddClusterProviderCredentialFieldMetrics.labelToControlSpacing) {
            Group {
                if dynamicTypeSize.isAccessibilitySize {
                    stackedLabel
                } else {
                    inlineLabel
                        .frame(minHeight: max(
                            AddClusterProviderCredentialFieldMetrics.standardLabelRowHeight,
                            interfaceFontSize + 5
                        ))
                }
            }
            .accessibilityHidden(true)

            content
                .accessibilityLabel(titleText)
                .accessibilityHint(accessibilityRequirementHint)
                .accessibilityIdentifier(fieldAccessibilityIdentifier)

            if let helpText {
                Text(helpText)
                    .runeInterfaceFont(relativeSize: -2)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                    .accessibilityHidden(true)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var inlineLabel: some View {
        HStack(alignment: .firstTextBaseline, spacing: 6) {
            title
            Spacer(minLength: 4)
            if let requirementTitle {
                requirementText(requirementTitle)
            }
        }
    }

    private var stackedLabel: some View {
        VStack(alignment: .leading, spacing: AddClusterProviderCredentialFieldMetrics.stackedLabelSpacing) {
            title
            requirement
        }
    }

    private var title: some View {
        Text(titleText)
            .runeInterfaceFont(relativeSize: -1, weight: .semibold)
            .lineLimit(dynamicTypeSize.isAccessibilitySize ? nil : 1)
            .minimumScaleFactor(0.85)
    }

    @ViewBuilder
    private var requirement: some View {
        if let requirementTitle {
            requirementText(requirementTitle)
        }
    }

    private func requirementText(_ value: String) -> some View {
        Text(value)
            .runeInterfaceFont(relativeSize: -2, weight: .medium)
            .foregroundStyle(.secondary)
            .lineLimit(dynamicTypeSize.isAccessibilitySize ? nil : 1)
    }
}

/// The production provider form grid, shared with rendered layout checks.
struct AddClusterProviderCredentialGrid<Content: View>: View {
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @Environment(\.runeInterfaceFontSize) private var interfaceFontSize

    let fields: [AddClusterProviderField]
    @ViewBuilder var input: (AddClusterProviderField) -> Content

    var body: some View {
        LazyVGrid(
            columns: columns,
            alignment: .leading,
            spacing: RuneUILayoutMetrics.dialogControlSpacing
        ) {
            ForEach(fields) { field in
                input(field)
            }
        }
        .runeInterfaceFont()
        .runeInterfaceControlSize()
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var columns: [GridItem] {
        if dynamicTypeSize.isAccessibilitySize || interfaceFontSize >= 16 {
            return [GridItem(.flexible(), spacing: RuneUILayoutMetrics.dialogControlSpacing, alignment: .top)]
        }
        return [GridItem(
            .adaptive(minimum: RuneAddClusterProviderActionLayout.minimumCredentialFieldWidth),
            spacing: RuneUILayoutMetrics.dialogControlSpacing,
            alignment: .top
        )]
    }
}

/// Renders ordinary identifiers as `TextField` and credential material as
/// `SecureField`, based on the provider presentation contract.
struct AddClusterProviderCredentialTextInput: View {
    let field: AddClusterProviderField
    @Binding var text: String

    init(field: AddClusterProviderField, text: Binding<String>) {
        precondition(
            field.input != .sensitiveJSONFile,
            "Sensitive file credentials require a user-mediated file picker."
        )
        self.field = field
        _text = text
    }

    @ViewBuilder
    var body: some View {
        if field.input == .secureText {
            AddClusterProviderCredentialField(field: field) {
                SecureField("", text: $text)
                    .textFieldStyle(.roundedBorder)
                    .runeTextInputCursor()
            }
        } else {
            AddClusterProviderCredentialField(field: field) {
                TextField("", text: $text)
                    .textFieldStyle(.roundedBorder)
                    .runeTextInputCursor()
            }
        }
    }
}
