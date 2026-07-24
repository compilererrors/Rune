import SwiftUI

enum AddClusterProviderCredentialFieldMetrics {
    static let labelToControlSpacing: CGFloat = 4
    static let inlineLabelSpacing: CGFloat = 8
    static let stackedLabelSpacing: CGFloat = 2
    static let supportedCompactWidth: CGFloat = 400
}

extension AddClusterProviderField {
    var requirementTitle: String {
        isRequired ? "Required" : "Optional"
    }

    var accessibilityRequirementHint: String {
        isRequired ? "Required field" : "Optional field"
    }

    var accessibilityIdentifier: String {
        "rune.add-cluster.provider-field.\(id.rawValue)"
    }
}

/// Keeps provider field names and their requirement visible after a value is entered.
/// The compact fallback also prevents longer labels from squeezing the control at
/// enlarged text sizes.
struct AddClusterProviderCredentialField<Content: View>: View {
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize

    private let titleText: String
    private let requirementTitle: String
    private let accessibilityRequirementHint: String
    private let fieldAccessibilityIdentifier: String
    private let content: Content

    init(
        field: AddClusterProviderField,
        @ViewBuilder content: () -> Content
    ) {
        titleText = field.title
        requirementTitle = field.requirementTitle
        accessibilityRequirementHint = field.accessibilityRequirementHint
        fieldAccessibilityIdentifier = field.accessibilityIdentifier
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
        self.content = content()
    }

    var body: some View {
        VStack(alignment: .leading, spacing: AddClusterProviderCredentialFieldMetrics.labelToControlSpacing) {
            Group {
                if dynamicTypeSize.isAccessibilitySize {
                    stackedLabel
                } else {
                    ViewThatFits(in: .horizontal) {
                        inlineLabel
                        stackedLabel
                    }
                }
            }
            .accessibilityHidden(true)

            content
                .accessibilityLabel(titleText)
                .accessibilityHint(accessibilityRequirementHint)
                .accessibilityIdentifier(fieldAccessibilityIdentifier)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var inlineLabel: some View {
        HStack(alignment: .firstTextBaseline, spacing: AddClusterProviderCredentialFieldMetrics.inlineLabelSpacing) {
            title
            Spacer(minLength: AddClusterProviderCredentialFieldMetrics.inlineLabelSpacing)
            requirement
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
            .font(.caption.weight(.semibold))
            .fixedSize(horizontal: false, vertical: true)
    }

    private var requirement: some View {
        Text(requirementTitle)
            .font(.caption2.weight(.medium))
            .foregroundStyle(.secondary)
            .fixedSize(horizontal: false, vertical: true)
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
            }
        } else {
            AddClusterProviderCredentialField(field: field) {
                TextField("", text: $text)
                    .textFieldStyle(.roundedBorder)
            }
        }
    }
}
