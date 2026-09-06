import Foundation
import SwiftUI

enum ManifestDocumentState: Equatable {
    case ready
    case stale(title: String, message: String)
    case loading(title: String, message: String? = nil)
    case failure(title: String, message: String)
    case empty(title: String, message: String)

    static func resolved(
        content: String,
        isLoading: Bool,
        error: String?,
        loadingTitle: String,
        loadingMessage: String,
        failureTitle: String,
        emptyTitle: String,
        emptyMessage: String
    ) -> ManifestDocumentState {
        let hasContent = !content.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        let normalizedError = error?.trimmingCharacters(in: .whitespacesAndNewlines)

        if hasContent {
            if let normalizedError, !normalizedError.isEmpty {
                return .stale(title: failureTitle, message: normalizedError)
            }
            return .ready
        }
        if isLoading {
            return .loading(title: loadingTitle, message: loadingMessage)
        }
        if let normalizedError, !normalizedError.isEmpty {
            return .failure(title: failureTitle, message: normalizedError)
        }
        return .empty(title: emptyTitle, message: emptyMessage)
    }

    fileprivate var contentState: RuneContentState? {
        switch self {
        case .ready, .stale:
            return nil
        case let .loading(title, message):
            return .loading(title: title, message: message)
        case let .failure(title, message):
            return .retryableError(title: title, message: message)
        case let .empty(title, message):
            return .empty(title: title, message: message)
        }
    }

    var staleContentState: RuneContentState? {
        guard case let .stale(title, message) = self else { return nil }
        return .retryableError(title: title, message: message)
    }
}

struct ManifestDocumentSurface<Content: View>: View {
    let state: ManifestDocumentState
    @ViewBuilder let content: Content

    var body: some View {
        content
            .opacity(state.contentState == nil ? 1 : 0)
            .accessibilityHidden(state.contentState != nil)
            .allowsHitTesting(state.contentState == nil)
            .overlay {
                if let contentState = state.contentState {
                    RuneContentStateView(contentState)
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                        .background(Color(nsColor: .textBackgroundColor))
                        .accessibilityIdentifier("manifestDocumentState")
                }
            }
    }
}

struct ManifestActionToolbar<PrimaryActions: View, SecondaryActions: View>: View {
    let applyTitle: String
    let canApply: Bool
    let applyHelp: String
    let statusText: String?
    let onApply: () -> Void
    @ViewBuilder let primaryActions: PrimaryActions
    @ViewBuilder let secondaryActions: SecondaryActions

    init(
        applyTitle: String,
        canApply: Bool,
        applyHelp: String,
        statusText: String? = nil,
        onApply: @escaping () -> Void,
        @ViewBuilder primaryActions: () -> PrimaryActions,
        @ViewBuilder secondaryActions: () -> SecondaryActions
    ) {
        self.applyTitle = applyTitle
        self.canApply = canApply
        self.applyHelp = applyHelp
        self.statusText = statusText
        self.onApply = onApply
        self.primaryActions = primaryActions()
        self.secondaryActions = secondaryActions()
    }

    var body: some View {
        ManifestToolbarScrollRow {
            ManifestToolbarGroup(role: .action) {
                Button(applyTitle, action: onApply)
                    .buttonStyle(RuneToolbarButtonStyle(isProminent: true))
                    .disabled(!canApply)
                    .help(applyHelp)

                primaryActions
            }

            ManifestToolbarGroup(role: .action) {
                secondaryActions

                if let statusText {
                    ManifestStatusChip(text: statusText, systemImage: "clock")
                }
            }
        }
    }
}

struct ManifestEditorUndoButton: View {
    let canUndo: Bool
    let onUndo: () -> Void

    var body: some View {
        Button(action: onUndo) {
            Label("Undo", systemImage: "arrow.uturn.backward")
        }
        .buttonStyle(RuneToolbarButtonStyle())
        .disabled(!canUndo)
        .help(canUndo ? "Undo the last local YAML edit (⌘Z)." : "No local YAML edit to undo.")
        .keyboardShortcut("z", modifiers: [.command])
        .accessibilityIdentifier("manifest-editor-undo")
    }
}

struct ManifestYAMLActionMenus: View {
    let draftTitle: String
    let fileTitle: String
    let yamlTextIsEmpty: Bool
    let hasUnsavedEdits: Bool
    let canReapplySnapshot: Bool
    let onReapplySnapshot: () -> Void
    let onRevert: () -> Void
    let onImport: () -> Void
    let onExport: () -> Void
    let onExportToExportFolder: () -> Void
    let onExportAndOpen: () -> Void

    var body: some View {
        RuneToolbarMenu {
            Button("Apply Last Fetched YAML", action: onReapplySnapshot)
                .disabled(!canReapplySnapshot)
                .help("Apply the last YAML fetched for this resource again. Rune shows a confirmation and diff before sending it.")

            Divider()

            Button("Revert Draft", action: onRevert)
                .disabled(!hasUnsavedEdits)
                .help("Discard local YAML edits and return to the current loaded draft.")
        } label: {
            Label(draftTitle, systemImage: "clock.arrow.circlepath")
                .runeInterfaceFont(relativeSize: -1, weight: .medium)
        }

        RuneToolbarMenu {
            Button("Import YAML…", action: onImport)
                .help("Replace the editor with the contents of a YAML file.")

            Button("Export YAML…", action: onExport)
                .disabled(yamlTextIsEmpty)
                .help("Export the current YAML text to a file.")

            Button("Save YAML to Export Folder", action: onExportToExportFolder)
                .disabled(yamlTextIsEmpty)
                .help("Save the current YAML text to the configured export folder.")

            Button("Save YAML and Open", action: onExportAndOpen)
                .disabled(yamlTextIsEmpty)
                .help("Save the current YAML text to the configured export folder and open it.")
        } label: {
            Label(fileTitle, systemImage: "doc")
                .runeInterfaceFont(relativeSize: -1, weight: .medium)
        }
    }
}

struct ManifestToolbarScrollRow<Content: View>: View {
    @ViewBuilder let content: Content
    @Environment(\.dynamicTypeSize) private var dynamicTypeSize
    @Environment(\.runeInterfaceFontSize) private var interfaceFontSize

    var body: some View {
        ScrollView(.horizontal, showsIndicators: true) {
            HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarGroupSpacing) {
                content
            }
            .fixedSize(horizontal: true, vertical: false)
        }
        .runeInterfaceFont(relativeSize: -1, weight: .medium)
        .controlSize(usesRegularControls ? .regular : .small)
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var usesRegularControls: Bool {
        dynamicTypeSize.isAccessibilitySize
            || interfaceFontSize > RuneInterfaceTypography.standardMenuFontSize + 1
    }
}

enum ManifestToolbarGroupRole {
    case action
    case source

    var height: CGFloat {
        switch self {
        case .action:
            return RuneUILayoutMetrics.inspectorToolbarActionGroupHeight
        case .source:
            return RuneUILayoutMetrics.inspectorToolbarSourceGroupHeight
        }
    }

    var verticalPadding: CGFloat {
        switch self {
        case .action:
            return RuneUILayoutMetrics.inspectorControlSurfaceVerticalPadding
        case .source:
            return RuneUILayoutMetrics.inspectorToolbarGroupVerticalPadding
        }
    }
}

struct ManifestToolbarGroup<Content: View>: View {
    var role: ManifestToolbarGroupRole = .action
    @ViewBuilder let content: Content

    var body: some View {
        HStack(alignment: .center, spacing: RuneUILayoutMetrics.inspectorToolbarControlSpacing) {
            content
        }
        .padding(.horizontal, RuneUILayoutMetrics.inspectorToolbarGroupHorizontalPadding)
        .padding(.vertical, role.verticalPadding)
        .frame(minHeight: role.height)
        .background {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .fill(Color.primary.opacity(0.04))
        }
        .overlay {
            RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                .strokeBorder(Color(nsColor: .separatorColor).opacity(0.14), lineWidth: 1)
        }
        .fixedSize(horizontal: true, vertical: false)
    }
}

struct ManifestStatusChip: View {
    let text: String
    var systemImage = "clock"

    var body: some View {
        Label(text, systemImage: systemImage)
            .font(.caption.weight(.semibold))
            .foregroundStyle(.runeSecondary)
            .lineLimit(1)
            .truncationMode(.middle)
            .monospacedDigit()
            .padding(.horizontal, 9)
            .padding(.vertical, 5)
            .background {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                    .fill(Color.primary.opacity(0.035))
            }
            .overlay {
                RoundedRectangle(cornerRadius: RuneUILayoutMetrics.interactiveRowCornerRadius, style: .continuous)
                    .strokeBorder(Color(nsColor: .separatorColor).opacity(0.14), lineWidth: 1)
            }
            .fixedSize(horizontal: false, vertical: true)
    }
}
