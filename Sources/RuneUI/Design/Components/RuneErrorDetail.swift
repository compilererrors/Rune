import AppKit
import RuneCore
import SwiftUI

/// When an error message folds and when Rune asks to always expand them.
enum RuneErrorDetailPolicy {
    /// A message longer than this, or with more than one line, shows its first line until it is opened.
    static let foldingCharacterCount = 120
    /// Opening a message by hand this many times makes Rune ask, once, whether to always expand them.
    static let expandsBeforeAsking = 3

    static func isFoldable(_ message: String) -> Bool {
        message.count > foldingCharacterCount || message.contains(where: \.isNewline)
    }

    static func firstLine(_ message: String) -> String {
        message.split(whereSeparator: \.isNewline).first.map(String.init) ?? message
    }

    static func shouldAskToAlwaysExpand(expandCount: Int, alreadyAsked: Bool, alwaysExpanded: Bool) -> Bool {
        !alreadyAsked && !alwaysExpanded && expandCount >= expandsBeforeAsking
    }
}

/// The error a server returned, word for word, under Rune's own line about it.
///
/// A long message shows its first line and opens with the chevron; Copy always copies the
/// whole message, status code included. The text is selectable. Settings > Diagnostics can
/// keep every message open, and Rune offers that after someone has opened a few by hand.
struct RuneErrorDetail: View {
    let message: String

    @AppStorage(RuneSettingsKeys.errorDetailsAlwaysExpanded) private var alwaysExpanded = false
    @AppStorage(RuneSettingsKeys.errorDetailsExpandCount) private var expandCount = 0
    @AppStorage(RuneSettingsKeys.errorDetailsAlwaysExpandAsked) private var alwaysExpandAsked = false
    @State private var isExpanded = false
    @State private var isAskingToAlwaysExpand = false

    init(_ message: String) {
        self.message = message.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private var isFoldable: Bool { RuneErrorDetailPolicy.isFoldable(message) }
    private var showsWholeMessage: Bool { !isFoldable || alwaysExpanded || isExpanded }

    var body: some View {
        HStack(alignment: .top, spacing: RuneSpacing.xxs) {
            if isFoldable && !alwaysExpanded {
                RuneIconButton(
                    isExpanded ? "Show less of the error" : "Show the whole error",
                    systemImage: isExpanded ? "chevron.down" : "chevron.right",
                    action: toggle
                )
            }

            Text(showsWholeMessage ? message : RuneErrorDetailPolicy.firstLine(message))
                .runeText(.micro, design: .monospaced)
                .foregroundStyle(.runeSecondary)
                .lineLimit(showsWholeMessage ? nil : 1)
                .truncationMode(.tail)
                .fixedSize(horizontal: false, vertical: showsWholeMessage)
                .textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading)
                .accessibilityLabel("Error message: \(message)")

            RuneIconButton("Copy error", systemImage: "doc.on.doc", help: "Copy the whole error message") {
                NSPasteboard.general.clearContents()
                NSPasteboard.general.setString(message, forType: .string)
            }
        }
        .alert("Always expand error messages?", isPresented: $isAskingToAlwaysExpand) {
            Button("Always Expand") { alwaysExpanded = true }
            Button("Not Now", role: .cancel) {}
        } message: {
            Text("Rune will show the whole error every time. You can change this in Settings > Diagnostics.")
        }
    }

    private func toggle() {
        isExpanded.toggle()
        guard isExpanded, !alwaysExpandAsked else { return }
        expandCount += 1
        if RuneErrorDetailPolicy.shouldAskToAlwaysExpand(
            expandCount: expandCount,
            alreadyAsked: alwaysExpandAsked,
            alwaysExpanded: alwaysExpanded
        ) {
            alwaysExpandAsked = true
            isAskingToAlwaysExpand = true
        }
    }
}
