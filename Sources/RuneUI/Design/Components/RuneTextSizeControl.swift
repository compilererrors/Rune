import SwiftUI
import RuneCore

/// Smaller and bigger text for the monospaced views (the terminal, logs,
/// YAML and Describe), as in Safari and Xcode: a small and a large "A" side
/// by side. One point a step within Settings' limits; ⌘−, ⌘+ and ⌘0 do the
/// same from the keyboard. Every monospaced view shares the size.
struct RuneTextSizeControl: View {
    @AppStorage(RuneSettingsKeys.terminalFontSize) private var size = RuneSettingsKeys.terminalFontSizeDefault

    var body: some View {
        HStack(spacing: 0) {
            Button { step(.smallerText) } label: {
                Image(systemName: "textformat.size.smaller")
            }
            .buttonStyle(RuneCompactButtonStyle())
            .disabled(!Self.canStep(.smallerText, from: size))
            .help("Smaller Text (⌘−)")
            .accessibilityLabel("Smaller text")

            Button { step(.biggerText) } label: {
                Image(systemName: "textformat.size.larger")
            }
            .buttonStyle(RuneCompactButtonStyle())
            .disabled(!Self.canStep(.biggerText, from: size))
            .help("Bigger Text (⌘+)")
            .accessibilityLabel("Bigger text")
        }
        .fixedSize()
        .accessibilityElement(children: .contain)
        .accessibilityLabel("Text size")
        .accessibilityValue("\(Int(size)) points")
    }

    private func step(_ command: TerminalKeyCommand) {
        TerminalKeyCommand.stepSharedTextSize(command)
    }

    static func canStep(_ command: TerminalKeyCommand, from size: Double) -> Bool {
        TerminalKeyCommand.textSize(after: command, from: size) != RuneSettingsKeys.clampedTerminalFontSize(size)
    }
}
