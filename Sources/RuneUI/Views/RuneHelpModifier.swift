import SwiftUI

extension View {
    @ViewBuilder
    func runeHelp(_ text: String?, enabled: Bool) -> some View {
        if enabled,
           let text = text?.trimmingCharacters(in: .whitespacesAndNewlines),
           !text.isEmpty {
            self.help(text)
        } else {
            self
        }
    }
}
