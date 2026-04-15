import SwiftUI

public enum RuneUIExports {
    @MainActor
    public static func makeRootView() -> some View {
        RuneRootView()
    }
}
