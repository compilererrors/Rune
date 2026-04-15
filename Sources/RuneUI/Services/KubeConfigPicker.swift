import AppKit
import Foundation
import UniformTypeIdentifiers

public protocol KubeConfigPicking {
    @MainActor
    func pickFiles() throws -> [URL]
}

public final class OpenPanelKubeConfigPicker: KubeConfigPicking {
    public init() {}

    @MainActor
    public func pickFiles() throws -> [URL] {
        let panel = NSOpenPanel()
        panel.allowsMultipleSelection = true
        panel.canChooseDirectories = false
        panel.canChooseFiles = true
        panel.allowedContentTypes = ["yaml", "yml", "config"].compactMap { UTType(filenameExtension: $0) }
        panel.prompt = "Importera"

        let result = panel.runModal()
        guard result == .OK else {
            return []
        }

        return panel.urls
    }
}
