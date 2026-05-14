import AppKit
import Foundation

public protocol KubeConfigPicking {
    @MainActor
    func pickFiles() throws -> [URL]

    @MainActor
    func pickFolder() throws -> URL?

    @MainActor
    func pickDefaultKubeConfig(at defaultURL: URL) throws -> URL?
}

public extension KubeConfigPicking {
    @MainActor
    func pickFolder() throws -> URL? {
        nil
    }
}

public final class OpenPanelKubeConfigPicker: KubeConfigPicking {
    public init() {}

    @MainActor
    public func pickFiles() throws -> [URL] {
        let panel = NSOpenPanel()
        panel.allowsMultipleSelection = true
        panel.canChooseDirectories = false
        panel.canChooseFiles = true
        panel.prompt = "Importera"

        let result = panel.runModal()
        guard result == .OK else {
            return []
        }

        return panel.urls
    }

    @MainActor
    public func pickFolder() throws -> URL? {
        let panel = NSOpenPanel()
        panel.allowsMultipleSelection = false
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.prompt = "Add Folder"
        panel.message = "Select a folder containing kubeconfig files."

        let result = panel.runModal()
        guard result == .OK else {
            return nil
        }

        return panel.url
    }

    @MainActor
    public func pickDefaultKubeConfig(at defaultURL: URL) throws -> URL? {
        let panel = NSOpenPanel()
        panel.allowsMultipleSelection = false
        panel.canChooseDirectories = false
        panel.canChooseFiles = true
        panel.directoryURL = defaultURL.deletingLastPathComponent()
        panel.nameFieldStringValue = defaultURL.lastPathComponent
        panel.showsHiddenFiles = true
        panel.prompt = "Use Config"
        panel.message = "Select your kubeconfig file to grant Rune access."

        let result = panel.runModal()
        guard result == .OK else {
            return nil
        }

        return panel.url
    }
}
