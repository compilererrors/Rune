import AppKit
import Foundation
import UniformTypeIdentifiers

public enum GKECredentialFileSelectionError: LocalizedError, Sendable {
    case unreadable
    case invalidSize

    public var errorDescription: String? {
        switch self {
        case .unreadable:
            return "The Google service-account JSON could not be read."
        case .invalidSize:
            return "The Google service-account JSON must be between 1 byte and 1 MiB."
        }
    }
}

public enum GKECredentialFileSelection: Sendable {
    case selected(Data)
    case cancelled
    case failed(GKECredentialFileSelectionError)
}

@MainActor
public protocol GKECredentialFilePicking: AnyObject {
    func beginSelection(
        completion: @escaping @MainActor (GKECredentialFileSelection) -> Void
    )

    func cancelSelection()
}

@MainActor
public final class OpenPanelGKECredentialFilePicker: GKECredentialFilePicking {
    private static let maximumCredentialBytes = 1_048_576
    private var panel: NSOpenPanel?

    public init() {}

    public func beginSelection(
        completion: @escaping @MainActor (GKECredentialFileSelection) -> Void
    ) {
        guard panel == nil else {
            completion(.failed(.unreadable))
            return
        }

        let panel = NSOpenPanel()
        panel.title = "Choose Google service-account JSON"
        panel.prompt = "Import"
        panel.allowedContentTypes = [.json]
        panel.allowsMultipleSelection = false
        panel.canChooseDirectories = false
        panel.canChooseFiles = true
        self.panel = panel

        panel.begin { [weak self] response in
            guard let self, self.panel === panel else { return }
            self.panel = nil
            guard response == .OK, let url = panel.url else {
                completion(.cancelled)
                return
            }
            completion(Self.readCredentialData(from: url))
        }
    }

    public func cancelSelection() {
        guard let panel else { return }
        self.panel = nil
        panel.cancel(nil)
    }

    private static func readCredentialData(from url: URL) -> GKECredentialFileSelection {
        let accessed = url.startAccessingSecurityScopedResource()
        defer {
            if accessed {
                url.stopAccessingSecurityScopedResource()
            }
        }

        do {
            let handle = try FileHandle(forReadingFrom: url)
            defer { try? handle.close() }
            let data = try handle.read(upToCount: maximumCredentialBytes + 1) ?? Data()
            guard !data.isEmpty, data.count <= maximumCredentialBytes else {
                return .failed(.invalidSize)
            }
            return .selected(data)
        } catch {
            return .failed(.unreadable)
        }
    }
}

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
        panel.prompt = "Import"

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
