import AppKit
import Foundation
import UniformTypeIdentifiers

public protocol FileExporting {
    @MainActor
    func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL
}

public final class SavePanelExporter: FileExporting {
    public init() {}

    @MainActor
    public func save(data: Data, suggestedName: String, allowedFileTypes: [String]) throws -> URL {
        let panel = NSSavePanel()
        panel.canCreateDirectories = true
        panel.nameFieldStringValue = suggestedName
        panel.allowedContentTypes = allowedFileTypes.compactMap { UTType(filenameExtension: $0) }

        let result = panel.runModal()
        guard result == .OK, let destination = panel.url else {
            throw NSError(domain: "RuneExport", code: 1, userInfo: [NSLocalizedDescriptionKey: "The user cancelled the save operation."])
        }

        try data.write(to: destination)
        return destination
    }
}
