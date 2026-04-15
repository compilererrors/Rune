import Foundation

public enum RuneError: LocalizedError, Sendable {
    case missingKubeConfig
    case commandFailed(command: String, message: String)
    case parseError(message: String)
    case readOnlyMode
    case invalidInput(message: String)
    case userCancelled

    public var errorDescription: String? {
        switch self {
        case .missingKubeConfig:
            return "Ingen kubeconfig vald."
        case let .commandFailed(command, message):
            return "Kommandot \(command) misslyckades: \(message)"
        case let .parseError(message):
            return "Kunde inte tolka data: \(message)"
        case .readOnlyMode:
            return "Read-only mode är aktivt. Write-actions är blockerade."
        case let .invalidInput(message):
            return "Ogiltig inmatning: \(message)"
        case .userCancelled:
            return "Åtgärden avbröts."
        }
    }
}
