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
            return "No kubeconfig selected."
        case let .commandFailed(command, message):
            return "Command failed: \(command): \(message)"
        case let .parseError(message):
            return "Could not parse data: \(message)"
        case .readOnlyMode:
            return "Read-only mode is on; write actions are blocked."
        case let .invalidInput(message):
            return "Invalid input: \(message)"
        case .userCancelled:
            return "The action was cancelled."
        }
    }
}
