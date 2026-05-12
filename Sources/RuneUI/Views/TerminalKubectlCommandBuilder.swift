import AppKit
import Foundation
import RuneCore

enum TerminalKubectlCommandBuilder {
    static func exec(
        contextName: String?,
        namespace: String,
        podName: String,
        containerName: String? = nil,
        command: String
    ) -> String {
        var parts = base(contextName: contextName, namespace: namespace)
        parts += ["exec", "-it", podName]
        let trimmedContainer = containerName?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if !trimmedContainer.isEmpty {
            parts += ["--container", trimmedContainer]
        }
        parts += ["--", "sh", "-lc", command]
        return shellCommand(parts)
    }

    static func portForward(
        contextName: String?,
        namespace: String,
        targetKind: PortForwardTargetKind,
        targetName: String,
        localPort: Int,
        remotePort: Int,
        address: String
    ) -> String {
        portForward(
            contextName: contextName,
            namespace: namespace,
            targetKind: targetKind,
            targetName: targetName,
            localPort: String(localPort),
            remotePort: String(remotePort),
            address: address
        )
    }

    static func portForward(
        contextName: String?,
        namespace: String,
        targetKind: PortForwardTargetKind,
        targetName: String,
        localPort: String,
        remotePort: String,
        address: String
    ) -> String {
        var parts = base(contextName: contextName, namespace: namespace)
        parts += ["port-forward", "--address", address, "\(targetKind.kubernetesResourcePathName)/\(targetName)", "\(localPort):\(remotePort)"]
        return shellCommand(parts)
    }

    static func copyToPasteboard(_ value: String) {
        let pasteboard = NSPasteboard.general
        pasteboard.clearContents()
        pasteboard.setString(value, forType: .string)
    }

    private static func base(contextName: String?, namespace: String) -> [String] {
        var parts = ["kubectl"]
        let trimmedContext = contextName?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if !trimmedContext.isEmpty {
            parts += ["--context", trimmedContext]
        }
        let trimmedNamespace = namespace.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmedNamespace.isEmpty {
            parts += ["--namespace", trimmedNamespace]
        }
        return parts
    }

    private static func shellCommand(_ parts: [String]) -> String {
        parts.map(shellQuote).joined(separator: " ")
    }

    private static func shellQuote(_ value: String) -> String {
        guard !value.isEmpty else { return "''" }
        if value.rangeOfCharacter(from: CharacterSet(charactersIn: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_./:=+-").inverted) == nil {
            return value
        }
        return "'" + value.replacingOccurrences(of: "'", with: "'\\''") + "'"
    }
}
