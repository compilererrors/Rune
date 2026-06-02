import OSLog

public enum RuneLoggers {
    private static let subsystem = RuneApplicationIdentifiers.bundleIdentifier

    public static let app = Logger(subsystem: subsystem, category: "app")
    public static let diagnostics = Logger(subsystem: subsystem, category: "diagnostics")
    public static let layout = Logger(subsystem: subsystem, category: "layout")
    public static let kubernetesExec = Logger(subsystem: subsystem, category: "kubernetes-exec")
}
