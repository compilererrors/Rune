import AppKit
import SwiftUI
import RuneCore

/// When a shell connects. Picking a pod (or opening a tab for one) connects
/// at once, except where opening a shell should stay a deliberate click:
/// read-only mode and production contexts. There, and whenever a tab is not
/// connected, the terminal itself shows the Connect action.
enum TerminalShellConnectPolicy {
    static func autoConnects(isReadOnly: Bool, isProduction: Bool, canApplyMutations: Bool) -> Bool {
        !isReadOnly && !isProduction && canApplyMutations
    }
}

struct TerminalShellPanelView: View {
    let session: PodTerminalSession?
    let sessions: [PodTerminalSession]
    let activeSessionID: String?
    let isComposingNewSession: Bool
    let selectedPod: PodSummary?
    let availablePods: [PodSummary]
    let canApplyMutations: Bool
    let transcriptHeight: CGFloat
    @Binding var selectedShellPodID: String
    @Binding var terminalInput: String
    let onStartSession: (PodSummary, String?) -> Void
    let onReconnectSession: (PodTerminalSession, PodSummary, String?) -> Void
    let onSend: () -> Void
    let onSendControlSequence: (String) -> Void
    let onResizeSession: (String, Int, Int) -> Void
    let onDisconnect: () -> Void
    let onSelectSession: (String) -> Void
    let onCloseSession: (String) -> Void
    let onComposeNewSession: () -> Void
    let onClearTranscript: () -> Void
    let onSaveActiveTranscript: () -> Void
    let onSaveAllTranscripts: () -> Void
    let onSaveActiveTranscriptToExportFolder: () -> Void
    let onSaveActiveTranscriptAndOpen: () -> Void
    let onSaveAllTranscriptsToExportFolder: () -> Void
    let onSaveAllTranscriptsAndOpen: () -> Void
    let isFavoritePod: (PodSummary) -> Bool
    let onToggleFavoritePod: (PodSummary) -> Void
    var onOpenSelectedPodLogs: ((PodSummary) -> Void)? = nil
    var autoConnects = false
    @AppStorage(RuneSettingsKeys.terminalFontSize) private var storedTerminalFontSize = RuneSettingsKeys.terminalFontSizeDefault
    @Environment(\.runeThemePalette) private var runeThemePalette
    @Environment(\.colorScheme) private var colorScheme
    @State private var isInputFocused = false
    @State private var commandHistory: [String] = []
    @State private var commandHistoryIndex: Int?
    @State private var pendingMultilinePasteConfirmation = false
    @State private var selectedTerminalContainerName = ""

    private var terminalFontSize: CGFloat {
        CGFloat(RuneSettingsKeys.clampedTerminalFontSize(storedTerminalFontSize))
    }

    private var terminalPromptLayout: TerminalPromptLayoutMetrics {
        TerminalPromptLayoutMetrics(fontSize: terminalFontSize)
    }

    private var activeTabLabel: String? {
        guard let session else { return nil }
        var parts = [session.namespace, "shell \(session.shell)"]
        if let containerName = session.containerName, !containerName.isEmpty {
            parts.append("container \(containerName)")
        }
        if let exitCode = session.lastExitCode {
            parts.append("exit \(exitCode)")
        }
        return parts.joined(separator: " - ")
    }

    private var canSendInput: Bool {
        session?.status == .connected
    }

    private var canStopActiveSession: Bool {
        session?.status == .connected || session?.status == .connecting
    }

    private var selectedPodExistingSession: PodTerminalSession? {
        guard session == nil, let selectedPod else { return nil }
        let containerName = selectedTerminalContainerName.isEmpty ? nil : selectedTerminalContainerName
        return sessions.first {
            $0.namespace == selectedPod.namespace
                && $0.podName == selectedPod.name
                && $0.containerName == containerName
        }
    }

    private var containerOptions: [String] {
        selectedPod?.containerNames ?? []
    }

    private var shouldShowContainerPicker: Bool {
        containerOptions.count > 1 || session?.containerName != nil
    }

    private var canOpenSession: Bool {
        session == nil || session?.status == .disconnected || session?.status == .failed
    }

    private var canSaveActiveTranscript: Bool {
        guard let session else { return false }
        return !session.transcript.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    private var canSaveAllTranscripts: Bool {
        sessions.contains {
            !$0.transcript.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        }
    }

    private var primaryActionTitle: String {
        guard let session else {
            return selectedPodExistingSession == nil ? "Connect" : "Open Tab"
        }
        switch session.status {
        case .connecting:
            return "Cancel"
        case .connected:
            return "Disconnect"
        case .disconnected, .failed:
            return "Reconnect"
        }
    }

    private var primaryActionSystemImage: String {
        guard let session else {
            return selectedPodExistingSession == nil ? "play.fill" : "arrowshape.turn.up.right"
        }
        switch session.status {
        case .connecting:
            return "xmark"
        case .connected:
            return "stop.fill"
        case .disconnected, .failed:
            return "arrow.clockwise"
        }
    }

    private var primaryActionDisabled: Bool {
        if canStopActiveSession {
            return false
        }
        if selectedPodExistingSession != nil {
            return false
        }
        return selectedPod == nil || !canApplyMutations || !canOpenSession
    }

    private var hasTranscript: Bool {
        session?.transcript.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty == false
    }

    private var showsConnectPrompt: Bool {
        guard selectedPod != nil || session != nil else { return false }
        guard let session else { return true }
        return session.status != .connected
    }

    private var connectPromptState: TerminalConnectPrompt.State {
        switch session?.status {
        case .connecting: return .connecting
        case .failed: return .failed
        case .disconnected: return .disconnected
        default: return .notConnected
        }
    }

    private var connectPromptActionTitle: String {
        switch session?.status {
        case .connecting: return "Cancel"
        case .failed, .disconnected: return "Reconnect"
        default: return selectedPodExistingSession == nil ? "Connect" : "Open Tab"
        }
    }

    private var connectPromptPodTitle: String {
        if let session { return session.podName }
        return selectedPod?.name ?? ""
    }

    private var transcriptPlaceholder: String {
        if selectedPod == nil && session == nil {
            return "Select a pod in this namespace, then connect the new shell tab."
        }
        if isComposingNewSession { return "New shell tab. Choose a pod and connect." }
        return "No shell session yet."
    }

    /// One container: a header line, then the terminal as a framed console:
    /// a slim session bar (pod and Connect), the shell tabs on the
    /// transcript's top edge, the transcript, and the prompt as its bottom
    /// line. Rare actions live in the header's one overflow menu.
    var body: some View {
        VStack(alignment: .leading, spacing: RuneSpacing.inset) {
            header

            RuneDocumentFrame(role: .terminal) {
                VStack(alignment: .leading, spacing: RuneSpacing.tight) {
                    TerminalSessionControlRow(
                        title: "Session",
                        systemImage: "terminal",
                        pods: availablePods,
                        terminalSessions: sessions,
                        primaryActionTitle: primaryActionTitle,
                        primaryActionSystemImage: primaryActionSystemImage,
                        isPrimaryActionDisabled: primaryActionDisabled,
                        isClearDisabled: session?.transcript.isEmpty ?? true,
                        isFavoritePod: isFavoritePod,
                        onToggleFavoritePod: onToggleFavoritePod,
                        onPrimaryAction: performPrimaryAction,
                        onClear: onClearTranscript,
                        selection: $selectedShellPodID,
                        showsClear: false,
                        showsPrimaryAction: false
                    )

                    containerPicker
                }
            } tabs: {
                TerminalSessionTabBar(
                    sessions: sessions,
                    activeSessionID: activeSessionID,
                    isComposingNewSession: isComposingNewSession,
                    selectedPod: selectedPod,
                    canApplyMutations: canApplyMutations,
                    onSelectSession: onSelectSession,
                    onCloseSession: onCloseSession,
                    onComposeNewSession: onComposeNewSession,
                    onDisconnectSession: { id in
                        onSelectSession(id)
                        onDisconnect()
                    }
                )
            } content: {
            TerminalTranscriptSurface(
                text: session?.transcript.isEmpty == false ? session?.transcript ?? "" : (showsConnectPrompt ? "" : transcriptPlaceholder),
                height: transcriptHeight,
                resetID: "terminal:\(session?.id ?? "empty")",
                fontSize: terminalFontSize,
                onPasteText: pasteIntoPrompt,
                onInputSequence: onSendControlSequence,
                onResizeGrid: reportTerminalResize
            )
            .overlay(alignment: hasTranscript ? .bottom : .center) {
                // Connect belongs to this terminal, so it sits on it: centred
                // on an empty tab, a slim bar over the end of a finished one.
                if showsConnectPrompt {
                    TerminalConnectPrompt(
                        podTitle: connectPromptPodTitle,
                        state: connectPromptState,
                        failureReason: session?.lastDiagnostic?.summary,
                        isCompact: hasTranscript,
                        isActionDisabled: primaryActionDisabled,
                        actionTitle: connectPromptActionTitle,
                        action: performPrimaryAction
                    )
                    .padding(RuneSpacing.sm)
                }
            }
            } status: {
                VStack(alignment: .leading, spacing: RuneSpacing.tight) {
                    multilinePasteConfirmationNote
                    inputRow
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .onAppear {
            syncContainerSelectionToActiveSession()
            syncShellPodSelectionToActiveSession()
            focusPromptIfConnected()
        }
        .onChange(of: activeSessionID) { _, _ in
            syncContainerSelectionToActiveSession()
            syncShellPodSelectionToActiveSession()
            focusPromptIfConnected()
        }
        .onChange(of: selectedPod?.id) { _, _ in
            syncContainerSelectionToSelectedPod()
        }
        .onChange(of: selectedShellPodID) { _, newValue in
            handleShellPodSelectionChange(newValue)
        }
        .onChange(of: session?.status) { _, _ in
            focusPromptIfConnected()
        }
        .onChange(of: isComposingNewSession) { _, isComposing in
            if isComposing {
                autoConnectComposedTabIfNeeded()
            }
        }
    }

    /// Title and state on the left; one overflow menu on the right for the
    /// rare actions (logs handoff, clear, export), so the header stays quiet.
    private var header: some View {
        HStack(alignment: .center, spacing: RuneSpacing.inset) {
            titleBlock
            Spacer(minLength: 0)
            exportMenu
        }
    }

    private var titleBlock: some View {
        HStack(spacing: RuneSpacing.xs) {
            Label("Pod Shell", systemImage: "terminal")
                .runeText(.headline)
            if let session {
                statusBadge(session.status)
            } else if isComposingNewSession {
                draftBadge
            }
        }
        .help(activeTabLabel ?? (isComposingNewSession ? "New shell tab" : "No active shell tab"))
    }

    private var exportMenu: some View {
        RuneToolbarMenu {
            if let selectedPod, onOpenSelectedPodLogs != nil {
                Button("Show \(selectedPod.name) in Logs") {
                    openSelectedPodLogs()
                }
                .help("Show logs for \(selectedPod.namespace)/\(selectedPod.name)")
                .accessibilityIdentifier("terminal-shell-open-pod-logs")
            }

            if canStopActiveSession {
                Button("Disconnect") {
                    onDisconnect()
                }
                .accessibilityIdentifier("terminal-session-disconnect")
            }

            Button("Clear Output") {
                onClearTranscript()
            }
            .keyboardShortcut("k", modifiers: [.command])
            .disabled(session?.transcript.isEmpty ?? true)
            .accessibilityIdentifier("terminal-session-clear")

            Divider()

            Button("Save Active Transcript") {
                onSaveActiveTranscript()
            }
            .disabled(!canSaveActiveTranscript)

            Button("Save Active to Export Folder") {
                onSaveActiveTranscriptToExportFolder()
            }
            .disabled(!canSaveActiveTranscript)

            Button("Save Active and Open") {
                onSaveActiveTranscriptAndOpen()
            }
            .disabled(!canSaveActiveTranscript)

            Divider()

            Button("Save All Transcripts ZIP") {
                onSaveAllTranscripts()
            }
            .disabled(!canSaveAllTranscripts)

            Button("Save All ZIP to Export Folder") {
                onSaveAllTranscriptsToExportFolder()
            }
            .disabled(!canSaveAllTranscripts)

            Button("Save All ZIP and Open") {
                onSaveAllTranscriptsAndOpen()
            }
            .disabled(!canSaveAllTranscripts)
        } label: {
            Label("More", systemImage: "ellipsis.circle")
                .labelStyle(.iconOnly)
        }
        .help("Logs, clear and export")
        .accessibilityLabel("More shell actions")
    }

    func openSelectedPodLogs() {
        guard let selectedPod, let onOpenSelectedPodLogs else { return }
        onOpenSelectedPodLogs(selectedPod)
    }

    @ViewBuilder
    private var containerPicker: some View {
        if shouldShowContainerPicker {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: RuneSpacing.inset) {
                    Label("Container", systemImage: "shippingbox")
                        .runeText(.microEmphasis)
                        .foregroundStyle(.runeSecondary)
                        .frame(width: 116, alignment: .leading)

                    Picker("Container", selection: $selectedTerminalContainerName) {
                        Text("Default container").tag("")
                        ForEach(containerOptions, id: \.self) { containerName in
                            Text(containerName).tag(containerName)
                        }
                    }
                    .labelsHidden()
                    .controlSize(.small)
                    .frame(width: 340, alignment: .leading)
                    .runeMinimumInteractiveTarget(alignment: .leading)
                    .help(selectedTerminalContainerName.isEmpty
                        ? "Default container"
                        : selectedTerminalContainerName)
                }
                .fixedSize(horizontal: true, vertical: false)
            }
        }
    }

    private var inputRow: some View {
        HStack(spacing: RuneSpacing.xs) {
            Text("$")
                .font(.system(size: terminalFontSize, weight: .semibold, design: .monospaced))
                .foregroundStyle(canSendInput ? Color.accentColor : .secondary)

            ZStack(alignment: .leading) {
                TerminalPromptTextEditor(
                    text: $terminalInput,
                    fontSize: terminalFontSize,
                    isEnabled: canSendInput,
                    isFocused: $isInputFocused,
                    onSubmit: sendPrompt,
                    onHistoryUp: recallPreviousCommand,
                    onHistoryDown: recallNextCommand,
                    onSendControlSequence: onSendControlSequence,
                    onClearTranscript: onClearTranscript
                )

                if terminalInput.isEmpty {
                    Text("Type a shell command and press Return")
                        .font(.system(size: terminalFontSize, weight: .regular, design: .monospaced))
                        .foregroundStyle(.runeTertiary)
                        .padding(.leading, RuneSpacing.xs)
                        .allowsHitTesting(false)
                }
            }
            .frame(height: terminalPromptLayout.controlHeight)
            // The prompt shares the transcript's terminal surface and frame,
            // so input and output read as one console.
            .background(
                RoundedRectangle(cornerRadius: RuneRadius.control, style: .continuous)
                    .fill(RuneSurfaceRole.terminal.fill(palette: runeThemePalette, colorScheme: colorScheme))
            )
            .overlay(
                RoundedRectangle(cornerRadius: RuneRadius.control, style: .continuous)
                    .stroke(RuneSurfaceRole.terminal.frame(palette: runeThemePalette).opacity(canSendInput ? 1 : 0.5), lineWidth: 1)
            )
            .opacity(canSendInput ? 1 : 0.62)

            Button("Send") {
                sendPrompt()
            }
            .disabled(!canSendInput || terminalInput.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
        }
        .controlSize(.small)
    }

    private var multilinePasteConfirmationNote: some View {
        TerminalMultilinePasteNotice(isActive: pendingMultilinePasteConfirmation)
    }

    private func statusBadge(_ status: PodTerminalSessionStatus) -> some View {
        Text(TerminalStatusStyling.title(status))
            .runeText(.microEmphasis)
            .padding(.horizontal, RuneSpacing.xs)
            .padding(.vertical, RuneSpacing.xxs)
            .background(
                TerminalStatusStyling.color(status, palette: runeThemePalette).opacity(0.16),
                in: Capsule()
            )
            .foregroundStyle(TerminalStatusStyling.color(status, palette: runeThemePalette))
    }

    private var draftBadge: some View {
        Text("Ready")
            .runeText(.microEmphasis)
            .padding(.horizontal, RuneSpacing.xs)
            .padding(.vertical, RuneSpacing.xxs)
            .background(Color.accentColor.opacity(0.14), in: Capsule())
            .foregroundStyle(.runeAccent)
    }

    private func syncShellPodSelectionToActiveSession() {
        guard !isComposingNewSession else { return }
        guard let session else { return }
        let id = "\(session.namespace)/\(session.podName)"
        if availablePods.contains(where: { $0.id == id }) {
            selectedShellPodID = id
        }
    }

    private func syncContainerSelectionToActiveSession() {
        guard !isComposingNewSession else {
            syncContainerSelectionToSelectedPod()
            return
        }
        selectedTerminalContainerName = session?.containerName ?? ""
    }

    private func syncContainerSelectionToSelectedPod() {
        if let selectedPod,
           !selectedTerminalContainerName.isEmpty,
           !selectedPod.containerNames.contains(selectedTerminalContainerName) {
            selectedTerminalContainerName = ""
        }
    }

    private func focusPromptIfConnected() {
        guard canSendInput else { return }
        Task { @MainActor in
            isInputFocused = true
        }
    }

    private func pasteIntoPrompt(_ text: String) {
        guard canSendInput else { return }
        let paste = TerminalShellPanelView.normalizedTerminalPasteForPrompt(text)
        guard !paste.text.isEmpty else { return }
        terminalInput += paste.text
        pendingMultilinePasteConfirmation = paste.requiresConfirmation
        isInputFocused = true
    }

    private func sendPrompt() {
        let send = TerminalShellPanelView.preparedTerminalPromptSend(
            input: terminalInput,
            pendingMultilinePasteConfirmation: pendingMultilinePasteConfirmation
        )
        if send.shouldRefocusPrompt {
            pendingMultilinePasteConfirmation = false
            isInputFocused = true
            return
        }
        if send.shouldClearConfirmation {
            pendingMultilinePasteConfirmation = false
        }
        if send.shouldSend {
            rememberCommand(send.command)
        }
        commandHistoryIndex = nil
        if send.shouldSend {
            onSend()
        }
    }

    private func reportTerminalResize(columns: Int, rows: Int) {
        guard let session, session.status == .connected else { return }
        onResizeSession(session.id, columns, rows)
    }

    nonisolated static func normalizedTerminalPasteForPrompt(_ text: String) -> (text: String, requiresConfirmation: Bool) {
        let normalized = text
            .replacingOccurrences(of: "\r\n", with: "\n")
            .replacingOccurrences(of: "\r", with: "\n")
            .trimmingCharacters(in: .newlines)
        var nonEmptyLineCount = 0
        var hasNonNewlineCharacter = false
        for character in normalized {
            if character == "\n" {
                if hasNonNewlineCharacter {
                    nonEmptyLineCount += 1
                    if nonEmptyLineCount > 1 {
                        return (normalized, true)
                    }
                    hasNonNewlineCharacter = false
                }
            } else {
                hasNonNewlineCharacter = true
            }
        }
        if hasNonNewlineCharacter {
            nonEmptyLineCount += 1
        }
        return (normalized, nonEmptyLineCount > 1)
    }

    nonisolated static func preparedTerminalPromptSend(
        input: String,
        pendingMultilinePasteConfirmation: Bool
    ) -> (command: String, shouldSend: Bool, shouldClearConfirmation: Bool, shouldRefocusPrompt: Bool) {
        let command = input.trimmingCharacters(in: .whitespacesAndNewlines)
        if pendingMultilinePasteConfirmation, command.contains("\n") {
            return (command, false, true, true)
        }
        return (command, !command.isEmpty, true, false)
    }

    private func rememberCommand(_ command: String) {
        if commandHistory.last != command {
            commandHistory.append(command)
        }
        if commandHistory.count > 100 {
            commandHistory.removeFirst(commandHistory.count - 100)
        }
    }

    private func recallPreviousCommand() {
        guard !commandHistory.isEmpty else { return }
        let nextIndex: Int
        if let commandHistoryIndex {
            nextIndex = max(commandHistory.startIndex, commandHistoryIndex - 1)
        } else {
            nextIndex = commandHistory.index(before: commandHistory.endIndex)
        }
        commandHistoryIndex = nextIndex
        terminalInput = commandHistory[nextIndex]
    }

    private func recallNextCommand() {
        guard let commandHistoryIndex else { return }
        let nextIndex = commandHistoryIndex + 1
        guard nextIndex < commandHistory.endIndex else {
            self.commandHistoryIndex = nil
            terminalInput = ""
            return
        }
        self.commandHistoryIndex = nextIndex
        terminalInput = commandHistory[nextIndex]
    }

    private func handleShellPodSelectionChange(_ podID: String) {
        guard !podID.isEmpty else { return }
        let containerName = selectedTerminalContainerName.isEmpty ? nil : selectedTerminalContainerName
        if let session,
           podID == "\(session.namespace)/\(session.podName)",
           session.containerName == containerName {
            return
        }
        if let matchingSession = sessions.first(where: {
            "\($0.namespace)/\($0.podName)" == podID && $0.containerName == containerName
        }) {
            onSelectSession(matchingSession.id)
        } else if autoConnects, let pod = availablePods.first(where: { $0.id == podID }) {
            // Choosing a pod is the intent to open its shell.
            onStartSession(pod, containerName)
        } else {
            onComposeNewSession()
        }
    }

    /// A new tab for a pod connects at once where auto-connect applies.
    private func autoConnectComposedTabIfNeeded() {
        guard autoConnects, isComposingNewSession, session == nil,
              let selectedPod, selectedPodExistingSession == nil, canApplyMutations else { return }
        let containerName = selectedTerminalContainerName.isEmpty ? nil : selectedTerminalContainerName
        onStartSession(selectedPod, containerName)
    }

    private func performPrimaryAction() {
        if canStopActiveSession {
            onDisconnect()
            return
        }
        guard let selectedPod else { return }
        if let existing = selectedPodExistingSession {
            onSelectSession(existing.id)
            return
        }
        let containerName = selectedTerminalContainerName.isEmpty ? nil : selectedTerminalContainerName
        if let session, session.status == .disconnected || session.status == .failed {
            onReconnectSession(session, selectedPod, containerName)
        } else {
            onStartSession(selectedPod, containerName)
        }
    }

}

struct TerminalMultilinePasteNotice: View {
    let isActive: Bool

    @ViewBuilder
    var body: some View {
        if isActive {
            HStack(spacing: RuneSpacing.tight) {
                Image(systemName: "return")
                    .runeText(.microEmphasis)
                Text("Multiline paste staged. Press Return again to send.")
                    .runeText(.microEmphasis)
                    .lineLimit(1)
            }
            .foregroundStyle(.runeAccent)
            .frame(height: 18, alignment: .leading)
            .accessibilityLabel("Multiline paste staged. Press Return again to send.")
        }
    }
}

struct TerminalPromptLayoutMetrics: Equatable {
    static let minimumControlHeight: CGFloat = 24
    static let horizontalInset: CGFloat = 7
    static let verticalInset: CGFloat = 3

    let fontSize: CGFloat
    let lineHeight: CGFloat
    let textContainerHeight: CGFloat
    let controlHeight: CGFloat

    init(fontSize: CGFloat) {
        let clampedFontSize = CGFloat(RuneSettingsKeys.clampedTerminalFontSize(Double(fontSize)))
        let font = NSFont.monospacedSystemFont(ofSize: clampedFontSize, weight: .regular)
        let measuredLineHeight = ceil(font.ascender - font.descender + font.leading)

        self.fontSize = clampedFontSize
        lineHeight = max(1, measuredLineHeight)
        textContainerHeight = max(1, measuredLineHeight)
        controlHeight = max(
            Self.minimumControlHeight,
            measuredLineHeight + Self.verticalInset * 2
        )
    }
}

enum TerminalPromptPalette {
    static var inputTextColor: NSColor { .labelColor }
    static var disabledInputTextColor: NSColor { .disabledControlTextColor }
    static var selectedInputTextColor: NSColor { .selectedTextColor }
    static var selectionBackgroundColor: NSColor { .selectedTextBackgroundColor }
    static var insertionPointColor: NSColor { .controlAccentColor }
}

struct TerminalPromptTextEditor: NSViewRepresentable {
    @Environment(\.runeThemePalette) private var palette
    @Binding var text: String
    let fontSize: CGFloat
    let isEnabled: Bool
    @Binding var isFocused: Bool
    let onSubmit: () -> Void
    let onHistoryUp: () -> Void
    let onHistoryDown: () -> Void
    let onSendControlSequence: (String) -> Void
    let onClearTranscript: () -> Void

    func makeCoordinator() -> Coordinator {
        Coordinator(self)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = NSScrollView(frame: .zero)
        scrollView.drawsBackground = false
        scrollView.borderType = .noBorder
        scrollView.hasVerticalScroller = false
        scrollView.hasHorizontalScroller = false
        scrollView.autohidesScrollers = true
        scrollView.automaticallyAdjustsContentInsets = false
        scrollView.contentInsets = NSEdgeInsets()

        let textView = TerminalPromptTextView(frame: .zero)
        textView.delegate = context.coordinator
        textView.onSubmit = onSubmit
        textView.onHistoryUp = onHistoryUp
        textView.onHistoryDown = onHistoryDown
        textView.onSendControlSequence = onSendControlSequence
        textView.onClearTranscript = onClearTranscript
        textView.onTextReplacement = { value in
            context.coordinator.parent.text = value
        }
        configure(textView)
        layoutTextView(textView, in: scrollView)
        textView.string = text
        scrollView.documentView = textView
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.parent = self
        guard let textView = scrollView.documentView as? TerminalPromptTextView else { return }
        textView.onSubmit = onSubmit
        textView.onHistoryUp = onHistoryUp
        textView.onHistoryDown = onHistoryDown
        textView.onSendControlSequence = onSendControlSequence
        textView.onClearTranscript = onClearTranscript
        textView.onTextReplacement = { value in
            context.coordinator.parent.text = value
        }
        configure(textView)
        layoutTextView(textView, in: scrollView)
        if textView.string != text {
            textView.string = text
            textView.setSelectedRange(NSRange(location: text.utf16.count, length: 0))
        }

        if isFocused, isEnabled, textView.window?.firstResponder !== textView {
            Task { @MainActor in
                textView.window?.makeFirstResponder(textView)
            }
        } else if !isEnabled, textView.window?.firstResponder === textView {
            textView.window?.makeFirstResponder(nil)
        }
    }

    private func configure(_ textView: TerminalPromptTextView) {
        let layout = TerminalPromptLayoutMetrics(fontSize: fontSize)
        textView.isEditable = isEnabled
        textView.isSelectable = true
        textView.isRichText = false
        textView.importsGraphics = false
        textView.usesFontPanel = false
        textView.allowsUndo = true
        textView.isAutomaticQuoteSubstitutionEnabled = false
        textView.isAutomaticDashSubstitutionEnabled = false
        textView.isAutomaticDataDetectionEnabled = false
        textView.isAutomaticLinkDetectionEnabled = false
        textView.isAutomaticSpellingCorrectionEnabled = false
        textView.isAutomaticTextCompletionEnabled = false
        textView.isGrammarCheckingEnabled = false
        textView.isContinuousSpellCheckingEnabled = false
        textView.isHorizontallyResizable = false
        textView.isVerticallyResizable = false
        textView.minSize = .zero
        textView.maxSize = NSSize(width: CGFloat.greatestFiniteMagnitude, height: layout.controlHeight)
        textView.autoresizingMask = [.width]
        textView.textContainerInset = NSSize(
            width: TerminalPromptLayoutMetrics.horizontalInset,
            height: TerminalPromptLayoutMetrics.verticalInset
        )
        textView.backgroundColor = .clear
        textView.drawsBackground = false
        let promptFont = NSFont.monospacedSystemFont(ofSize: fontSize, weight: .regular)
        let inputTextColor = palette.map { NSColor(isEnabled ? $0.foreground : $0.mutedText) }
            ?? RuneThemeContrast.nativeInk(isEnabled ? TerminalPromptPalette.inputTextColor : TerminalPromptPalette.disabledInputTextColor)
        textView.font = promptFont
        textView.textColor = inputTextColor
        textView.insertionPointColor = palette.map { NSColor($0.accent) } ?? TerminalPromptPalette.insertionPointColor
        textView.selectedTextAttributes = [
            .backgroundColor: TerminalPromptPalette.selectionBackgroundColor,
            .foregroundColor: TerminalPromptPalette.selectedInputTextColor
        ]
        textView.typingAttributes = [
            .font: promptFont,
            .foregroundColor: inputTextColor
        ]
        textView.textContainer?.lineFragmentPadding = 0
        textView.textContainer?.widthTracksTextView = true
    }

    private func layoutTextView(_ textView: TerminalPromptTextView, in scrollView: NSScrollView) {
        let layout = TerminalPromptLayoutMetrics(fontSize: fontSize)
        let viewportSize = scrollView.contentView.bounds.size
        let resolvedWidth = max(viewportSize.width, 1)
        let resolvedSize = NSSize(width: resolvedWidth, height: layout.controlHeight)
        textView.frame = NSRect(origin: .zero, size: resolvedSize)
        textView.minSize = resolvedSize
        textView.maxSize = resolvedSize
        textView.textContainer?.containerSize = NSSize(
            width: max(1, resolvedWidth - TerminalPromptLayoutMetrics.horizontalInset * 2),
            height: layout.textContainerHeight
        )
    }

    final class Coordinator: NSObject, NSTextViewDelegate {
        var parent: TerminalPromptTextEditor

        init(_ parent: TerminalPromptTextEditor) {
            self.parent = parent
        }

        func textDidChange(_ notification: Notification) {
            guard let textView = notification.object as? NSTextView else { return }
            parent.text = textView.string
        }

        func textDidBeginEditing(_ notification: Notification) {
            parent.isFocused = true
        }

        func textDidEndEditing(_ notification: Notification) {
            parent.isFocused = false
        }
    }
}

private final class TerminalPromptTextView: NSTextView {
    var onSubmit: (() -> Void)?
    var onHistoryUp: (() -> Void)?
    var onHistoryDown: (() -> Void)?
    var onSendControlSequence: ((String) -> Void)?
    var onClearTranscript: (() -> Void)?
    var onTextReplacement: ((String) -> Void)?

    override func keyDown(with event: NSEvent) {
        if handleTerminalControlEvent(event) {
            return
        }

        switch event.keyCode {
        case 36, 76:
            onSubmit?()
        case 126:
            onHistoryUp?()
        case 125:
            onHistoryDown?()
        default:
            super.keyDown(with: event)
        }
    }

    override func insertNewline(_ sender: Any?) {
        onSubmit?()
    }

    override func performKeyEquivalent(with event: NSEvent) -> Bool {
        let flags = event.modifierFlags.intersection(.deviceIndependentFlagsMask)
        guard flags.contains(.command),
              let key = event.charactersIgnoringModifiers?.lowercased() else {
            return super.performKeyEquivalent(with: event)
        }

        if key == "k" {
            onClearTranscript?()
            return true
        }

        return super.performKeyEquivalent(with: event)
    }

    private func handleTerminalControlEvent(_ event: NSEvent) -> Bool {
        let flags = event.modifierFlags.intersection(.deviceIndependentFlagsMask)
        guard flags.contains(.control),
              let key = event.charactersIgnoringModifiers?.lowercased() else {
            return false
        }

        switch key {
        case "c":
            onSendControlSequence?("\u{3}")
            replaceText("")
            return true
        case "d":
            if string.isEmpty {
                onSendControlSequence?("\u{4}")
                return true
            }
            return false
        case "l":
            onClearTranscript?()
            onSendControlSequence?("\u{c}")
            return true
        case "u":
            replaceText("")
            return true
        case "w":
            deletePreviousWord()
            return true
        case "a":
            setSelectedRange(NSRange(location: 0, length: 0))
            return true
        case "e":
            setSelectedRange(NSRange(location: string.utf16.count, length: 0))
            return true
        default:
            return false
        }
    }

    private func deletePreviousWord() {
        let range = selectedRange()
        let nsString = string as NSString
        if range.length > 0 {
            replaceCharacters(in: range, with: "")
            onTextReplacement?(string)
            return
        }

        guard range.location > 0 else { return }
        var index = range.location
        while index > 0, CharacterSet.whitespacesAndNewlines.contains(character(at: index - 1, in: nsString)) {
            index -= 1
        }
        while index > 0, !CharacterSet.whitespacesAndNewlines.contains(character(at: index - 1, in: nsString)) {
            index -= 1
        }
        let deleteRange = NSRange(location: index, length: range.location - index)
        replaceCharacters(in: deleteRange, with: "")
        onTextReplacement?(string)
    }

    private func character(at index: Int, in string: NSString) -> Unicode.Scalar {
        Unicode.Scalar(string.character(at: index)) ?? " "
    }

    private func replaceText(_ newValue: String) {
        string = newValue
        setSelectedRange(NSRange(location: newValue.utf16.count, length: 0))
        onTextReplacement?(newValue)
    }
}

/// The connect action on the terminal surface itself: the pod, its state,
/// why it failed, and one primary button. Centred on an empty tab; a slim
/// bar at the bottom of a tab that still shows its transcript.
struct TerminalConnectPrompt: View {
    enum State: Equatable {
        case notConnected
        case connecting
        case disconnected
        case failed

        var title: String {
            switch self {
            case .notConnected: return "Not connected"
            case .connecting: return "Connecting…"
            case .disconnected: return "Disconnected"
            case .failed: return "Connection failed"
            }
        }
    }

    let podTitle: String
    let state: State
    let failureReason: String?
    let isCompact: Bool
    let isActionDisabled: Bool
    let actionTitle: String
    let action: () -> Void
    @Environment(\.runeThemePalette) private var palette

    var body: some View {
        Group {
            if isCompact {
                HStack(spacing: RuneSpacing.sm) {
                    statusLine
                    Spacer(minLength: RuneSpacing.xs)
                    actionButton
                }
                .padding(.horizontal, RuneSpacing.sm)
                .padding(.vertical, RuneSpacing.xs)
                .frame(maxWidth: .infinity)
            } else {
                VStack(spacing: RuneSpacing.sm) {
                    Image(systemName: "terminal")
                        .runeText(.title)
                        .foregroundStyle(.runeSecondary)
                    Text(podTitle)
                        .runeText(.headline)
                        .lineLimit(1)
                        .truncationMode(.middle)
                    statusLine
                    if let failureReason, state == .failed, !failureReason.isEmpty {
                        Text(failureReason)
                            .runeText(.caption)
                            .foregroundStyle(.runeSecondary)
                            .multilineTextAlignment(.center)
                            .lineLimit(3)
                            .frame(maxWidth: 360)
                    }
                    actionButton
                }
                .padding(RuneSpacing.lg)
            }
        }
        .modifier(RuneCardSurface())
        .accessibilityElement(children: .contain)
        .accessibilityLabel("\(podTitle), \(state.title)")
        .accessibilityIdentifier("terminal-connect-prompt")
    }

    private var statusLine: some View {
        HStack(spacing: RuneSpacing.tight) {
            if state == .connecting {
                ProgressView().controlSize(.small)
            } else {
                Circle()
                    .fill(state == .failed ? RuneSemanticColorRole.danger.color(in: palette) : Color.secondary)
                    .frame(width: 7, height: 7)
            }
            Text(isCompact ? "\(podTitle) · \(state.title)" : state.title)
                .runeText(.caption)
                .foregroundStyle(.runeSecondary)
                .lineLimit(1)
                .truncationMode(.middle)
        }
    }

    private var actionButton: some View {
        Button(action: action) {
            Label(actionTitle, systemImage: state == .connecting ? "xmark" : "play.fill")
        }
        .buttonStyle(RuneToolbarButtonStyle(isProminent: state != .connecting))
        .controlSize(.regular)
        .disabled(isActionDisabled)
        .keyboardShortcut(state == .connecting ? nil : .defaultAction)
        .accessibilityIdentifier("terminal-session-primary-action")
    }
}
