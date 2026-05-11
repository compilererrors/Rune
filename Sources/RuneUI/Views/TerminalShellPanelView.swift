import AppKit
import SwiftUI
import RuneCore

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
    let onDisconnect: () -> Void
    let onSelectSession: (String) -> Void
    let onCloseSession: (String) -> Void
    let onComposeNewSession: () -> Void
    let onClearTranscript: () -> Void
    let onSaveActiveTranscript: () -> Void
    let onSaveAllTranscripts: () -> Void
    @AppStorage(RuneSettingsKeys.terminalFontSize) private var storedTerminalFontSize = RuneSettingsKeys.terminalFontSizeDefault
    @State private var isInputFocused = false
    @State private var commandHistory: [String] = []
    @State private var commandHistoryIndex: Int?
    @State private var pendingMultilinePasteConfirmation = false
    @State private var selectedTerminalContainerName = ""

    private var terminalFontSize: CGFloat {
        CGFloat(RuneSettingsKeys.clampedTerminalFontSize(storedTerminalFontSize))
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

    private var transcriptPlaceholder: String {
        if selectedPod == nil && session == nil {
            return "Select a pod in this namespace, then connect the new shell tab."
        }
        if isComposingNewSession { return "New shell tab. Choose a pod and connect." }
        return "No shell session yet."
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            header

            TerminalSessionTabBar(
                sessions: sessions,
                activeSessionID: activeSessionID,
                isComposingNewSession: isComposingNewSession,
                selectedPod: selectedPod,
                canApplyMutations: canApplyMutations,
                selectedShellPodID: $selectedShellPodID,
                onStartSession: { pod in
                    onStartSession(pod, selectedTerminalContainerName.isEmpty ? nil : selectedTerminalContainerName)
                },
                onSelectSession: onSelectSession,
                onCloseSession: onCloseSession,
                onComposeNewSession: onComposeNewSession
            )

            TerminalSessionControlRow(
                title: "Session",
                systemImage: "terminal",
                pods: availablePods,
                terminalSessions: sessions,
                primaryActionTitle: primaryActionTitle,
                primaryActionSystemImage: primaryActionSystemImage,
                isPrimaryActionDisabled: primaryActionDisabled,
                isClearDisabled: session?.transcript.isEmpty ?? true,
                onPrimaryAction: performPrimaryAction,
                onClear: onClearTranscript,
                selection: $selectedShellPodID
            )

            containerPicker

            TerminalTranscriptSurface(
                text: session?.transcript.isEmpty == false ? session?.transcript ?? "" : transcriptPlaceholder,
                height: transcriptHeight,
                resetID: "terminal:\(session?.id ?? "empty")",
                fontSize: terminalFontSize,
                onPasteText: pasteIntoPrompt
            )

            multilinePasteConfirmationNote
            inputRow
        }
        .runePanelCard(padding: RuneUILayoutMetrics.paneInnerPadding)
        .onAppear {
            syncShellPodSelectionToActiveSession()
            syncContainerSelectionToActiveSession()
            focusPromptIfConnected()
        }
        .onChange(of: activeSessionID) { _, _ in
            syncShellPodSelectionToActiveSession()
            syncContainerSelectionToActiveSession()
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
    }

    private var header: some View {
        HStack(alignment: .top, spacing: 10) {
            titleBlock
            Spacer(minLength: 0)
            exportMenu
        }
    }

    private var titleBlock: some View {
        VStack(alignment: .leading, spacing: 5) {
            HStack(spacing: 8) {
                Label("Pod Shell", systemImage: "terminal")
                    .font(.headline)
                if let session {
                    statusBadge(session.status)
                } else if isComposingNewSession {
                    draftBadge
                }
            }

            if let activeTabLabel {
                Text(activeTabLabel)
                    .font(.footnote.weight(.semibold))
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .help(activeTabLabel)
            } else {
                Text(isComposingNewSession ? "New shell tab" : "No active shell tab")
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            }
        }
    }

    private var exportMenu: some View {
        Menu {
            Button("Save Active Transcript") {
                onSaveActiveTranscript()
            }
            .disabled(!canSaveActiveTranscript)

            Button("Save All Transcripts ZIP") {
                onSaveAllTranscripts()
            }
            .disabled(!canSaveAllTranscripts)
        } label: {
            Label("Export", systemImage: "square.and.arrow.down")
        }
        .menuStyle(.button)
        .controlSize(.small)
        .help("Export terminal transcripts")
    }

    @ViewBuilder
    private var containerPicker: some View {
        if shouldShowContainerPicker {
            ScrollView(.horizontal, showsIndicators: false) {
                HStack(spacing: 10) {
                    Label("Container", systemImage: "shippingbox")
                        .font(.footnote.weight(.semibold))
                        .foregroundStyle(.secondary)
                        .frame(width: 116, alignment: .leading)

                    Picker("Container", selection: $selectedTerminalContainerName) {
                        Text("Default container").tag("")
                        ForEach(containerOptions, id: \.self) { containerName in
                            Text(containerName).tag(containerName)
                        }
                    }
                    .labelsHidden()
                    .controlSize(.small)
                    .frame(width: 340, height: 26, alignment: .leading)
                }
                .fixedSize(horizontal: true, vertical: false)
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 8)
            .background(RuneSurfaceBackground(kind: .editor))
        }
    }

    private var inputRow: some View {
        HStack(spacing: 8) {
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
                        .foregroundStyle(.tertiary)
                        .padding(.leading, 8)
                        .allowsHitTesting(false)
                }
            }
            .frame(height: 24)
            .background(
                RoundedRectangle(cornerRadius: 5, style: .continuous)
                    .fill(Color(nsColor: canSendInput ? .textBackgroundColor : .controlBackgroundColor).opacity(0.72))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 5, style: .continuous)
                    .stroke(Color.primary.opacity(canSendInput ? 0.16 : 0.08), lineWidth: 1)
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
        HStack(spacing: 6) {
            Image(systemName: "return")
                .font(.caption.weight(.semibold))
            Text("Multiline paste staged. Press Return again to send.")
                .font(.caption.weight(.semibold))
                .lineLimit(1)
        }
        .foregroundStyle(Color.accentColor)
        .opacity(pendingMultilinePasteConfirmation ? 1 : 0)
        .frame(height: 18, alignment: .leading)
        .accessibilityHidden(!pendingMultilinePasteConfirmation)
    }

    private func statusBadge(_ status: PodTerminalSessionStatus) -> some View {
        Text(TerminalStatusStyling.title(status))
            .font(.caption.weight(.semibold))
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(TerminalStatusStyling.color(status).opacity(0.16), in: Capsule())
            .foregroundStyle(TerminalStatusStyling.color(status))
    }

    private var draftBadge: some View {
        Text("Ready")
            .font(.caption.weight(.semibold))
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(Color.accentColor.opacity(0.14), in: Capsule())
            .foregroundStyle(Color.accentColor)
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
        if let session, podID == "\(session.namespace)/\(session.podName)" {
            return
        }
        if let matchingSession = sessions.first(where: { "\($0.namespace)/\($0.podName)" == podID }) {
            onSelectSession(matchingSession.id)
        } else {
            onComposeNewSession()
        }
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

private enum TerminalPromptPalette {
    static let inputTextColor = NSColor(calibratedWhite: 0.92, alpha: 1)
    static let disabledInputTextColor = NSColor(calibratedWhite: 0.58, alpha: 1)
    static let selectedInputTextColor = NSColor.white
    static let selectionBackgroundColor = NSColor.controlAccentColor.withAlphaComponent(0.34)
    static let insertionPointColor = NSColor.controlAccentColor
}

private struct TerminalPromptTextEditor: NSViewRepresentable {
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
        textView.maxSize = NSSize(width: CGFloat.greatestFiniteMagnitude, height: 24)
        textView.autoresizingMask = [.width]
        textView.textContainerInset = NSSize(width: 7, height: 3)
        textView.backgroundColor = .clear
        textView.drawsBackground = false
        let promptFont = NSFont.monospacedSystemFont(ofSize: fontSize, weight: .regular)
        let inputTextColor = isEnabled ? TerminalPromptPalette.inputTextColor : TerminalPromptPalette.disabledInputTextColor
        textView.font = promptFont
        textView.textColor = inputTextColor
        textView.insertionPointColor = TerminalPromptPalette.insertionPointColor
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
        let viewportSize = scrollView.contentView.bounds.size
        let resolvedWidth = max(viewportSize.width, 1)
        let resolvedSize = NSSize(width: resolvedWidth, height: 24)
        textView.frame = NSRect(origin: .zero, size: resolvedSize)
        textView.minSize = resolvedSize
        textView.maxSize = resolvedSize
        textView.textContainer?.containerSize = resolvedSize
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
