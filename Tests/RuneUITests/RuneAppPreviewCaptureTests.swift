// The scripted scenario it drives exists only in debug builds.
#if DEBUG
import AppKit
import AVFoundation
import CoreVideo
import RuneCore
@testable import RuneUI
import XCTest

/// Footage for the three App Store App Previews, against the local Docker
/// Compose K3s clusters: overview, logs and access. Each test records Rune's
/// own window at 2880 by 1620 (a 1440 by 810 point window, 16 by 9, so the
/// renderer can zoom in and stay sharp) into
/// `<name>-recording.mov`, with `<name>-markers.json` saying where each scene
/// starts, in seconds; the marketing renderer cuts the previews from them.
/// Needs no screen recording or accessibility permission.
///
/// Runs only when `RUNE_UI_APPPREVIEW_DIR` is set; see
/// `RuneAppStoreCaptureFixture` for the rest. Run one test at a time.
@MainActor
final class RuneAppPreviewCaptureTests: XCTestCase {
    /// One scene from each part of Rune.
    func testRecordOverviewPreviewFootage() async throws {
        try await record("overview", steps: [
            (.overview, 4),
            (.workloadPodLogs, 6),
            (.workloadDeploymentUnifiedLogs, 5),
            (.terminalLogs, 7),
            (.workloadDeploymentYAMLValidation, 4),
            (.authDoctor, 6),
            (.rbacCanI, 4)
        ]) { step, take in
            switch step {
            case .terminalLogs:
                await take.pause(0.7)
                for character in "ps -o pid,comm" {
                    take.viewModel.terminalSessionInput.append(character)
                    await take.pause(0.09)
                }
                take.viewModel.sendTerminalSessionInput()
            case .authDoctor:
                // Run it again on camera so the checks fill in.
                await take.pause(0.5)
                take.viewModel.runAuthDoctor()
            default:
                break
            }
        } after: { take in
            let viewModel = take.viewModel
            viewModel.setSection(.workloads)
            viewModel.setWorkloadKind(.pod)
            await take.pause(1.5)
            take.mark("commandPalette")
            viewModel.presentCommandPalette()
            await take.pause(0.7)
            await take.typeSlowly(":po log", every: 0.14)
            await take.pause(1.4)
            take.press("\r", keyCode: 36)
            await take.pause(3)
            if take.state.isCommandPalettePresented { viewModel.dismissCommandPalette() }
            viewModel.setSection(.overview)
            await take.pause(1.5)
            take.mark("closing")
            await take.pause(5)
        }
    }

    /// Every container in one timeline, timestamps, one container, how many
    /// lines, search, a whole deployment, a service, log tabs beside the
    /// shell, and saving from the palette.
    func testRecordLogsPreviewFootage() async throws {
        try await record("logs", steps: [
            (.workloadPodLogs, 0),
            (.workloadDeploymentUnifiedLogs, 5),
            (.networkingServiceUnifiedLogs, 4.5),
            (.terminalLogs, 4.5)
        ], prepare: { fixture in
            // Off at first, so the preview can turn them on.
            fixture.setSetting(false, forKey: RuneSettingsKeys.logsShowTimestamps)
        }) { step, take in
            let viewModel = take.viewModel
            switch step {
            case .workloadPodLogs:
                await take.pause(2.8)
                take.mark("timestamps")
                take.fixture.setSetting(true, forKey: RuneSettingsKeys.logsShowTimestamps)
                await take.pause(3.2)
                take.mark("oneContainer")
                viewModel.selectedLogContainer = "sidecar"
                viewModel.reloadLogsForSelection()
                await take.pause(2.8)
                take.mark("range")
                viewModel.selectedLogContainer = ""
                viewModel.selectedLogPreset = .customOne
                viewModel.reloadLogsForSelection()
                await take.pause(2.2)
                viewModel.selectedLogPreset = .largeTail
                viewModel.reloadLogsForSelection()
                await take.pause(2.4)
                take.mark("search")
                take.press("f", keyCode: 3, modifiers: .command)
                await take.pause(0.5)
                await take.typeSlowly("index=1", every: 0.13)
                await take.pause(2.6)
            case .workloadDeploymentUnifiedLogs:
                await take.pause(1)
                viewModel.isLogTailModeEnabled = true
            default:
                break
            }
        } after: { take in
            let viewModel = take.viewModel
            take.mark("save")
            viewModel.presentCommandPalette()
            await take.pause(0.7)
            await take.typeSlowly(":so", every: 0.16)
            await take.pause(3.2)
            viewModel.dismissCommandPalette()
            await take.pause(1)
            take.mark("closing")
            await take.pause(5)
        }
    }

    /// One story about access, as a user who may read but not exec: the
    /// shell is refused, Auth Doctor shows the login is fine, and RBAC Can I,
    /// filled in on camera, says exec is denied and logs are allowed.
    func testRecordAccessPreviewFootage() async throws {
        let viewer = try PreviewViewer.makeKubeconfig()
        try await record("access", steps: [
            (.authDoctor, 0),
            (.rbacCanI, 0)
        ], context: PreviewViewer.context, extraKubeconfigs: [viewer]) { step, take in
            let viewModel = take.viewModel
            let state = take.state
            switch step {
            case .authDoctor:
                // A wide shell for the pod, so the whole refusal fits on a line.
                await take.fixture.selectLogMatrixPod()
                viewModel.isDetailPaneVisible = false
                viewModel.setSection(.terminal)
                await take.pause(1.2)
                take.mark("denied")
                let name = RuneAppStoreCaptureFixture.podName
                if !state.terminalSessions.contains(where: { $0.podName == name }),
                   let pod = state.pods.first(where: { $0.name == name }) {
                    viewModel.startTerminalSession(for: pod)
                }
                await take.pause(5.5)
                viewModel.isDetailPaneVisible = true
                viewModel.setSection(.overview)
                await take.pause(0.8)
                take.mark("authDoctorRun")
                viewModel.runAuthDoctor()
                await take.pause(6)
            case .rbacCanI:
                // The scenario's own check ran first; start from empty fields.
                viewModel.rbacCanIVerb = ""
                viewModel.rbacCanIResource = ""
                viewModel.rbacCanISubresource = ""
                await take.pause(1.2)
                take.mark("canIExec")
                await take.pause(0.6)
                await take.fill(\.rbacCanIVerb, "create")
                await take.fill(\.rbacCanIResource, "pods")
                await take.fill(\.rbacCanISubresource, "exec")
                await take.pause(0.5)
                viewModel.runRBACCanISimulator()
                await take.pause(5)
                take.mark("canILogs")
                await take.pause(0.4)
                viewModel.rbacCanIVerb = ""
                viewModel.rbacCanISubresource = ""
                await take.fill(\.rbacCanIVerb, "get")
                await take.fill(\.rbacCanISubresource, "log")
                await take.pause(0.5)
                viewModel.runRBACCanISimulator()
                await take.pause(4.5)
            default:
                break
            }
        } after: { take in
            take.mark("closing")
            await take.pause(5)
        }
    }

    private typealias Steps = [(RuneRootLiveDebugScenarioStep, TimeInterval)]

    /// Runs the scenario's steps while recording. `during` runs once each step
    /// has settled, after its marker; the step's time is recorded after it.
    /// `after` runs once the scenario is done.
    private func record(
        _ name: String,
        steps: Steps,
        context: String = "fake-orbit-mesh",
        extraKubeconfigs: [URL] = [],
        prepare: (RuneAppStoreCaptureFixture) -> Void = { _ in },
        during: @escaping @MainActor (RuneRootLiveDebugScenarioStep, PreviewTake) async -> Void,
        after: @MainActor (PreviewTake) async throws -> Void
    ) async throws {
        let fixture = try await RuneAppStoreCaptureFixture(
            self,
            outputVariable: "RUNE_UI_APPPREVIEW_DIR",
            steps: steps.map(\.0),
            windowSize: NSSize(width: 1440, height: 810),
            context: context,
            extraKubeconfigs: extraKubeconfigs
        )
        prepare(fixture)
        let movie = fixture.output.appendingPathComponent("\(name)-recording.mov")
        try? FileManager.default.removeItem(at: movie)

        var settled = 0
        var current: PreviewTake?
        RuneRootLayoutDebug.liveScenarioSnapshotHandler = { step in
            guard let take = current, let dwell = steps.first(where: { $0.0 == step })?.1 else { return }
            await fixture.keepWindowSize()
            if step == .workloadPodLogs {
                await fixture.selectLogMatrixPod()
            }
            take.mark(step.rawValue)
            await during(step, take)
            await take.pause(dwell)
            settled += 1
        }
        defer { RuneRootLayoutDebug.liveScenarioSnapshotHandler = nil }

        // The recorder is in place before the scenario's first step can run.
        let window = fixture.openWindow()
        defer { fixture.closeWindow() }
        let recorder = try RuneWindowRecorder(url: movie, windowID: CGWindowID(window.windowNumber))
        let take = PreviewTake(fixture: fixture, recorder: recorder, window: window)
        current = take
        recorder.start()
        await fixture.keepWindowSize()

        try await fixture.waitUntil(timeout: 400) { settled == steps.count }
        try await after(take)

        let result = await recorder.finish()
        XCTAssertNil(result.error, "recording finished")
        XCTAssertGreaterThan(result.frames, 300, "frames recorded")
        let markers = result.markers.map { ["name": $0.name, "seconds": $0.seconds] as [String: Any] }
        let json = try JSONSerialization.data(withJSONObject: markers, options: [.prettyPrinted, .sortedKeys])
        try json.write(to: fixture.output.appendingPathComponent("\(name)-markers.json"))
        let names = Set(result.markers.map(\.name))
        XCTAssertTrue(names.isSuperset(of: steps.map(\.0.rawValue) + ["closing"]), "scene markers")
    }
}

/// A user of the orbit test cluster who may read everything but not exec,
/// for the access preview. Made with kubectl against the local Docker
/// Compose stack only; the kubeconfig goes to a temporary folder.
@MainActor
enum PreviewViewer {
    static let context = "fake-orbit-viewer"
    private static let manifest = """
    apiVersion: v1
    kind: ServiceAccount
    metadata:
      name: preview-viewer
      namespace: kube-system
    ---
    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRole
    metadata:
      name: preview-viewer
    rules:
      - apiGroups: ["*"]
        resources: ["*"]
        verbs: ["get", "list", "watch"]
    ---
    apiVersion: rbac.authorization.k8s.io/v1
    kind: ClusterRoleBinding
    metadata:
      name: preview-viewer
    subjects:
      - kind: ServiceAccount
        name: preview-viewer
        namespace: kube-system
    roleRef:
      apiGroup: rbac.authorization.k8s.io
      kind: ClusterRole
      name: preview-viewer
    """

    /// Skips, before touching any cluster, unless the preview capture is
    /// switched on and the kubeconfig is the local-only fake-cluster one.
    static func makeKubeconfig() throws -> URL {
        let environment = ProcessInfo.processInfo.environment
        guard environment["RUNE_UI_APPPREVIEW_DIR"]?.isEmpty == false,
              environment["RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS"] == "1" else {
            throw XCTSkip("Set RUNE_UI_APPPREVIEW_DIR and RUNE_RUN_LOCAL_K8S_INTEGRATION_TESTS=1 to record the preview.")
        }
        let admin = RuneAppStoreCaptureFixture.repoRoot
            .appendingPathComponent("docker-compose/generated/rune-fake-kubeconfig.yaml").path
        guard let contents = try? String(contentsOfFile: admin, encoding: .utf8),
              contents.contains("server: https://127.0.0.1:16443") else {
            throw XCTSkip("The Docker Compose kubeconfig is not the local-only fake-cluster fixture.")
        }
        let base = ["--kubeconfig", admin, "--context", "fake-orbit-mesh"]
        _ = try kubectl(base + ["apply", "-f", "-"], input: manifest)
        let token = try kubectl(base + ["create", "token", "preview-viewer", "-n", "kube-system", "--duration=1h"])
            .trimmingCharacters(in: .whitespacesAndNewlines)
        let config = """
        apiVersion: v1
        kind: Config
        clusters:
          - name: \(context)
            cluster:
              server: https://127.0.0.1:16443
              insecure-skip-tls-verify: true
        users:
          - name: \(context)
            user:
              token: \(token)
        contexts:
          - name: \(context)
            context:
              cluster: \(context)
              user: \(context)
              namespace: alpha-zone
        current-context: \(context)
        """
        let folder = FileManager.default.temporaryDirectory.appendingPathComponent("rune-preview-viewer-\(UUID().uuidString)")
        try FileManager.default.createDirectory(at: folder, withIntermediateDirectories: true)
        let url = folder.appendingPathComponent("viewer-kubeconfig.yaml")
        try config.write(to: url, atomically: true, encoding: .utf8)
        return url
    }

    private static func kubectl(_ arguments: [String], input: String? = nil) throws -> String {
        guard let path = ["/opt/homebrew/bin/kubectl", "/usr/local/bin/kubectl"]
            .first(where: FileManager.default.isExecutableFile(atPath:)) else {
            throw XCTSkip("kubectl is needed to make the read-only preview user.")
        }
        let process = Process()
        process.executableURL = URL(fileURLWithPath: path)
        process.arguments = arguments
        let output = Pipe()
        process.standardOutput = output
        process.standardError = Pipe()
        let stdin = Pipe()
        process.standardInput = stdin
        try process.run()
        if let input { stdin.fileHandleForWriting.write(Data(input.utf8)) }
        try stdin.fileHandleForWriting.close()
        let data = output.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else {
            throw XCTSkip("kubectl \(arguments.dropFirst(4).joined(separator: " ")) failed.")
        }
        return String(decoding: data, as: UTF8.self)
    }
}

/// What a preview's script works with while it is being recorded.
@MainActor
final class PreviewTake {
    let fixture: RuneAppStoreCaptureFixture
    let recorder: RuneWindowRecorder
    let window: NSWindow

    init(fixture: RuneAppStoreCaptureFixture, recorder: RuneWindowRecorder, window: NSWindow) {
        self.fixture = fixture
        self.recorder = recorder
        self.window = window
    }

    var viewModel: RuneAppViewModel { fixture.viewModel }
    var state: RuneAppState { fixture.state }

    func mark(_ name: String) {
        recorder.mark(name)
    }

    func pause(_ seconds: TimeInterval) async {
        guard seconds > 0 else { return }
        try? await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
    }

    /// Types into one of the view model's text fields a character at a time.
    func fill(_ field: ReferenceWritableKeyPath<RuneAppViewModel, String>, _ text: String, every seconds: TimeInterval = 0.11) async {
        for character in text {
            viewModel[keyPath: field].append(character)
            await pause(seconds)
        }
        await pause(0.25)
    }

    func typeSlowly(_ text: String, every seconds: TimeInterval) async {
        for character in text {
            press(String(character), keyCode: 0)
            await pause(seconds)
        }
    }

    /// A key press delivered to the window that has keyboard focus, as if
    /// typed: needs no accessibility permission because it stays in process.
    /// A shortcut with modifiers goes through the window's key equivalents
    /// first, as a menu or button shortcut would.
    func press(_ characters: String, keyCode: UInt16, modifiers: NSEvent.ModifierFlags = []) {
        let target = window.attachedSheet ?? NSApp.keyWindow ?? window
        for type in [NSEvent.EventType.keyDown, .keyUp] {
            guard let event = NSEvent.keyEvent(
                with: type,
                location: .zero,
                modifierFlags: modifiers,
                timestamp: ProcessInfo.processInfo.systemUptime,
                windowNumber: target.windowNumber,
                context: nil,
                characters: characters,
                charactersIgnoringModifiers: characters,
                isARepeat: false,
                keyCode: keyCode
            ) else { continue }
            if type == .keyDown, !modifiers.isEmpty, target.performKeyEquivalent(with: event) {
                continue
            }
            target.sendEvent(event)
        }
    }
}

/// Records one window of this process, with any sheet, popover or menu of
/// the process above it, into a 2880 by 1620 H.264 movie on a background
/// thread. `CGImage(windowListFromArrayScreenBounds:)` of the process's own
/// windows needs no screen recording permission.
final class RuneWindowRecorder: @unchecked Sendable {
    struct Marker: Sendable {
        let name: String
        let seconds: Double
    }

    struct Result: Sendable {
        let frames: Int
        let markers: [Marker]
        let error: String?
    }

    private static let size = CGSize(width: 2880, height: 1620)
    private static let frameInterval = 1.0 / 30

    private let writer: AVAssetWriter
    private let input: AVAssetWriterInput
    private let adaptor: AVAssetWriterInputPixelBufferAdaptor
    private let windowID: CGWindowID
    private let lock = NSLock()
    private let stopped = DispatchSemaphore(value: 0)
    private var isRunning = false
    private var origin: CFTimeInterval = 0
    private var markers: [Marker] = []
    private var frames = 0

    init(url: URL, windowID: CGWindowID) throws {
        self.windowID = windowID
        writer = try AVAssetWriter(outputURL: url, fileType: .mov)
        input = AVAssetWriterInput(mediaType: .video, outputSettings: [
            AVVideoCodecKey: AVVideoCodecType.h264,
            AVVideoWidthKey: Int(Self.size.width),
            AVVideoHeightKey: Int(Self.size.height),
            AVVideoCompressionPropertiesKey: [
                AVVideoAverageBitRateKey: 60_000_000,
                AVVideoProfileLevelKey: AVVideoProfileLevelH264HighAutoLevel
            ]
        ])
        input.expectsMediaDataInRealTime = true
        adaptor = AVAssetWriterInputPixelBufferAdaptor(assetWriterInput: input, sourcePixelBufferAttributes: [
            kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32BGRA,
            kCVPixelBufferWidthKey as String: Int(Self.size.width),
            kCVPixelBufferHeightKey as String: Int(Self.size.height)
        ])
        writer.add(input)
    }

    func start() {
        writer.startWriting()
        writer.startSession(atSourceTime: .zero)
        lock.withLock {
            isRunning = true
            origin = CACurrentMediaTime()
        }
        let thread = Thread { [self] in run() }
        thread.qualityOfService = .userInitiated
        thread.start()
    }

    func mark(_ name: String) {
        lock.withLock {
            markers.append(Marker(name: name, seconds: CACurrentMediaTime() - origin))
        }
    }

    func finish() async -> Result {
        lock.withLock { isRunning = false }
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            DispatchQueue.global().async { [stopped] in
                stopped.wait()
                continuation.resume()
            }
        }
        input.markAsFinished()
        await writer.finishWriting()
        return lock.withLock {
            Result(frames: frames, markers: markers, error: writer.error.map { "\($0)" })
        }
    }

    private func run() {
        var next = CACurrentMediaTime()
        var last = -1.0
        while lock.withLock({ isRunning }) {
            let now = CACurrentMediaTime()
            let seconds = now - lock.withLock { origin }
            if seconds > last, input.isReadyForMoreMediaData, let image = capture(),
               let buffer = pixelBuffer(drawing: image) {
                if adaptor.append(buffer, withPresentationTime: CMTime(seconds: seconds, preferredTimescale: 6000)) {
                    last = seconds
                    lock.withLock { frames += 1 }
                }
            }
            next += Self.frameInterval
            let wait = next - CACurrentMediaTime()
            if wait > 0 {
                Thread.sleep(forTimeInterval: wait)
            } else {
                next = CACurrentMediaTime()
            }
        }
        stopped.signal()
    }

    /// The window and the process's other on-screen windows inside its
    /// bounds, front to back as the window server lists them.
    private func capture() -> CGImage? {
        let process = getpid()
        guard let list = CGWindowListCopyWindowInfo([.optionOnScreenOnly], kCGNullWindowID) as? [[String: Any]] else {
            return nil
        }
        var windows: [CGWindowID] = []
        var bounds: CGRect?
        for entry in list where (entry[kCGWindowOwnerPID as String] as? Int32) == process {
            guard let number = entry[kCGWindowNumber as String] as? CGWindowID else { continue }
            windows.append(number)
            if number == windowID, let frame = entry[kCGWindowBounds as String] as? NSDictionary {
                bounds = CGRect(dictionaryRepresentation: frame as CFDictionary)
            }
        }
        guard let bounds, !windows.isEmpty else { return nil }
        var pointers = windows.map { UnsafeRawPointer(bitPattern: UInt($0)) }
        guard let array = CFArrayCreate(nil, &pointers, pointers.count, nil) else { return nil }
        return CGImage(windowListFromArrayScreenBounds: bounds, windowArray: array, imageOption: [.bestResolution])
    }

    private func pixelBuffer(drawing image: CGImage) -> CVPixelBuffer? {
        guard let pool = adaptor.pixelBufferPool else { return nil }
        var created: CVPixelBuffer?
        CVPixelBufferPoolCreatePixelBuffer(nil, pool, &created)
        guard let buffer = created else { return nil }
        CVPixelBufferLockBaseAddress(buffer, [])
        defer { CVPixelBufferUnlockBaseAddress(buffer, []) }
        guard let context = CGContext(
            data: CVPixelBufferGetBaseAddress(buffer),
            width: Int(Self.size.width),
            height: Int(Self.size.height),
            bitsPerComponent: 8,
            bytesPerRow: CVPixelBufferGetBytesPerRow(buffer),
            space: CGColorSpace(name: CGColorSpace.sRGB) ?? CGColorSpaceCreateDeviceRGB(),
            bitmapInfo: CGImageAlphaInfo.premultipliedFirst.rawValue | CGBitmapInfo.byteOrder32Little.rawValue
        ) else { return nil }
        let frame = CGRect(origin: .zero, size: Self.size)
        context.setFillColor(CGColor(red: 0.04, green: 0.03, blue: 0.03, alpha: 1))
        context.fill(frame)
        context.interpolationQuality = .high
        context.draw(image, in: frame)
        return buffer
    }
}
#endif
