// The scripted scenario it drives exists only in debug builds.
#if DEBUG
import AppKit
import AVFoundation
import CoreVideo
@testable import RuneUI
import XCTest

/// Footage for the App Store App Preview: the scripted scenario against the
/// local Docker Compose K3s clusters, recorded from Rune's own windows at
/// 1920 by 1080 (a 1440 by 810 point window, 16 by 9). Needs no screen
/// recording permission. Writes `app-preview-recording.mov` and
/// `app-preview-markers.json` (where each scene starts, in seconds); the
/// marketing renderer cuts the preview from them.
///
/// Runs only when `RUNE_UI_APPPREVIEW_DIR` is set; see
/// `RuneAppStoreCaptureFixture` for the rest. Run it on its own.
@MainActor
final class RuneAppPreviewCaptureTests: XCTestCase {
    /// Each step and how long to keep recording once it has settled.
    private static let steps: [(RuneRootLiveDebugScenarioStep, TimeInterval)] = [
        (.overview, 4),
        (.workloadPodLogs, 6),
        (.workloadDeploymentUnifiedLogs, 5),
        (.terminalLogs, 7),
        (.workloadDeploymentYAMLValidation, 4),
        (.authDoctor, 6),
        (.rbacCanI, 4)
    ]

    func testRecordAppPreviewFootage() async throws {
        let fixture = try await RuneAppStoreCaptureFixture(
            self,
            outputVariable: "RUNE_UI_APPPREVIEW_DIR",
            steps: Self.steps.map(\.0),
            windowSize: NSSize(width: 1440, height: 810)
        )
        let viewModel = fixture.viewModel
        let state = fixture.state
        let movie = fixture.output.appendingPathComponent("app-preview-recording.mov")
        try? FileManager.default.removeItem(at: movie)

        var settled: [RuneRootLiveDebugScenarioStep] = []
        var recorder: RuneWindowRecorder?
        RuneRootLayoutDebug.liveScenarioSnapshotHandler = { step in
            guard let recorder, let dwell = Self.steps.first(where: { $0.0 == step })?.1 else { return }
            await fixture.keepWindowSize()
            if step == .workloadPodLogs {
                await fixture.selectLogMatrixPod()
            }
            recorder.mark(step.rawValue)
            switch step {
            case .terminalLogs:
                try? await Task.sleep(nanoseconds: 700_000_000)
                for character in "ps -o pid,comm" {
                    viewModel.terminalSessionInput.append(character)
                    try? await Task.sleep(nanoseconds: 90_000_000)
                }
                viewModel.sendTerminalSessionInput()
            case .authDoctor:
                // Run it again on camera so the checks fill in.
                try? await Task.sleep(nanoseconds: 500_000_000)
                viewModel.runAuthDoctor()
            default:
                break
            }
            try? await Task.sleep(nanoseconds: UInt64(dwell * 1_000_000_000))
            settled.append(step)
        }
        defer { RuneRootLayoutDebug.liveScenarioSnapshotHandler = nil }

        // The recorder is in place before the scenario's first step can run.
        let window = fixture.openWindow()
        defer { fixture.closeWindow() }
        let started = try RuneWindowRecorder(url: movie, windowID: CGWindowID(window.windowNumber))
        recorder = started
        started.start()
        await fixture.keepWindowSize()

        try await fixture.waitUntil(timeout: 400) { settled.count == Self.steps.count }

        // The command palette, typed into as a person would.
        viewModel.setSection(.workloads)
        viewModel.setWorkloadKind(.pod)
        try await Task.sleep(nanoseconds: 1_500_000_000)
        started.mark("commandPalette")
        viewModel.presentCommandPalette()
        try await Task.sleep(nanoseconds: 700_000_000)
        for character in ":po log" {
            type(String(character), keyCode: 0, near: window)
            try await Task.sleep(nanoseconds: 140_000_000)
        }
        try await Task.sleep(nanoseconds: 1_400_000_000)
        type("\r", keyCode: 36, near: window)
        try await Task.sleep(nanoseconds: 3_000_000_000)
        if state.isCommandPalettePresented { viewModel.dismissCommandPalette() }

        // The overview again, for the closing scene.
        viewModel.setSection(.overview)
        try await Task.sleep(nanoseconds: 1_500_000_000)
        started.mark("closing")
        try await Task.sleep(nanoseconds: 5_000_000_000)

        let result = await started.finish()
        XCTAssertNil(result.error, "recording finished")
        XCTAssertGreaterThan(result.frames, 400, "frames recorded")
        let markers = result.markers.map { ["name": $0.name, "seconds": $0.seconds] as [String: Any] }
        let json = try JSONSerialization.data(withJSONObject: markers, options: [.prettyPrinted, .sortedKeys])
        try json.write(to: fixture.output.appendingPathComponent("app-preview-markers.json"))
        let names = Set(result.markers.map(\.name))
        XCTAssertEqual(names, Set(Self.steps.map(\.0.rawValue) + ["commandPalette", "closing"]), "scene markers")
    }

    /// A key press delivered to the window that has keyboard focus, as if
    /// typed: needs no accessibility permission because it stays in process.
    private func type(_ characters: String, keyCode: UInt16, near window: NSWindow) {
        let target = window.attachedSheet ?? NSApp.keyWindow ?? window
        for type in [NSEvent.EventType.keyDown, .keyUp] {
            guard let event = NSEvent.keyEvent(
                with: type,
                location: .zero,
                modifierFlags: [],
                timestamp: ProcessInfo.processInfo.systemUptime,
                windowNumber: target.windowNumber,
                context: nil,
                characters: characters,
                charactersIgnoringModifiers: characters,
                isARepeat: false,
                keyCode: keyCode
            ) else { continue }
            target.sendEvent(event)
        }
    }
}

/// Records one window of this process, with any sheet, popover or menu of
/// the process above it, into a 1920 by 1080 H.264 movie on a background
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

    private static let size = CGSize(width: 1920, height: 1080)
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
                AVVideoAverageBitRateKey: 40_000_000,
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
