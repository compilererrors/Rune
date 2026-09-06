import AppKit
import ApplicationServices
import CoreGraphics
import Foundation

private enum SmokeAXError: Error, CustomStringConvertible {
    case invalidArguments
    case applicationUnavailable(pid_t)
    case elementMissing(String)
    case accessibilityFailure(String, AXError)
    case assertion(String)

    var description: String {
        switch self {
        case .invalidArguments:
            return "usage: rune-ui-smoke-ax <pid> <focus|log-search|log-export|settings-sort|enable-skip-cluster|skip-cluster-nav|compare|write-dialog|yaml-dialog|import-kubeconfig|reimport-kubeconfig>"
        case let .applicationUnavailable(pid):
            return "Rune process is unavailable: \(pid)"
        case let .elementMissing(identifier):
            return "Accessibility element not found: \(identifier)"
        case let .accessibilityFailure(operation, error):
            return "Accessibility operation failed (\(operation)): \(error.rawValue)"
        case let .assertion(message):
            return message
        }
    }
}

private func attribute(_ element: AXUIElement, _ name: String) -> CFTypeRef? {
    var value: CFTypeRef?
    guard AXUIElementCopyAttributeValue(element, name as CFString, &value) == .success else { return nil }
    return value
}

private func stringAttribute(_ element: AXUIElement, _ name: String) -> String? {
    attribute(element, name) as? String
}

private func boolAttribute(_ element: AXUIElement, _ name: String) -> Bool {
    (attribute(element, name) as? NSNumber)?.boolValue == true
}

private struct ElementFrame: Equatable {
    let origin: CGPoint
    let size: CGSize
}

private func pointAttribute(_ element: AXUIElement, _ name: String) -> CGPoint? {
    guard let value = attribute(element, name), CFGetTypeID(value) == AXValueGetTypeID() else {
        return nil
    }
    var point = CGPoint.zero
    guard AXValueGetValue(value as! AXValue, .cgPoint, &point) else { return nil }
    return point
}

private func sizeAttribute(_ element: AXUIElement, _ name: String) -> CGSize? {
    guard let value = attribute(element, name), CFGetTypeID(value) == AXValueGetTypeID() else {
        return nil
    }
    var size = CGSize.zero
    guard AXValueGetValue(value as! AXValue, .cgSize, &size) else { return nil }
    return size
}

private func frame(of element: AXUIElement) -> ElementFrame? {
    guard let origin = pointAttribute(element, kAXPositionAttribute),
          let size = sizeAttribute(element, kAXSizeAttribute)
    else { return nil }
    return ElementFrame(origin: origin, size: size)
}

private func children(of element: AXUIElement) -> [AXUIElement] {
    attribute(element, kAXChildrenAttribute) as? [AXUIElement] ?? []
}

private func elementAttribute(_ element: AXUIElement, _ name: String) -> AXUIElement? {
    guard let value = attribute(element, name),
          CFGetTypeID(value) == AXUIElementGetTypeID()
    else { return nil }
    return unsafeBitCast(value, to: AXUIElement.self)
}

private func applicationWindows(_ root: AXUIElement) -> [AXUIElement] {
    var stack = attribute(root, kAXWindowsAttribute) as? [AXUIElement] ?? []
    if let focusedWindow = elementAttribute(root, kAXFocusedWindowAttribute) {
        stack.append(focusedWindow)
    }
    if let mainWindow = elementAttribute(root, kAXMainWindowAttribute) {
        stack.append(mainWindow)
    }
    stack.append(contentsOf: children(of: root))
    var visited = AXElementIdentitySet()
    var windows: [AXUIElement] = []
    _ = visited.insert(root)

    while let candidate = stack.popLast(), windows.count < 32 {
        guard !CFEqual(candidate, root), visited.insert(candidate) else { continue }
        if stringAttribute(candidate, kAXRoleAttribute) == (kAXWindowRole as String) {
            windows.append(candidate)
            continue
        }
        stack.append(contentsOf: children(of: candidate).reversed())
    }
    return windows
}

private func traversalRoots(for root: AXUIElement) -> [AXUIElement] {
    guard stringAttribute(root, kAXRoleAttribute) == (kAXApplicationRole as String) else {
        return [root]
    }
    return [root] + applicationWindows(root)
}

private struct AXElementIdentitySet {
    private var buckets: [CFHashCode: [AXUIElement]] = [:]

    mutating func insert(_ element: AXUIElement) -> Bool {
        let hash = CFHash(element)
        if buckets[hash]?.contains(where: { CFEqual($0, element) }) == true {
            return false
        }
        buckets[hash, default: []].append(element)
        return true
    }
}

private func firstElement(
    in root: AXUIElement,
    limit: Int = 8_000,
    matching predicate: (AXUIElement) -> Bool
) -> AXUIElement? {
    var stack = traversalRoots(for: root)
    var visited = AXElementIdentitySet()
    var inspected = 0

    while let element = stack.popLast(), inspected < limit {
        guard visited.insert(element) else { continue }
        inspected += 1
        if predicate(element) { return element }
        stack.append(contentsOf: children(of: element).reversed())
    }
    return nil
}

private func findElement(
    in root: AXUIElement,
    identifier: String,
    fallbackLabel: String? = nil
) -> AXUIElement? {
    firstElement(in: root) { element in
        if stringAttribute(element, kAXIdentifierAttribute) == identifier {
            return true
        }

        if let fallbackLabel {
            let role = stringAttribute(element, kAXRoleAttribute)
            let labels = [
                stringAttribute(element, kAXTitleAttribute),
                stringAttribute(element, kAXDescriptionAttribute),
                stringAttribute(element, kAXHelpAttribute),
            ]
            if role == (kAXTextFieldRole as String), labels.compactMap({ $0 }).contains(fallbackLabel) {
                return true
            }
        }
        return false
    }
}

private func waitForElement(
    in root: AXUIElement,
    identifier: String,
    fallbackLabel: String? = nil,
    timeout: TimeInterval = 3
) -> AXUIElement? {
    let deadline = Date().addingTimeInterval(timeout)
    repeat {
        if let element = findElement(in: root, identifier: identifier, fallbackLabel: fallbackLabel) {
            return element
        }
        Thread.sleep(forTimeInterval: 0.05)
    } while Date() < deadline
    return nil
}

private func findButton(in root: AXUIElement, label: String) -> AXUIElement? {
    firstElement(in: root) { element in
        if stringAttribute(element, kAXRoleAttribute) == (kAXButtonRole as String) {
            let labels = [
                stringAttribute(element, kAXTitleAttribute),
                stringAttribute(element, kAXDescriptionAttribute),
                stringAttribute(element, kAXHelpAttribute),
            ]
            if labels.compactMap({ $0 }).contains(label) {
                return true
            }
        }
        return false
    }
}

private func findLabeledElement(
    in root: AXUIElement,
    label: String,
    role: String? = nil,
    contains: Bool = false
) -> AXUIElement? {
    firstElement(in: root) { element in
        let elementRole = stringAttribute(element, kAXRoleAttribute)
        if role == nil || elementRole == role {
            let labels = [
                stringAttribute(element, kAXTitleAttribute),
                stringAttribute(element, kAXDescriptionAttribute),
                stringAttribute(element, kAXValueAttribute),
                stringAttribute(element, kAXHelpAttribute),
            ].compactMap { $0 }
            if labels.contains(where: { contains ? $0.contains(label) : $0 == label }) {
                return true
            }
        }
        return false
    }
}

private func waitForLabeledElement(
    in root: AXUIElement,
    label: String,
    role: String? = nil,
    contains: Bool = false,
    timeout: TimeInterval = 3
) -> AXUIElement? {
    let deadline = Date().addingTimeInterval(timeout)
    repeat {
        if let element = findLabeledElement(in: root, label: label, role: role, contains: contains) {
            return element
        }
        Thread.sleep(forTimeInterval: 0.05)
    } while Date() < deadline
    return nil
}

private func performPress(_ element: AXUIElement, operation: String) throws {
    let result = AXUIElementPerformAction(element, kAXPressAction as CFString)
    guard result == .success else {
        throw SmokeAXError.accessibilityFailure(operation, result)
    }
}

private func postKey(
    _ keyCode: CGKeyCode,
    flags: CGEventFlags = [],
    to processID: pid_t
) throws {
    guard let keyDown = CGEvent(keyboardEventSource: nil, virtualKey: keyCode, keyDown: true),
          let keyUp = CGEvent(keyboardEventSource: nil, virtualKey: keyCode, keyDown: false)
    else {
        throw SmokeAXError.assertion("Could not create key event for code \(keyCode)")
    }
    keyDown.flags = flags
    keyUp.flags = flags
    keyDown.postToPid(processID)
    keyUp.postToPid(processID)
    Thread.sleep(forTimeInterval: 0.10)
}

private func isSelected(_ element: AXUIElement) -> Bool {
    if boolAttribute(element, kAXSelectedAttribute) { return true }
    if (attribute(element, kAXValueAttribute) as? NSNumber)?.boolValue == true { return true }
    return stringAttribute(element, kAXValueAttribute) == "Selected"
}

private func waitForSelectedChoice(
    in root: AXUIElement,
    label: String,
    timeout: TimeInterval = 3
) -> AXUIElement? {
    let deadline = Date().addingTimeInterval(timeout)
    repeat {
        if let element = findLabeledElement(in: root, label: label), isSelected(element) {
            return element
        }
        Thread.sleep(forTimeInterval: 0.05)
    } while Date() < deadline
    return nil
}

private func keyCode(for character: Character) -> CGKeyCode? {
    switch character {
    case "t": return 17
    case "i": return 34
    case "c": return 8
    case "k": return 40
    default: return nil
    }
}

private func type(_ text: String, into processID: pid_t) throws {
    for character in text {
        guard let keyCode = keyCode(for: character) else {
            throw SmokeAXError.assertion("Unsupported smoke-test character: \(character)")
        }
        try postKey(keyCode, to: processID)
    }
}

private func accessibilityLabel(of element: AXUIElement) -> String {
    [kAXDescriptionAttribute, kAXTitleAttribute, kAXValueAttribute, kAXHelpAttribute]
        .compactMap { stringAttribute(element, $0) }
        .first { !$0.isEmpty } ?? ""
}

private func logSearchStatusLabel(in root: AXUIElement) -> String {
    if let status = findElement(in: root, identifier: "resource-log-search-match-status") {
        let label = accessibilityLabel(of: status)
        if !label.isEmpty { return label }
    }
    return findLabeledElement(
        in: root,
        label: "Search match ",
        contains: true
    ).map(accessibilityLabel(of:)) ?? ""
}

private struct OnScreenLayerZeroWindow {
    let identifier: CGWindowID
    let bounds: CGRect
    let name: String
}

private func onScreenLayerZeroWindow(processID: pid_t) -> OnScreenLayerZeroWindow? {
    guard let descriptions = CGWindowListCopyWindowInfo(
        [.optionOnScreenOnly, .excludeDesktopElements],
        kCGNullWindowID
    ) as? [[String: Any]] else { return nil }

    let windows = descriptions.compactMap { description -> OnScreenLayerZeroWindow? in
        guard (description[kCGWindowOwnerPID as String] as? NSNumber)?.int32Value == processID,
              (description[kCGWindowLayer as String] as? NSNumber)?.intValue == 0,
              (description[kCGWindowIsOnscreen as String] as? NSNumber)?.boolValue == true,
              let identifier = (description[kCGWindowNumber as String] as? NSNumber)?.uint32Value,
              let rawBounds = description[kCGWindowBounds as String] as? [String: Any],
              let x = (rawBounds["X"] as? NSNumber)?.doubleValue,
              let y = (rawBounds["Y"] as? NSNumber)?.doubleValue,
              let width = (rawBounds["Width"] as? NSNumber)?.doubleValue,
              let height = (rawBounds["Height"] as? NSNumber)?.doubleValue,
              width >= 200,
              height >= 200
        else { return nil }

        return OnScreenLayerZeroWindow(
            identifier: identifier,
            bounds: CGRect(x: x, y: y, width: width, height: height),
            name: description[kCGWindowName as String] as? String ?? ""
        )
    }

    return windows.sorted { lhs, rhs in
        let lhsIsRune = lhs.name == "Rune"
        let rhsIsRune = rhs.name == "Rune"
        if lhsIsRune != rhsIsRune { return lhsIsRune }
        return lhs.bounds.width * lhs.bounds.height > rhs.bounds.width * rhs.bounds.height
    }.first
}

private func clickTitlebarOfOnScreenLayerZeroWindow(processID: pid_t) -> CGWindowID? {
    guard let window = onScreenLayerZeroWindow(processID: processID) else { return nil }
    let point = CGPoint(x: window.bounds.midX, y: window.bounds.minY + 14)
    guard window.bounds.contains(point),
          let source = CGEventSource(stateID: .hidSystemState),
          let move = CGEvent(
              mouseEventSource: source,
              mouseType: .mouseMoved,
              mouseCursorPosition: point,
              mouseButton: .left
          ),
          let down = CGEvent(
              mouseEventSource: source,
              mouseType: .leftMouseDown,
              mouseCursorPosition: point,
              mouseButton: .left
          ),
          let up = CGEvent(
              mouseEventSource: source,
              mouseType: .leftMouseUp,
              mouseCursorPosition: point,
              mouseButton: .left
          )
    else { return nil }

    move.post(tap: .cghidEventTap)
    down.post(tap: .cghidEventTap)
    up.post(tap: .cghidEventTap)
    return window.identifier
}

private func containsImportedKubeconfig(at rootPath: String) -> Bool {
    guard let enumerator = FileManager.default.enumerator(atPath: rootPath) else { return false }
    while let relativePath = enumerator.nextObject() as? String {
        if relativePath.hasSuffix(".yaml") || relativePath.hasSuffix(".yml") {
            return true
        }
    }
    return false
}

private func containsActivatedContextProof(at path: String) -> Bool {
    guard let raw = try? String(contentsOfFile: path, encoding: .utf8) else { return false }
    return raw.contains("\"alpha-zone\"") && raw.contains("\"bravo-zone\"")
}

private func assertKubeconfigImportWithNativeMenu(processID: pid_t) throws {
    guard let importRoot = ProcessInfo.processInfo.environment["RUNE_UI_SMOKE_IMPORT_ROOT"],
          !importRoot.isEmpty else {
        throw SmokeAXError.assertion("RUNE_UI_SMOKE_IMPORT_ROOT is required for native-menu import fallback")
    }
    guard let activationProof = ProcessInfo.processInfo.environment["RUNE_UI_SMOKE_ACTIVATION_PROOF"],
          !activationProof.isEmpty else {
        throw SmokeAXError.assertion("RUNE_UI_SMOKE_ACTIVATION_PROOF is required for native-menu import fallback")
    }

    let root = AXUIElementCreateApplication(processID)
    guard let pasteCommand = waitForLabeledElement(
        in: root,
        label: "Paste Kubeconfig",
        role: kAXMenuItemRole as String,
        timeout: 5
    ) else {
        throw SmokeAXError.elementMissing("native Paste Kubeconfig menu item")
    }
    try performPress(pasteCommand, operation: "invoke native Paste Kubeconfig command")
    Thread.sleep(forTimeInterval: 0.50)

    let deadline = Date().addingTimeInterval(35)
    var importPublished = false
    repeat {
        if !importPublished {
            importPublished = containsImportedKubeconfig(at: importRoot)
        }
        if !importPublished {
            try postKey(36, to: processID) // Return confirms the review's default action.
            importPublished = containsImportedKubeconfig(at: importRoot)
        }
        if importPublished, containsActivatedContextProof(at: activationProof) {
            print("import-kubeconfig-e2e passed route=native-menu-default-action source=app-owned app-context-loaded=1")
            return
        }
        Thread.sleep(forTimeInterval: 0.25)
    } while Date() < deadline
    throw SmokeAXError.assertion(
        "Native-menu kubeconfig import did not publish and load the activated app-owned context"
    )
}

private func assertLogSearch(processID: pid_t) throws {
    guard let application = NSRunningApplication(processIdentifier: processID), !application.isTerminated else {
        throw SmokeAXError.applicationUnavailable(processID)
    }
    application.activate()

    let root = AXUIElementCreateApplication(processID)
    // AppKit can flatten the SwiftUI container around native accessories. The
    // input identifier and the same focus/frame/navigation checks still apply.
    let searchChrome = waitForElement(
        in: root,
        identifier: "resource-log-search-chrome",
        timeout: 1
    ) ?? root
    guard let field = waitForElement(
        in: searchChrome,
        identifier: "resource-log-search-input",
        fallbackLabel: "Search logs"
    ) else {
        throw SmokeAXError.elementMissing("resource-log-search-input")
    }
    guard let originalFrame = frame(of: field) else {
        throw SmokeAXError.assertion("Log search did not expose a stable accessibility frame")
    }

    let focusResult = AXUIElementSetAttributeValue(field, kAXFocusedAttribute as CFString, kCFBooleanTrue)
    guard focusResult == .success else {
        throw SmokeAXError.accessibilityFailure("focus log search", focusResult)
    }
    Thread.sleep(forTimeInterval: 0.08)

    var expected = ""
    for character in "tick" {
        try type(String(character), into: processID)
        expected.append(character)

        guard let currentField = findElement(
            in: searchChrome,
            identifier: "resource-log-search-input",
            fallbackLabel: "Search logs"
        ) else {
            throw SmokeAXError.elementMissing("resource-log-search-input after typing \(expected)")
        }
        guard CFEqual(field, currentField) else {
            throw SmokeAXError.assertion("Log search accessibility element was replaced while typing \(expected)")
        }
        guard frame(of: currentField) == originalFrame else {
            throw SmokeAXError.assertion("Log search moved or resized while typing \(expected)")
        }
        guard stringAttribute(currentField, kAXValueAttribute) == expected else {
            throw SmokeAXError.assertion("Log search value changed while typing \(expected)")
        }
        guard boolAttribute(currentField, kAXFocusedAttribute) else {
            throw SmokeAXError.assertion("Log search lost focus while typing \(expected)")
        }
    }

    let deadline = Date().addingTimeInterval(2)
    var statusLabel = ""
    repeat {
        // SwiftUI refreshes the accessibility child list when the disabled match
        // controls become enabled. Reacquire dynamic controls from the application
        // root while retaining the original field reference for the identity check.
        statusLabel = logSearchStatusLabel(in: root)
        if statusLabel.hasPrefix("Search match ") { break }
        Thread.sleep(forTimeInterval: 0.05)
    } while Date() < deadline

    guard statusLabel.hasPrefix("Search match ") else {
        let currentValue = findElement(
            in: root,
            identifier: "resource-log-search-input",
            fallbackLabel: "Search logs"
        ).flatMap { stringAttribute($0, kAXValueAttribute) } ?? "missing"
        throw SmokeAXError.assertion(
            "Log search did not publish a match position; status=\(statusLabel) fieldValue=\(currentValue)"
        )
    }
    guard let settledField = findElement(
        in: root,
        identifier: "resource-log-search-input",
        fallbackLabel: "Search logs"
    ), CFEqual(field, settledField), frame(of: settledField) == originalFrame else {
        throw SmokeAXError.assertion("Log search remounted or changed frame after match status appeared")
    }
    guard stringAttribute(settledField, kAXValueAttribute) == "tick", boolAttribute(settledField, kAXFocusedAttribute) else {
        throw SmokeAXError.assertion("Log search was not stable after match status appeared")
    }

    if let nextButton = findButton(in: root, label: "Next match") {
        try performPress(nextButton, operation: "advance to next log match")
    } else {
        guard let optionsButton = findLabeledElement(in: root, label: "Log search options") else {
            throw SmokeAXError.elementMissing("Log search options")
        }
        try performPress(optionsButton, operation: "open compact log search options")
        guard let nextMenuItem = waitForLabeledElement(
            in: root,
            label: "Next match",
            role: kAXMenuItemRole as String,
            timeout: 2
        ) else {
            throw SmokeAXError.elementMissing("Next match menu item")
        }
        try performPress(nextMenuItem, operation: "advance to next compact log match")
    }

    let navigationDeadline = Date().addingTimeInterval(2)
    var nextStatusLabel = statusLabel
    repeat {
        let candidate = logSearchStatusLabel(in: root)
        if candidate.hasPrefix("Search match "), candidate != statusLabel {
            nextStatusLabel = candidate
            break
        }
        Thread.sleep(forTimeInterval: 0.04)
    } while Date() < navigationDeadline
    guard nextStatusLabel != statusLabel, nextStatusLabel.hasPrefix("Search match ") else {
        throw SmokeAXError.assertion("Next match did not advance the match position; status=\(nextStatusLabel)")
    }

    print("log-search-e2e passed value=tick status=\(statusLabel) next=\(nextStatusLabel)")
}

private func openCommandPalette(processID: pid_t) throws {
    let root = try activateApplication(processID: processID)
    guard let button = waitForElement(
        in: root,
        identifier: "rune.command-palette.open",
        fallbackLabel: "Command Palette",
        timeout: 4
    ) else {
        throw SmokeAXError.elementMissing("rune.command-palette.open")
    }
    try performPress(button, operation: "open command palette")

    guard let input = waitForElement(
        in: root,
        identifier: "rune.command-palette.input",
        fallbackLabel: "Search commands and resources",
        timeout: 4
    ) else {
        throw SmokeAXError.elementMissing("rune.command-palette.input")
    }
    let focusResult = AXUIElementSetAttributeValue(
        input,
        kAXFocusedAttribute as CFString,
        kCFBooleanTrue
    )
    guard focusResult == .success else {
        throw SmokeAXError.accessibilityFailure("focus command palette input", focusResult)
    }
    print("command-palette-open passed")
}

private func activateApplication(processID: pid_t) throws -> AXUIElement {
    guard let application = NSRunningApplication(processIdentifier: processID), !application.isTerminated else {
        throw SmokeAXError.applicationUnavailable(processID)
    }
    if #available(macOS 14.0, *) {
        _ = application.activate(options: [.activateAllWindows])
    } else {
        _ = application.activate(options: [.activateAllWindows, .activateIgnoringOtherApps])
    }
    let root = AXUIElementCreateApplication(processID)
    _ = AXUIElementSetAttributeValue(
        root,
        kAXFrontmostAttribute as CFString,
        kCFBooleanTrue
    )

    // The app owns exactly-once WindowGroup recovery. Waiting here avoids racing
    // that recovery with a second Command-N request from the test driver.
    let windowDeadline = Date().addingTimeInterval(7)
    var titlebarFocusWindowID: CGWindowID?
    while Date() < windowDeadline {
        if !applicationWindows(root).isEmpty {
            if titlebarFocusWindowID == nil {
                titlebarFocusWindowID = clickTitlebarOfOnScreenLayerZeroWindow(processID: processID)
                Thread.sleep(forTimeInterval: 0.10)
            }
            return root
        }
        if titlebarFocusWindowID == nil {
            titlebarFocusWindowID = clickTitlebarOfOnScreenLayerZeroWindow(processID: processID)
        }
        Thread.sleep(forTimeInterval: 0.05)
    }
    if let titlebarFocusWindowID {
        throw SmokeAXError.assertion(
            "Rune CGWindow \(titlebarFocusWindowID) was clicked for exact PID \(processID), but no AXWindow materialized"
        )
    }
    throw SmokeAXError.elementMissing("Rune application window")
}

private func enableSkipClusterSetting(processID: pid_t) throws {
    let root = try activateApplication(processID: processID)
    try postKey(43, flags: .maskCommand, to: processID) // Command-,

    guard let keyBindings = waitForLabeledElement(in: root, label: "Key Bindings", timeout: 4) else {
        throw SmokeAXError.elementMissing("Key Bindings settings tab")
    }
    try performPress(keyBindings, operation: "open Key Bindings settings")

    let settingLabel = "Skip Cluster on Tab navigation from Sections"
    guard let toggle = waitForLabeledElement(
        in: root,
        label: settingLabel,
        role: kAXCheckBoxRole as String,
        timeout: 3
    ) else {
        throw SmokeAXError.elementMissing(settingLabel)
    }
    if !isSelected(toggle) {
        try performPress(toggle, operation: "enable skip-cluster setting")
    }
    guard isSelected(toggle) else {
        throw SmokeAXError.assertion("Skip-cluster setting did not become selected")
    }

    try postKey(13, flags: .maskCommand, to: processID) // Command-W
    Thread.sleep(forTimeInterval: 0.25)
    _ = try activateApplication(processID: processID)
    print("enable-skip-cluster-e2e passed")
}

private func assertSkipClusterNavigation(processID: pid_t) throws {
    let root = try activateApplication(processID: processID)
    guard waitForSelectedChoice(in: root, label: "Pods") != nil else {
        throw SmokeAXError.assertion("Pod kind was not selected before skip-cluster navigation")
    }

    try postKey(48, to: processID) // Tab: Sections -> content when cluster is skipped.
    try postKey(124, to: processID) // Right: Pods -> Deployments.
    guard waitForSelectedChoice(in: root, label: "Deployments", timeout: 4) != nil else {
        let choices = ["Pods", "Deployments", "Services", "Workloads", "Networking"].map { label in
            guard let element = findLabeledElement(in: root, label: label) else { return "\(label)=missing" }
            return "\(label)=\(stringAttribute(element, kAXRoleAttribute) ?? "unknown")/selected:\(isSelected(element))"
        }.joined(separator: ", ")
        let focus = attribute(root, kAXFocusedUIElementAttribute).flatMap { value -> String? in
            guard CFGetTypeID(value) == AXUIElementGetTypeID() else { return nil }
            let element = unsafeBitCast(value, to: AXUIElement.self)
            return "\(stringAttribute(element, kAXRoleAttribute) ?? "unknown")/\(accessibilityLabel(of: element))"
        } ?? "none"
        throw SmokeAXError.assertion("Tab from Sections did not skip Cluster and reach the middle panel; \(choices); focus=\(focus)")
    }

    try postKey(123, to: processID) // Restore Pods.
    guard waitForSelectedChoice(in: root, label: "Pods", timeout: 4) != nil else {
        throw SmokeAXError.assertion("Could not restore Pods after middle-panel navigation")
    }

    try postKey(48, flags: .maskShift, to: processID) // Content -> Sections.
    try postKey(125, to: processID) // Workloads -> Networking.
    guard waitForSelectedChoice(in: root, label: "Services", timeout: 4) != nil else {
        throw SmokeAXError.assertion("Shift-Tab from content did not return to Sections")
    }

    try postKey(126, to: processID) // Restore Workloads.
    guard waitForSelectedChoice(in: root, label: "Pods", timeout: 4) != nil else {
        throw SmokeAXError.assertion("Could not restore Workloads after pane-cycle verification")
    }
    print("skip-cluster-nav-e2e passed sequence=Sections>content>Sections")
}

private func assertCompare(processID: pid_t) throws {
    let root = try activateApplication(processID: processID)
    guard let selectAll = waitForLabeledElement(
        in: root,
        label: "Select Visible",
        role: kAXButtonRole as String
    ) else {
        throw SmokeAXError.elementMissing("Select Visible")
    }
    try performPress(selectAll, operation: "select visible generic resources")

    guard let actionsButton = waitForLabeledElement(in: root, label: "Actions", timeout: 3) else {
        throw SmokeAXError.assertion("Actions did not appear after Select Visible")
    }
    try performPress(actionsButton, operation: "open generic resource actions")
    guard let compareAction = waitForLabeledElement(
        in: root,
        label: "Compare Selected",
        role: kAXMenuItemRole as String,
        timeout: 3
    ) else {
        throw SmokeAXError.elementMissing("Compare Selected")
    }
    try performPress(compareAction, operation: "open resource comparison")

    guard waitForLabeledElement(in: root, label: "Compare selected resources", timeout: 3) != nil,
          let copyButton = waitForLabeledElement(
              in: root,
              label: "Copy Summary",
              role: kAXButtonRole as String,
              timeout: 3
          )
    else {
        throw SmokeAXError.elementMissing("resource comparison popover")
    }
    NSPasteboard.general.clearContents()
    try performPress(copyButton, operation: "copy resource comparison")

    let clipboardDeadline = Date().addingTimeInterval(2)
    var copiedText = ""
    repeat {
        copiedText = NSPasteboard.general.string(forType: .string) ?? ""
        if copiedText.contains("Selected ConfigMaps Compare") { break }
        Thread.sleep(forTimeInterval: 0.05)
    } while Date() < clipboardDeadline
    guard copiedText.contains("Selected ConfigMaps Compare"),
          copiedText.contains("Count:"),
          copiedText.contains("orbit-grid")
    else {
        throw SmokeAXError.assertion("Compare copied an incomplete summary: \(copiedText.prefix(160))")
    }

    guard let doneButton = findButton(in: root, label: "Done") else {
        throw SmokeAXError.elementMissing("Done comparison")
    }
    try performPress(doneButton, operation: "close resource comparison")
    if let deselectAll = waitForLabeledElement(
        in: root,
        label: "Deselect Visible",
        role: kAXButtonRole as String,
        timeout: 2
    ) {
        try performPress(deselectAll, operation: "restore generic resource selection")
    }
    print("compare-e2e passed copiedChars=\(copiedText.count)")
}

private func findEditableYAMLTextArea(in root: AXUIElement) -> AXUIElement? {
    firstElement(in: root) { element in
        if stringAttribute(element, kAXRoleAttribute) == (kAXTextAreaRole as String),
           stringAttribute(element, kAXValueAttribute)?.contains("apiVersion") == true {
            var settable = DarwinBoolean(false)
            if AXUIElementIsAttributeSettable(element, kAXValueAttribute as CFString, &settable) == .success,
               settable.boolValue {
                return true
            }
        }
        return false
    }
}

private func assertWriteDialog(processID: pid_t) throws {
    let root = try activateApplication(processID: processID)
    // Restrict this check to the disposable Docker fixture. Never accept the
    // final destructive action; only review its production gate and cancel it.
    func contextMenu() throws -> AXUIElement {
        guard let menu = waitForLabeledElement(in: root, label: "fake-orbit-mesh", role: kAXMenuButtonRole as String, timeout: 3) else {
            throw SmokeAXError.elementMissing("Synthetic fixture context menu")
        }
        return menu
    }
    func chooseContextAction(_ title: String) throws {
        try performPress(try contextMenu(), operation: "open synthetic context menu")
        guard let item = waitForLabeledElement(in: root, label: title, role: kAXMenuItemRole as String, timeout: 3) else {
            throw SmokeAXError.elementMissing(title)
        }
        try performPress(item, operation: title)
    }
    func selectInspectorTab(_ title: String) throws {
        guard let tab = waitForLabeledElement(in: root, label: title, role: kAXRadioButtonRole as String, timeout: 3) else {
            throw SmokeAXError.elementMissing("Selected fixture pod \(title) tab")
        }
        try performPress(tab, operation: "select fixture pod \(title)")
    }
    try selectInspectorTab("Overview")
    try chooseContextAction("Mark as Production")
    var needsRestore = true
    defer {
        if needsRestore {
            try? postKey(53, to: processID)
            try? chooseContextAction("Unmark Production")
            try? selectInspectorTab("Logs")
        }
    }
    guard waitForLabeledElement(in: root, label: "Production context active", timeout: 3) != nil else {
        throw SmokeAXError.assertion("Mark as Production did not activate the fixture's production state")
    }
    guard let delete = waitForLabeledElement(in: root, label: "Delete", role: kAXButtonRole as String, timeout: 3) else {
        throw SmokeAXError.elementMissing("Selected fixture pod Delete button")
    }
    try performPress(delete, operation: "open synthetic pod write review")
    guard let review = waitForLabeledElement(in: root, label: "Review Production Action", role: kAXButtonRole as String, timeout: 3) else {
        let actual = findElement(in: root, identifier: "rune.write-review.confirm").map(accessibilityLabel(of:)) ?? "missing"
        throw SmokeAXError.assertion("The synthetic write review must expose its first production confirmation; actual=\(actual)")
    }
    guard waitForLabeledElement(in: root, label: "Destructive production actions require a second confirmation", contains: true, timeout: 3) != nil else {
        throw SmokeAXError.assertion("The synthetic write review must explain its second production confirmation")
    }
    guard let target = waitForElement(in: root, identifier: "rune.write-review.target", timeout: 3),
          waitForLabeledElement(in: target, label: "fake-orbit-mesh", contains: true, timeout: 3) != nil else {
        throw SmokeAXError.assertion("The synthetic write review must show the exact fixture context in its target")
    }
    try performPress(review, operation: "review first production confirmation without executing the write")
    guard waitForLabeledElement(in: root, label: "Final confirmation required", contains: true, timeout: 3) != nil,
          findElement(in: root, identifier: "rune.write-review.confirm") != nil,
          let cancel = findButton(in: root, label: "Cancel") else {
        throw SmokeAXError.assertion("The write sheet did not update to its final confirmation stage")
    }
    try performPress(cancel, operation: "cancel synthetic production action")
    let deadline = Date().addingTimeInterval(3)
    while findElement(in: root, identifier: "rune.write-review.confirm") != nil && Date() < deadline {
        Thread.sleep(forTimeInterval: 0.05)
    }
    guard findElement(in: root, identifier: "rune.write-review.confirm") == nil else {
        throw SmokeAXError.assertion("Cancel did not dismiss the write review")
    }
    try chooseContextAction("Unmark Production")
    try selectInspectorTab("Logs")
    needsRestore = false
    print("write-dialog-e2e passed production-review=updated final-action=cancelled")
}

private func assertYAMLDialog(processID: pid_t) throws {
    let root = try activateApplication(processID: processID)
    guard let editButton = waitForElement(
        in: root,
        identifier: "resource-describe-edit-yaml",
        timeout: 3
    ) else {
        throw SmokeAXError.elementMissing("resource-describe-edit-yaml")
    }
    try performPress(editButton, operation: "open YAML editor from Describe")

    guard waitForLabeledElement(in: root, label: "YAML manifest", timeout: 3) != nil,
          findEditableYAMLTextArea(in: root) != nil,
          waitForLabeledElement(
              in: root,
              label: "Apply YAML can write only after confirmation.",
              contains: true,
              timeout: 3
          ) != nil
    else {
        throw SmokeAXError.assertion("YAML editor sheet did not expose its header, editable manifest, and safety explanation")
    }
    guard let applyButton = waitForLabeledElement(
        in: root,
        label: "Apply YAML",
        role: kAXButtonRole as String
    ), !boolAttribute(applyButton, kAXEnabledAttribute) else {
        throw SmokeAXError.assertion("Apply YAML should be present and disabled before an edit")
    }
    guard let closeButton = findButton(in: root, label: "Close") else {
        throw SmokeAXError.elementMissing("Close YAML editor")
    }
    try performPress(closeButton, operation: "close YAML editor")
    print("yaml-dialog-e2e passed")
}

private func importedFixtureSnapshot() throws -> [String: Data] {
    guard let path = ProcessInfo.processInfo.environment["RUNE_UI_SMOKE_IMPORT_ROOT"],
          path.hasPrefix("/tmp/") || path.hasPrefix("/private/tmp/"),
          let enumerator = FileManager.default.enumerator(atPath: path) else {
        throw SmokeAXError.assertion("Reimport E2E requires the isolated temporary import folder")
    }
    var snapshot: [String: Data] = [:]
    while let relativePath = enumerator.nextObject() as? String {
        guard snapshot.count < 32 else { throw SmokeAXError.assertion("Unexpected fixture import size") }
        let url = URL(fileURLWithPath: path).appendingPathComponent(relativePath)
        let values = try url.resourceValues(forKeys: [.isRegularFileKey, .isSymbolicLinkKey, .fileSizeKey])
        guard values.isSymbolicLink != true else { throw SmokeAXError.assertion("Unexpected fixture symlink") }
        if values.isRegularFile == true {
            guard (values.fileSize ?? Int.max) < 1_048_576 else { throw SmokeAXError.assertion("Unexpected fixture file size") }
            snapshot[relativePath] = try Data(contentsOf: url)
        }
    }
    guard snapshot.keys.contains(where: { $0.hasSuffix("/.rune-import.json") }) else {
        throw SmokeAXError.assertion("The first import has no ownership record")
    }
    return snapshot
}

private func assertKubeconfigImport(processID: pid_t, reimport: Bool = false) throws {
    guard let path = ProcessInfo.processInfo.environment["RUNE_UI_SMOKE_IMPORT_KUBECONFIG"],
          !path.isEmpty
    else {
        throw SmokeAXError.assertion("RUNE_UI_SMOKE_IMPORT_KUBECONFIG is required for import-kubeconfig")
    }
    let raw = try String(contentsOfFile: path, encoding: .utf8)
    let requiredMarkers = [
        "name: fake-orbit-mesh",
        "name: fake-lattice-spark",
        "server: https://127.0.0.1:16443",
        "server: https://127.0.0.1:17443",
    ]
    guard requiredMarkers.allSatisfy(raw.contains) else {
        throw SmokeAXError.assertion("Import E2E refused a kubeconfig that was not the two local fake clusters")
    }
    let originalSnapshot = reimport ? try importedFixtureSnapshot() : nil

    let pasteboard = NSPasteboard.general
    pasteboard.clearContents()
    guard pasteboard.setString(raw, forType: .string) else {
        throw SmokeAXError.assertion("Could not stage the local fake kubeconfig on the pasteboard")
    }
    defer { pasteboard.clearContents() }

    let root: AXUIElement
    do {
        root = try activateApplication(processID: processID)
    } catch let SmokeAXError.elementMissing(identifier) where identifier == "Rune application window" {
        guard !reimport else { throw SmokeAXError.elementMissing(identifier) }
        try assertKubeconfigImportWithNativeMenu(processID: processID)
        return
    } catch {
        throw error
    }
    let importRoute: String
    if let addClusterButton = waitForElement(
        in: root,
        identifier: "rune.add-cluster.button",
        timeout: 8
    ) {
        try performPress(addClusterButton, operation: "open Add Cluster popover")
        guard let pasteAction = waitForLabeledElement(
            in: root,
            label: "Paste Kubeconfig",
            role: kAXButtonRole as String,
            contains: true,
            timeout: 3
        ) else {
            throw SmokeAXError.elementMissing("Add Cluster Paste Kubeconfig action")
        }
        try performPress(pasteAction, operation: "paste local fake kubeconfig through Add Cluster")
        importRoute = "add-cluster-popover"
    } else {
        let methods = waitForElement(
            in: root,
            identifier: "rune.onboarding.connection-methods",
            timeout: 3
        ) ?? waitForLabeledElement(in: root, label: "Other Connection Methods", timeout: 3)
        guard let methods else {
            throw SmokeAXError.elementMissing("rune.add-cluster.button or rune.onboarding.connection-methods")
        }
        try performPress(methods, operation: "open onboarding connection methods")

        let pasteAction = waitForElement(
            in: root,
            identifier: "rune.onboarding.paste-kubeconfig",
            timeout: 3
        ) ?? waitForLabeledElement(in: root, label: "Paste Kubeconfig", timeout: 3)
        guard let pasteAction else {
            throw SmokeAXError.elementMissing("Paste Kubeconfig")
        }
        try performPress(pasteAction, operation: "paste local fake kubeconfig through onboarding")
        importRoute = "onboarding"
    }

    guard waitForElement(
        in: root,
        identifier: "rune.kubeconfig-import.review",
        timeout: 8
    ) != nil else {
        throw SmokeAXError.elementMissing("rune.kubeconfig-import.review")
    }
    if reimport {
        guard let update = waitForLabeledElement(in: root, label: "Update existing", role: kAXRadioButtonRole as String, timeout: 5) else {
            throw SmokeAXError.elementMissing("Update existing duplicate policy")
        }
        try performPress(update, operation: "choose Update existing for unchanged owned configuration")
        let deadline = Date().addingTimeInterval(5)
        while Date() < deadline {
            if let status = findElement(in: root, identifier: "rune.kubeconfig-import.status"),
               accessibilityLabel(of: status) == "2 contexts ready to confirm" { break }
            Thread.sleep(forTimeInterval: 0.05)
        }
    }
    guard findElement(in: root, identifier: "rune.kubeconfig-import.context.fake-orbit-mesh") != nil,
          findElement(in: root, identifier: "rune.kubeconfig-import.context.fake-lattice-spark") != nil,
          let status = findElement(in: root, identifier: "rune.kubeconfig-import.status"),
          accessibilityLabel(of: status) == "2 contexts ready to confirm"
    else {
        throw SmokeAXError.assertion("Import review did not expose both local contexts and its confirmation status")
    }
    guard let confirm = findElement(in: root, identifier: "rune.kubeconfig-import.confirm"),
          boolAttribute(confirm, kAXEnabledAttribute)
    else {
        throw SmokeAXError.assertion("Kubeconfig Import action was missing or disabled")
    }
    try performPress(confirm, operation: "confirm local fake kubeconfig import")

    let activationDeadline = Date().addingTimeInterval(35)
    var activated = false
    repeat {
        let reviewDismissed = findElement(in: root, identifier: "rune.kubeconfig-import.review") == nil
        let orbitContext = findElement(in: root, identifier: "rune.context.fake-orbit-mesh")
        let latticeContext = findElement(in: root, identifier: "rune.context.fake-lattice-spark")
        if reviewDismissed, let orbitContext, latticeContext != nil, isSelected(orbitContext) {
            activated = true
            break
        }
        Thread.sleep(forTimeInterval: 0.10)
    } while Date() < activationDeadline
    guard activated else {
        throw SmokeAXError.assertion("Imported local contexts did not become available after confirmation")
    }

    // The source watcher runs every two seconds. Verify the app-owned bookmark is
    // still discoverable after that boundary instead of accepting a transient UI.
    Thread.sleep(forTimeInterval: 2.4)
    guard let persistedOrbit = findElement(in: root, identifier: "rune.context.fake-orbit-mesh"),
          findElement(in: root, identifier: "rune.context.fake-lattice-spark") != nil,
          isSelected(persistedOrbit)
    else {
        throw SmokeAXError.assertion("Imported contexts disappeared during kubeconfig source synchronization")
    }

    if let originalSnapshot {
        guard try importedFixtureSnapshot() == originalSnapshot else {
            throw SmokeAXError.assertion("Reimport created another copy or changed the original ownership revision")
        }
        guard waitForLabeledElement(in: root, label: "Connections reused", timeout: 8) != nil else {
            throw SmokeAXError.elementMissing("Connections reused notice")
        }
        print("reimport-kubeconfig-e2e passed contexts=2 copies=1 ownership=unchanged")
    } else {
        print("import-kubeconfig-e2e passed route=\(importRoute) contexts=2 source=app-owned")
    }
}

private func assertLogExport(processID: pid_t) throws {
    let root = try activateApplication(processID: processID)
    guard let path = ProcessInfo.processInfo.environment["RUNE_UI_SMOKE_EXPORT_DIRECTORY"] else {
        throw SmokeAXError.assertion("RUNE_UI_SMOKE_EXPORT_DIRECTORY must name the isolated test export folder")
    }
    let directory = URL(fileURLWithPath: path, isDirectory: true).resolvingSymlinksInPath()
    guard directory.path.hasPrefix("/private/tmp/") || directory.path.hasPrefix("/tmp/") else {
        throw SmokeAXError.assertion("Log export smoke requires a temporary export folder")
    }
    func files() throws -> Set<URL> {
        Set(try FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil))
    }
    func control(_ id: String) throws -> AXUIElement {
        guard let element = waitForElement(in: root, identifier: id, timeout: 5),
              stringAttribute(element, kAXRoleAttribute) == (kAXButtonRole as String),
              boolAttribute(element, kAXEnabledAttribute) else {
            throw SmokeAXError.assertion("Expected an enabled accessible button: \(id)")
        }
        return element
    }
    let marker = ProcessInfo.processInfo.environment["RUNE_UI_SMOKE_LOG_MARKER"] ?? "tick"
    let initialPlayback = try control("log-tail-playback")
    if accessibilityLabel(of: initialPlayback) != "Tail" {
        _ = AXUIElementPerformAction(initialPlayback, kAXShowMenuAction as CFString)
        guard let stop = waitForLabeledElement(in: root, label: "Stop Tail", role: kAXMenuItemRole as String) else {
            throw SmokeAXError.assertion("Could not reset playback through its Stop Tail menu")
        }
        try performPress(stop, operation: "stop tail before playback regression")
        guard waitForLabeledElement(in: root, label: "Tail", role: kAXButtonRole as String) != nil else {
            throw SmokeAXError.assertion("Stop Tail did not restore the Tail action")
        }
    }
    for (before, after) in [("Tail", "Pause"), ("Pause", "Resume"), ("Resume", "Pause")] {
        let playback = try control("log-tail-playback")
        guard accessibilityLabel(of: playback) == before else {
            throw SmokeAXError.assertion("Expected playback action \(before), got \(accessibilityLabel(of: playback))")
        }
        try performPress(playback, operation: "activate \(before)")
        guard waitForLabeledElement(in: root, label: after, role: kAXButtonRole as String, timeout: 5) != nil else {
            throw SmokeAXError.assertion("Playback did not expose \(after)")
        }
    }
    // Pause before exporting so the saved-file assertion has a stable source.
    try performPress(try control("log-tail-playback"), operation: "pause log stream")
    guard waitForLabeledElement(in: root, label: "Resume", role: kAXButtonRole as String) != nil else {
        throw SmokeAXError.assertion("Log stream did not pause")
    }
    let quickSave = try control("log-quick-save")
    guard let save = findButton(in: root, label: "Save Logs"),
          let quickFrame = frame(of: quickSave), let saveFrame = frame(of: save),
          abs(quickFrame.origin.y - saveFrame.origin.y) < 1,
          abs(quickFrame.size.height - saveFrame.size.height) < 1,
          quickFrame.origin.x >= saveFrame.origin.x + saveFrame.size.width - 1,
          quickFrame.size.width < saveFrame.size.width,
          applicationWindows(root).contains(where: { window in
              guard let windowFrame = frame(of: window) else { return false }
              return CGRect(origin: windowFrame.origin, size: windowFrame.size)
                  .contains(CGRect(origin: quickFrame.origin, size: quickFrame.size))
          }) else {
        throw SmokeAXError.assertion("Quick Save must remain visible beside Save Logs with the same control height")
    }
    let original = try files()
    try performPress(quickSave, operation: "quick save logs")
    guard let folderAction = waitForElement(in: root, identifier: "saved-log-open-folder", timeout: 10),
          let fileAction = findElement(in: root, identifier: "saved-log-open-file") else {
        throw SmokeAXError.assertion("Quick Save did not show both saved-file toast actions")
    }
    let created = try files().subtracting(original)
    guard created.count == 1, let saved = created.first,
          let contents = try? String(contentsOf: saved, encoding: .utf8),
          contents.contains(marker) else {
        throw SmokeAXError.assertion("One click must create exactly one nonempty synthetic log containing the fixture marker")
    }
    func waitForOpenedURL(_ url: URL, bundleIdentifier: String) -> Bool {
        let deadline = Date().addingTimeInterval(15)
        repeat {
            for app in NSRunningApplication.runningApplications(withBundleIdentifier: bundleIdentifier) {
                let appRoot = AXUIElementCreateApplication(app.processIdentifier)
                AXUIElementSetMessagingTimeout(appRoot, 0.5)
                for window in applicationWindows(appRoot) {
                    if stringAttribute(window, kAXTitleAttribute)?.contains(url.lastPathComponent) == true { return true }
                    if firstElement(in: window, limit: 100, matching: { element in
                        if let document = stringAttribute(element, kAXDocumentAttribute),
                           let actual = URL(string: document),
                           actual.resolvingSymlinksInPath().path == url.resolvingSymlinksInPath().path { return true }
                        return stringAttribute(element, kAXTitleAttribute) == url.lastPathComponent
                    }) != nil { return true }
                }
            }
            Thread.sleep(forTimeInterval: 0.10)
        } while Date() < deadline
        return false
    }
    try performPress(folderAction, operation: "open saved folder")
    guard waitForOpenedURL(directory, bundleIdentifier: "com.apple.finder") else {
        throw SmokeAXError.assertion("Open Folder did not reveal the saved export folder in Finder")
    }
    _ = try activateApplication(processID: processID)
    guard let openerURL = NSWorkspace.shared.urlForApplication(toOpen: saved),
          let openerID = Bundle(url: openerURL)?.bundleIdentifier else {
        throw SmokeAXError.assertion("No system default opener for the exported log")
    }
    try performPress(fileAction, operation: "open saved log file")
    guard waitForOpenedURL(saved, bundleIdentifier: openerID) else {
        throw SmokeAXError.assertion("Open File did not expose the saved log in its default application")
    }
    guard try files() == original.union(created) else {
        throw SmokeAXError.assertion("Opening the saved file/folder must not export another file")
    }
    _ = try activateApplication(processID: processID)
    print("log-export-e2e passed playback=start/pause/resume quick-save=1 toast=folder/file duplicate-exports=0 geometry=visible/equal-height")
}

private func assertSettingsAndSort(processID: pid_t) throws {
    let root = try activateApplication(processID: processID)
    func pressChoice(_ label: String, role: String) throws {
        guard let choice = waitForLabeledElement(in: root, label: label, role: role) else {
            throw SmokeAXError.elementMissing(label)
        }
        try performPress(choice, operation: "select \(label)")
        if role == (kAXMenuItemRole as String) {
            let deadline = Date().addingTimeInterval(3)
            while findLabeledElement(in: root, label: label, role: role) != nil, Date() < deadline {
                Thread.sleep(forTimeInterval: 0.05)
            }
            guard findLabeledElement(in: root, label: label, role: role) == nil else {
                throw SmokeAXError.assertion("Menu did not close after selecting \(label)")
            }
        }
    }
    func sortMenu() throws -> AXUIElement {
        guard let menu = waitForElement(in: root, identifier: "resource-sort-menu") else {
            throw SmokeAXError.elementMissing("resource-sort-menu")
        }
        return menu
    }
    func expectSort(_ value: String) throws {
        let deadline = Date().addingTimeInterval(3)
        repeat {
            if let menu = findElement(in: root, identifier: "resource-sort-menu"),
               stringAttribute(menu, kAXValueAttribute) == value { return }
            Thread.sleep(forTimeInterval: 0.05)
        } while Date() < deadline
        throw SmokeAXError.assertion("Resource sort did not become \(value)")
    }
    try performPress(try sortMenu(), operation: "open sort menu")
    try pressChoice("Age", role: kAXMenuItemRole as String)
    guard waitForLabeledElement(in: root, label: "Age, ", role: kAXMenuButtonRole as String, contains: true) != nil else {
        throw SmokeAXError.assertion("Age sort did not become active")
    }
    try performPress(try sortMenu(), operation: "choose initial date order")
    try pressChoice("Newest First", role: kAXMenuItemRole as String)
    try expectSort("Age, Newest First")
    try performPress(try sortMenu(), operation: "open sort order")
    try pressChoice("Oldest First", role: kAXMenuItemRole as String)
    try expectSort("Age, Oldest First")
    guard let ageHeader = findButton(in: root, label: "Age"),
          stringAttribute(ageHeader, kAXValueAttribute) == "Sorted descending" else {
        throw SmokeAXError.assertion("Age table header and sort menu disagree")
    }
    try performPress(try sortMenu(), operation: "reopen sort column")
    try pressChoice("Age", role: kAXMenuItemRole as String)
    try expectSort("Age, Oldest First")
    guard let settings = findElement(in: root, identifier: "rune.settings.open") else {
        throw SmokeAXError.elementMissing("rune.settings.open")
    }
    try performPress(settings, operation: "open Settings")
    for pane in ["General", "Key Bindings", "Logs", "Safety", "Diagnostics", "Performance", "Themes"] {
        try pressChoice(pane, role: kAXButtonRole as String)
        guard waitForLabeledElement(in: root, label: pane, role: kAXWindowRole as String) != nil else {
            throw SmokeAXError.assertion("Settings pane did not open: \(pane)")
        }
    }
    for title in ["Aurora", "Graphite Blue", "Ember Glass", "Moss Terminal", "Fjord", "Paper", "Daylight", "Contrast Dark", "Contrast Light", "Native"] {
        if let card = findButton(in: root, label: title) {
            try performPress(card, operation: "select theme \(title)")
        } else {
            try pressChoice("More Themes", role: kAXMenuButtonRole as String)
            try pressChoice(title, role: kAXMenuItemRole as String)
        }
        guard waitForLabeledElement(in: root, label: "Current: \(title)", role: kAXStaticTextRole as String) != nil else {
            throw SmokeAXError.assertion("Theme did not apply: \(title)")
        }
        guard let card = findButton(in: root, label: title), isSelected(card) else {
            throw SmokeAXError.assertion("Theme selection is not accessible: \(title)")
        }
    }
    print("settings-sort-e2e passed settings-panes=7 themes=10 age=newest/oldest header=synchronized reselect=preserves-order")
}

do {
    guard CommandLine.arguments.count == 3,
          let processID = pid_t(CommandLine.arguments[1])
    else {
        throw SmokeAXError.invalidArguments
    }
    switch CommandLine.arguments[2] {
    case "focus":
        _ = try activateApplication(processID: processID)
        print("focus passed")
    case "log-search":
        try assertLogSearch(processID: processID)
    case "write-dialog":
        try assertWriteDialog(processID: processID)
    case "log-export":
        try assertLogExport(processID: processID)
    case "settings-sort":
        try assertSettingsAndSort(processID: processID)
    case "open-command-palette":
        try openCommandPalette(processID: processID)
    case "enable-skip-cluster":
        try enableSkipClusterSetting(processID: processID)
    case "skip-cluster-nav":
        try assertSkipClusterNavigation(processID: processID)
    case "compare":
        try assertCompare(processID: processID)
    case "yaml-dialog":
        try assertYAMLDialog(processID: processID)
    case "import-kubeconfig":
        try assertKubeconfigImport(processID: processID)
    case "reimport-kubeconfig":
        try assertKubeconfigImport(processID: processID, reimport: true)
    default:
        throw SmokeAXError.invalidArguments
    }
} catch {
    FileHandle.standardError.write(Data("\(error)\n".utf8))
    exit(1)
}
