import AppKit
import SwiftUI

/// Keep the native controls alive while queries alternate between pending and current results.
/// The view always receives the latest action closure, including after a resource change.
struct ResourceLogSearchAccessories: NSViewRepresentable {
    enum Action: Int { case clear, matchCase, previous, next, jump }

    static let regularWidth: CGFloat = 4 * RuneUILayoutMetrics.iconButtonSize + ResourceLogsLayoutMetrics.searchMatchStatusWidth + 12
    static let compactWidth: CGFloat = RuneUILayoutMetrics.iconButtonSize + ResourceLogsLayoutMetrics.compactSearchMatchStatusWidth

    let compact: Bool
    let hasQuery: Bool
    let matchCase: Bool
    let hasMatches: Bool
    let status: String
    let statusLabel: String
    let statusColor: Color
    let matchCaseHelp: String
    let action: (Action) -> Void
    @Environment(\.runeThemePalette) private var palette

    func makeNSView(context: Context) -> Controls {
        Controls(compact: compact)
    }

    func updateNSView(_ view: Controls, context: Context) {
        view.update(from: self)
    }

    static func dismantleNSView(_ view: Controls, coordinator: ()) {
        view.invalidate()
    }

    final class Controls: NSView {
        let compact: Bool
        let statusButton = NSButton(title: "", target: nil, action: nil)
        private(set) var buttons: [Action: NSButton] = [:]
        private(set) var menuButton: NSPopUpButton?
        private var performAction: ((Action) -> Void)?
        private var statusInk: NSColor?

        init(compact: Bool) {
            self.compact = compact
            super.init(frame: .zero)
            setAccessibilityElement(false)
            let unit = RuneUILayoutMetrics.iconButtonSize
            let statusWidth = ResourceLogsLayoutMetrics.searchMatchStatusWidth
            let compactStatusWidth = ResourceLogsLayoutMetrics.compactSearchMatchStatusWidth
            if compact {
                configure(statusButton, action: .jump, label: "Search match position", symbol: nil)
                statusButton.frame = NSRect(x: 0, y: 0, width: compactStatusWidth, height: unit)
                let menu = NSPopUpButton(frame: NSRect(x: compactStatusWidth, y: 0, width: unit, height: unit), pullsDown: true)
                menu.isBordered = false
                menu.imagePosition = .imageOnly
                (menu.cell as? NSPopUpButtonCell)?.arrowPosition = .noArrow
                menu.menu?.autoenablesItems = false
                menu.addItem(withTitle: "Search options")
                menu.item(at: 0)?.image = NSImage(systemSymbolName: "ellipsis.circle", accessibilityDescription: nil)
                for (action, title) in [(Action.clear, "Clear search"), (.matchCase, "Match case"), (.previous, "Previous match"), (.next, "Next match")] {
                    let item = NSMenuItem(title: title, action: #selector(menuAction(_:)), keyEquivalent: "")
                    item.tag = action.rawValue
                    item.target = self
                    menu.menu?.addItem(item)
                }
                menu.setAccessibilityLabel("Log search options")
                menu.setAccessibilityIdentifier("resource-log-search-compact-controls")
                menu.toolTip = "Search options"
                menuButton = menu
                addSubview(menu)
            } else {
                for (action, label, symbol, x) in [
                    (Action.clear, "Clear log search", "xmark.circle.fill", CGFloat(0)),
                    (.matchCase, "Match case", "textformat", unit + 2),
                    (.previous, "Previous match", "chevron.up", 2 * unit + statusWidth + 12),
                    (.next, "Next match", "chevron.down", 3 * unit + statusWidth + 12)
                ] {
                    let button = NSButton(title: "", target: nil, action: nil)
                    configure(button, action: action, label: label, symbol: symbol)
                    button.frame = NSRect(x: x, y: 0, width: unit, height: unit)
                }
                let separator = NSBox(frame: NSRect(x: 2 * unit + 6, y: (unit - 16) / 2, width: 1, height: 16))
                separator.boxType = .separator
                addSubview(separator)
                configure(statusButton, action: .jump, label: "Search match position", symbol: nil)
                statusButton.frame = NSRect(x: 2 * unit + 12, y: 0, width: statusWidth, height: unit)
            }
            statusButton.font = .monospacedDigitSystemFont(ofSize: 11, weight: .medium)
            statusButton.alignment = .right
            (statusButton.cell as? NSButtonCell)?.lineBreakMode = .byTruncatingMiddle
            statusButton.setAccessibilityIdentifier("resource-log-search-match-status")
            statusButton.toolTip = "Go to match number"
        }

        required init?(coder: NSCoder) { nil }

        func invalidate() { performAction = nil }

        override var intrinsicContentSize: NSSize {
            NSSize(width: compact ? ResourceLogSearchAccessories.compactWidth : ResourceLogSearchAccessories.regularWidth, height: RuneUILayoutMetrics.iconButtonSize)
        }

        private func configure(_ button: NSButton, action: Action, label: String, symbol: String?) {
            button.tag = action.rawValue
            button.target = self
            button.action = #selector(buttonAction(_:))
            button.isBordered = false
            button.setButtonType(.momentaryChange)
            button.font = .systemFont(ofSize: 11, weight: .semibold)
            if let symbol {
                button.image = NSImage(systemSymbolName: symbol, accessibilityDescription: nil)
                button.imagePosition = .imageOnly
                button.symbolConfiguration = .init(pointSize: 11, weight: .semibold)
            }
            button.setAccessibilityLabel(label)
            button.toolTip = label
            buttons[action] = button
            addSubview(button)
        }

        func update(from value: ResourceLogSearchAccessories) {
            performAction = value.action
            let secondary = NSColor(value.palette?.secondaryText ?? Color(nsColor: RuneThemeContrast.nativeInk(.secondaryLabelColor)))
            let muted = NSColor(value.palette?.mutedText ?? Color(nsColor: RuneThemeContrast.nativeInk(.secondaryLabelColor)))
            let accent = NSColor(value.palette?.accent ?? Color(nsColor: RuneThemeContrast.nativeInk(.controlAccentColor)))
            for (action, button) in buttons {
                let enabled = action == .clear ? value.hasQuery : action == .matchCase || value.hasMatches
                if button.isEnabled != enabled { button.isEnabled = enabled }
                let tint = action == .jump ? NSColor(value.statusColor)
                    : action == .matchCase && value.matchCase ? accent : enabled ? secondary : muted
                if button.contentTintColor != tint { button.contentTintColor = tint }
            }
            if let clear = buttons[.clear], clear.isHidden == value.hasQuery { clear.isHidden = !value.hasQuery }
            if let match = buttons[.matchCase] {
                match.toolTip = value.matchCaseHelp
                match.setAccessibilityValue(value.matchCase ? "On" : "Off")
                match.wantsLayer = true
                match.layer?.cornerRadius = RuneUILayoutMetrics.compactGlyphCornerRadius
                match.layer?.backgroundColor = value.matchCase ? accent.withAlphaComponent(0.16).cgColor : nil
            }
            let statusInk = NSColor(value.statusColor)
            if statusButton.title != value.status || self.statusInk != statusInk {
                self.statusInk = statusInk
                statusButton.attributedTitle = NSAttributedString(string: value.status, attributes: [
                    .foregroundColor: statusInk,
                    .font: NSFont.monospacedDigitSystemFont(ofSize: 11, weight: .medium)
                ])
            }
            if statusButton.accessibilityLabel() != value.statusLabel { statusButton.setAccessibilityLabel(value.statusLabel) }
            if let menuButton {
                if menuButton.contentTintColor != secondary { menuButton.contentTintColor = secondary }
                for item in menuButton.itemArray.dropFirst() {
                    guard let action = Action(rawValue: item.tag) else { continue }
                    item.isEnabled = action == .clear ? value.hasQuery : action == .matchCase || value.hasMatches
                    item.state = action == .matchCase && value.matchCase ? .on : .off
                }
            }
        }

        @objc private func buttonAction(_ sender: NSButton) {
            guard sender.isEnabled, let action = Action(rawValue: sender.tag) else { return }
            performAction?(action)
        }

        @objc private func menuAction(_ sender: NSMenuItem) {
            guard sender.isEnabled, let action = Action(rawValue: sender.tag) else { return }
            performAction?(action)
        }
    }
}
