#!/usr/bin/env swift
import CoreGraphics
import Foundation

let owner = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "RuneApp"
let ownerPID = CommandLine.arguments.count > 2 ? Int(CommandLine.arguments[2]) : nil

guard let windows = CGWindowListCopyWindowInfo([.optionOnScreenOnly], kCGNullWindowID) as? [[String: Any]] else {
    fputs("CGWindowListCopyWindowInfo failed\n", stderr)
    exit(1)
}

for window in windows {
    if let ownerPID {
        guard (window[kCGWindowOwnerPID as String] as? Int) == ownerPID else { continue }
    } else {
        guard (window[kCGWindowOwnerName as String] as? String) == owner else { continue }
    }
    guard (window[kCGWindowLayer as String] as? Int) == 0 else { continue }
    guard let windowID = window[kCGWindowNumber as String] as? Int else { continue }
    print(windowID)
    exit(0)
}

if let ownerPID {
    fputs("No on-screen window for pid \(ownerPID) (\(owner))\n", stderr)
} else {
    fputs("No on-screen window for process \(owner)\n", stderr)
}
exit(1)
