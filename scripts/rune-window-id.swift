#!/usr/bin/env swift
import CoreGraphics
import Foundation

let owner = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "RuneApp"
let ownerPID = CommandLine.arguments.count > 2 ? Int(CommandLine.arguments[2]) : nil
let mode = CommandLine.arguments.count > 3 ? CommandLine.arguments[3] : "--id"

guard let windows = CGWindowListCopyWindowInfo([.optionOnScreenOnly], kCGNullWindowID) as? [[String: Any]] else {
    fputs("CGWindowListCopyWindowInfo failed\n", stderr)
    exit(1)
}

let matchingWindows = windows.filter { window in
    if let ownerPID {
        guard (window[kCGWindowOwnerPID as String] as? Int) == ownerPID else { return false }
    } else {
        guard (window[kCGWindowOwnerName as String] as? String) == owner else { return false }
    }
    return (window[kCGWindowLayer as String] as? Int) == 0
}

if mode == "--bounds-largest" {
    let largest = matchingWindows.compactMap { window -> (CGRect, CGFloat)? in
        guard let dictionary = window[kCGWindowBounds as String] as? [String: Any],
              let bounds = CGRect(dictionaryRepresentation: dictionary as CFDictionary)
        else {
            return nil
        }
        return (bounds, bounds.width * bounds.height)
    }
    .max { $0.1 < $1.1 }

    if let bounds = largest?.0 {
        print(
            "\(Int(bounds.origin.x)),\(Int(bounds.origin.y))," +
            "\(Int(bounds.width)),\(Int(bounds.height))"
        )
        exit(0)
    }
} else if let window = matchingWindows.first,
          let windowID = window[kCGWindowNumber as String] as? Int {
    print(windowID)
    exit(0)
}

if let ownerPID {
    fputs("No on-screen window for pid \(ownerPID) (\(owner))\n", stderr)
} else {
    fputs("No on-screen window for process \(owner)\n", stderr)
}
exit(1)
