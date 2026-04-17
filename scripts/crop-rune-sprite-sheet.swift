#!/usr/bin/env swift
import AppKit
import Foundation

/// Crops regions from the 1536×1024 Rune sprite sheet (NSImage top-left coords).
func cgRectFromNS(_ ns: CGRect, imageHeight: CGFloat) -> CGRect {
    CGRect(
        x: ns.origin.x,
        y: imageHeight - ns.origin.y - ns.size.height,
        width: ns.size.width,
        height: ns.size.height
    )
}

func savePNG(cgImage: CGImage, to url: URL) {
    let rep = NSBitmapImageRep(cgImage: cgImage)
    guard let data = rep.representation(using: .png, properties: [:]) else {
        fputs("Failed to encode PNG\n", stderr)
        exit(1)
    }
    try! data.write(to: url)
}

let args = CommandLine.arguments
guard args.count >= 3 else {
    fputs(
        "Usage: \(args[0]) <input.png> <output-dir>\n",
        stderr
    )
    exit(2)
}
let inputPath = args[1]
let outDir = URL(fileURLWithPath: args[2], isDirectory: true)
try FileManager.default.createDirectory(at: outDir, withIntermediateDirectories: true)

guard let nsImage = NSImage(contentsOfFile: inputPath),
      let cgFull = nsImage.cgImage(forProposedRect: nil, context: nil, hints: nil)
else {
    fputs("Could not load image: \(inputPath)\n", stderr)
    exit(1)
}

let iw = CGFloat(cgFull.width)
let ih = CGFloat(cgFull.height)

// Top row: large logo | app icon
let topH = ih / 2
var crops: [(String, CGRect)] = [
    ("logo_hex_wordmark", CGRect(x: 0, y: 0, width: iw / 2, height: topH)),
    ("logo_app_icon_tile", CGRect(x: iw / 2, y: 0, width: iw / 2, height: topH)),
]

// Bottom row: wheel | small hex | cubes | segmented hex | terminal
let bottomY = topH
let bottomH = ih - topH
let colW = floor(iw / 5)
var x: CGFloat = 0
let bottomNames = ["icon_wheel", "icon_hex_small", "icon_cubes", "icon_hex_segmented", "icon_terminal"]
for (i, name) in bottomNames.enumerated() {
    let w = (i == 4) ? (iw - x) : colW
    let r = CGRect(x: x, y: bottomY, width: w, height: bottomH)
    x += w
    crops.append((name, r))
}

for (baseName, nsRect) in crops {
    let cgRect = cgRectFromNS(nsRect, imageHeight: ih)
    guard let cropped = cgFull.cropping(to: cgRect) else {
        fputs("Crop failed for \(baseName)\n", stderr)
        exit(1)
    }
    let out = outDir.appendingPathComponent("\(baseName).png")
    savePNG(cgImage: cropped, to: out)
    print(out.path)
}
