#!/usr/bin/env swift

import AppKit
import CoreGraphics
import Foundation
import ImageIO
import UniformTypeIdentifiers

enum IconAssetError: Error {
    case imageLoadFailed(String)
    case alphaBoundsMissing(String)
    case renderFailed(String)
}

struct IconAssetGenerator {
    let root = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
    let logoPath = "assets/rune_logo_redone/rune_logo_no_edge_halo_hard_trim.png"
    let iconsetPath = "assets/rune_app_iconset_wheel.iconset"
    let icnsPath = "assets/rune_wheel.icns"

    private let logoCanvasSize = 1024
    private let iconCanvasSize = 1024
    private let logoPadding: CGFloat = 0
    private let iconPlateInset: CGFloat = 0
    /// Scale applied to the logo before clipping it to the icon plate.
    private let iconLogoScale: CGFloat = 0.94
    private let iconLogoFitCornerFraction: CGFloat = 0.017

    func run() throws {
        let logoURL = root.appendingPathComponent(logoPath)
        let iconsetURL = root.appendingPathComponent(iconsetPath)
        let icnsURL = root.appendingPathComponent(icnsPath)

        let originalLogo = try loadImage(at: logoURL)
        let trimmedLogoBounds = try alphaBounds(of: originalLogo, name: logoPath)
        let tightenedLogo = try renderTightenedLogo(from: originalLogo, croppedTo: trimmedLogoBounds)

        let masterIcon = try renderMasterIcon(from: tightenedLogo)
        try writeIconset(masterIcon, to: iconsetURL)
        try rebuildICNS(from: iconsetURL, to: icnsURL)
    }

    private func loadImage(at url: URL) throws -> CGImage {
        guard let source = CGImageSourceCreateWithURL(url as CFURL, nil),
              let image = CGImageSourceCreateImageAtIndex(source, 0, nil) else {
            throw IconAssetError.imageLoadFailed(url.path)
        }
        return image
    }

    private func alphaBounds(of image: CGImage, name: String) throws -> CGRect {
        let width = image.width
        let height = image.height
        let bytesPerPixel = 4
        let bytesPerRow = width * bytesPerPixel
        let colorSpace = CGColorSpaceCreateDeviceRGB()
        var data = [UInt8](repeating: 0, count: height * bytesPerRow)

        guard let context = CGContext(
            data: &data,
            width: width,
            height: height,
            bitsPerComponent: 8,
            bytesPerRow: bytesPerRow,
            space: colorSpace,
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
        ) else {
            throw IconAssetError.renderFailed("bitmap context for \(name)")
        }

        context.draw(image, in: CGRect(x: 0, y: 0, width: width, height: height))

        var minX = width
        var minY = height
        var maxX = -1
        var maxY = -1

        for y in 0..<height {
            for x in 0..<width {
                let offset = (y * bytesPerRow) + (x * bytesPerPixel) + 3
                if data[offset] > 8 {
                    minX = min(minX, x)
                    minY = min(minY, y)
                    maxX = max(maxX, x)
                    maxY = max(maxY, y)
                }
            }
        }

        guard maxX >= minX, maxY >= minY else {
            throw IconAssetError.alphaBoundsMissing(name)
        }

        return CGRect(x: minX, y: minY, width: maxX - minX + 1, height: maxY - minY + 1).integral
    }

    private func renderTightenedLogo(from logo: CGImage, croppedTo bounds: CGRect) throws -> CGImage {
        let canvas = CGSize(width: logoCanvasSize, height: logoCanvasSize)
        let targetRect = CGRect(
            x: logoPadding,
            y: logoPadding,
            width: canvas.width - (logoPadding * 2),
            height: canvas.height - (logoPadding * 2)
        )

        return try render(size: canvas, name: "tightened logo") { context in
            context.clear(CGRect(origin: .zero, size: canvas))
            draw(image: logo, croppedTo: bounds, in: fittedRect(for: bounds.size, inside: targetRect), context: context)
        }
    }

    private func renderMasterIcon(from tightenedLogo: CGImage) throws -> CGImage {
        let canvas = CGSize(width: iconCanvasSize, height: iconCanvasSize)
        let plateRect = CGRect(
            x: iconPlateInset,
            y: iconPlateInset,
            width: canvas.width - (iconPlateInset * 2),
            height: canvas.height - (iconPlateInset * 2)
        )
        let plateCornerRadius = plateRect.width * 0.22
        let inner = plateRect.insetBy(dx: plateRect.width * (1 - iconLogoScale) / 2,
                                      dy: plateRect.height * (1 - iconLogoScale) / 2)
        let fitPlate = inner.insetBy(dx: plateCornerRadius * iconLogoFitCornerFraction,
                                     dy: plateCornerRadius * iconLogoFitCornerFraction)
        let logoRect = fittedRect(
            for: CGSize(width: tightenedLogo.width, height: tightenedLogo.height),
            inside: fitPlate
        )

        return try render(size: canvas, name: "master icon") { context in
            context.clear(CGRect(origin: .zero, size: canvas))

            let shadow = NSShadow()
            shadow.shadowColor = NSColor.black.withAlphaComponent(0.18)
            shadow.shadowBlurRadius = 28
            shadow.shadowOffset = CGSize(width: 0, height: -10)
            shadow.set()

            let platePath = NSBezierPath(roundedRect: plateRect, xRadius: plateCornerRadius, yRadius: plateCornerRadius)
            let gradient = NSGradient(
                starting: NSColor(calibratedRed: 0.98, green: 0.985, blue: 0.995, alpha: 1),
                ending: NSColor(calibratedRed: 0.86, green: 0.88, blue: 0.92, alpha: 1)
            )!
            gradient.draw(in: platePath, angle: 90)

            NSGraphicsContext.saveGraphicsState()
            let graphicsContext = NSGraphicsContext(cgContext: context, flipped: false)
            NSGraphicsContext.current = graphicsContext

            NSColor.white.withAlphaComponent(0.35).setStroke()
            platePath.lineWidth = 3
            platePath.stroke()

            let highlightPath = NSBezierPath(roundedRect: plateRect.insetBy(dx: 14, dy: 14), xRadius: plateCornerRadius * 0.82, yRadius: plateCornerRadius * 0.82)
            NSColor.white.withAlphaComponent(0.08).setFill()
            highlightPath.fill()

            NSGraphicsContext.restoreGraphicsState()

            context.saveGState()
            context.addPath(platePath.cgPath)
            context.clip()
            draw(image: tightenedLogo, croppedTo: CGRect(x: 0, y: 0, width: tightenedLogo.width, height: tightenedLogo.height), in: logoRect, context: context)
            context.restoreGState()
        }
    }

    private func render(size: CGSize, name: String, drawing: (CGContext) -> Void) throws -> CGImage {
        let colorSpace = CGColorSpaceCreateDeviceRGB()
        guard let context = CGContext(
            data: nil,
            width: Int(size.width),
            height: Int(size.height),
            bitsPerComponent: 8,
            bytesPerRow: 0,
            space: colorSpace,
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
        ) else {
            throw IconAssetError.renderFailed("context for \(name)")
        }

        context.interpolationQuality = .high
        drawing(context)

        guard let image = context.makeImage() else {
            throw IconAssetError.renderFailed(name)
        }

        return image
    }

    private func draw(image: CGImage, croppedTo cropRect: CGRect, in rect: CGRect, context: CGContext) {
        let integralCrop = cropRect.integral
        guard let cropped = image.cropping(to: integralCrop) else { return }
        context.draw(cropped, in: rect.integral)
    }

    private func fittedRect(for imageSize: CGSize, inside container: CGRect) -> CGRect {
        let scale = min(container.width / imageSize.width, container.height / imageSize.height)
        let width = imageSize.width * scale
        let height = imageSize.height * scale
        return CGRect(
            x: container.midX - (width / 2),
            y: container.midY - (height / 2),
            width: width,
            height: height
        )
    }

    private func writePNG(_ image: CGImage, to url: URL) throws {
        guard let destination = CGImageDestinationCreateWithURL(url as CFURL, UTType.png.identifier as CFString, 1, nil) else {
            throw IconAssetError.renderFailed("png destination for \(url.lastPathComponent)")
        }

        CGImageDestinationAddImage(destination, image, nil)
        guard CGImageDestinationFinalize(destination) else {
            throw IconAssetError.renderFailed("png finalize for \(url.lastPathComponent)")
        }
    }

    private func writeIconset(_ masterIcon: CGImage, to iconsetURL: URL) throws {
        let sizes: [(String, Int)] = [
            ("icon_16x16.png", 16),
            ("icon_16x16@2x.png", 32),
            ("icon_32x32.png", 32),
            ("icon_32x32@2x.png", 64),
            ("icon_128x128.png", 128),
            ("icon_128x128@2x.png", 256),
            ("icon_256x256.png", 256),
            ("icon_256x256@2x.png", 512),
            ("icon_512x512.png", 512),
            ("icon_512x512@2x.png", 1024)
        ]

        for (fileName, size) in sizes {
            let scaled = try render(size: CGSize(width: size, height: size), name: fileName) { context in
                context.clear(CGRect(x: 0, y: 0, width: size, height: size))
                context.interpolationQuality = .high
                context.draw(masterIcon, in: CGRect(x: 0, y: 0, width: size, height: size))
            }
            try writePNG(scaled, to: iconsetURL.appendingPathComponent(fileName))
        }
    }

    private func rebuildICNS(from iconsetURL: URL, to icnsURL: URL) throws {
        let task = Process()
        task.executableURL = URL(fileURLWithPath: "/usr/bin/iconutil")
        task.arguments = ["-c", "icns", iconsetURL.path, "-o", icnsURL.path]
        try task.run()
        task.waitUntilExit()

        if task.terminationStatus != 0 {
            throw IconAssetError.renderFailed("iconutil")
        }
    }
}

do {
    try IconAssetGenerator().run()
    print("Updated app icon assets (iconset + rune_wheel.icns).")
} catch {
    fputs("error: \(error)\n", stderr)
    exit(1)
}
