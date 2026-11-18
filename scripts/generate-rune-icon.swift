import AppKit
import CoreGraphics

// Rune app icon drawn from geometry. Design space: 1024 x 1024, origin top-left.
// Renders PNG through Core Graphics and writes the same geometry as SVG.

struct RGBA { var r, g, b, a: Double
    init(_ hex: UInt32, _ a: Double = 1) { r = Double((hex >> 16) & 255) / 255; g = Double((hex >> 8) & 255) / 255; b = Double(hex & 255) / 255; self.a = a }
    var cg: CGColor { CGColor(srgbRed: r, green: g, blue: b, alpha: a) }
    var svg: String { String(format: "#%02X%02X%02X", Int(r * 255 + 0.5), Int(g * 255 + 0.5), Int(b * 255 + 0.5)) }
    func mix(_ o: RGBA, _ t: Double) -> RGBA { var c = self; c.r += (o.r - r) * t; c.g += (o.g - g) * t; c.b += (o.b - b) * t; c.a += (o.a - a) * t; return c }
}
typealias Stops = [(Double, RGBA)]
enum Paint {
    case solid(RGBA)
    case linear(Stops, CGPoint, CGPoint)
    case radial(Stops, CGPoint, Double)
}

// Path segments shared by both backends.
enum Seg { case move(CGPoint), line(CGPoint), quad(CGPoint, CGPoint), cubic(CGPoint, CGPoint, CGPoint), close }
struct Shape { var segs: [Seg] = []; var evenOdd = false }

func circle(_ c: CGPoint, _ r: Double) -> [Seg] {
    let k = 0.5522847498 * r
    return [.move(CGPoint(x: c.x + r, y: c.y)),
            .cubic(CGPoint(x: c.x + r, y: c.y + k), CGPoint(x: c.x + k, y: c.y + r), CGPoint(x: c.x, y: c.y + r)),
            .cubic(CGPoint(x: c.x - k, y: c.y + r), CGPoint(x: c.x - r, y: c.y + k), CGPoint(x: c.x - r, y: c.y)),
            .cubic(CGPoint(x: c.x - r, y: c.y - k), CGPoint(x: c.x - k, y: c.y - r), CGPoint(x: c.x, y: c.y - r)),
            .cubic(CGPoint(x: c.x + k, y: c.y - r), CGPoint(x: c.x + r, y: c.y - k), CGPoint(x: c.x + r, y: c.y)), .close]
}
func roundedPolygon(_ pts: [CGPoint], radii: [Double]) -> [Seg] {
    var segs: [Seg] = []
    let n = pts.count
    for i in 0..<n {
        let v = pts[i], prev = pts[(i + n - 1) % n], next = pts[(i + 1) % n]
        func toward(_ p: CGPoint, _ d: Double) -> CGPoint { let dx = p.x - v.x, dy = p.y - v.y, l = hypot(dx, dy); return CGPoint(x: v.x + dx / l * d, y: v.y + dy / l * d) }
        let r = radii[i]
        let pin = toward(prev, r), pout = toward(next, r)
        segs.append(i == 0 ? .move(pin) : .line(pin))
        segs.append(.quad(v, pout))
    }
    segs.append(.close)
    return segs
}
func capsule(_ a: CGPoint, _ b: CGPoint, _ r: Double) -> [Seg] {
    let dx = b.x - a.x, dy = b.y - a.y, l = hypot(dx, dy), ux = dx / l, uy = dy / l, nx = -uy * r, ny = ux * r
    let k = 0.5522847498 * r
    let a1 = CGPoint(x: a.x + nx, y: a.y + ny), a2 = CGPoint(x: a.x - nx, y: a.y - ny)
    let b1 = CGPoint(x: b.x + nx, y: b.y + ny), b2 = CGPoint(x: b.x - nx, y: b.y - ny)
    let bt = CGPoint(x: b.x + ux * r, y: b.y + uy * r), at = CGPoint(x: a.x - ux * r, y: a.y - uy * r)
    return [.move(a1), .line(b1),
            .cubic(CGPoint(x: b1.x + ux * k, y: b1.y + uy * k), CGPoint(x: bt.x + nx / r * k, y: bt.y + ny / r * k), bt),
            .cubic(CGPoint(x: bt.x - nx / r * k, y: bt.y - ny / r * k), CGPoint(x: b2.x + ux * k, y: b2.y + uy * k), b2),
            .line(a2),
            .cubic(CGPoint(x: a2.x - ux * k, y: a2.y - uy * k), CGPoint(x: at.x - nx / r * k, y: at.y - ny / r * k), at),
            .cubic(CGPoint(x: at.x + nx / r * k, y: at.y + ny / r * k), CGPoint(x: a1.x - ux * k, y: a1.y - uy * k), a1), .close]
}
func squircle(_ rect: CGRect, _ n: Double = 5) -> [Seg] {
    var segs: [Seg] = []
    let a = rect.width / 2, b = rect.height / 2
    for s in 0..<360 {
        let t = Double(s) / 360 * 2 * .pi, c = cos(t), si = sin(t)
        let p = CGPoint(x: rect.midX + a * (c < 0 ? -1 : 1) * pow(abs(c), 2 / n), y: rect.midY + b * (si < 0 ? -1 : 1) * pow(abs(si), 2 / n))
        segs.append(s == 0 ? .move(p) : .line(p))
    }
    segs.append(.close)
    return segs
}
// Four-point sparkle: long horizontal points, slightly shorter vertical ones.
func star(_ c: CGPoint, long: Double, waist: Double, rot: Double) -> [Seg] {
    var segs: [Seg] = []
    for i in 0..<8 {
        let t = rot + Double(i) * .pi / 4
        let r = i % 2 == 1 ? waist : (i % 4 == 0 ? long : long * 0.8)
        let p = CGPoint(x: c.x + cos(t) * r, y: c.y + sin(t) * r)
        segs.append(i == 0 ? .move(p) : .line(p))
    }
    segs.append(.close)
    return segs
}

enum Op {
    case fill(Shape, Paint, glow: (RGBA, Double)?, clip: [Seg]?)
    case stroke([Seg], RGBA, width: Double, glow: (RGBA, Double)?, clip: [Seg]?)
    case shadow(Shape, RGBA, dy: Double, blur: Double)
}
var ops: [Op] = []

let C = CGPoint(x: 512, y: 512)
func P(_ x: Double, _ y: Double) -> CGPoint { CGPoint(x: x, y: y) }
func polar(_ r: Double, _ deg: Double) -> CGPoint { P(512 + cos(deg * .pi / 180) * r, 512 + sin(deg * .pi / 180) * r) }
let lightFrom = P(300, 250), lightTo = P(724, 800) // light comes from the top left

// Tile
let tileRect = CGRect(x: 100, y: 100, width: 824, height: 824)
ops.append(.shadow(Shape(segs: squircle(tileRect)), RGBA(0x000000, 0.28), dy: 10, blur: 24))
ops.append(.fill(Shape(segs: squircle(tileRect)), .linear([(0, RGBA(0xFFFFFF)), (1, RGBA(0xECF1F8))], P(512, 100), P(512, 924)), glow: nil, clip: nil))

// Spokes, behind the ring
let spokeTip = 391.0, spokeR = 19.0
for i in 0..<8 {
    let deg = Double(i) * 45
    let lit = (cos((deg - 225) * .pi / 180) + 1) / 2
    let a = polar(300, deg), b = polar(spokeTip - spokeR, deg)
    ops.append(.fill(Shape(segs: capsule(a, b, spokeR)), .solid(RGBA(0x022B66)), glow: nil, clip: nil))
    let body = RGBA(0x0A4C90).mix(RGBA(0x1A8AD0), lit)
    ops.append(.fill(Shape(segs: capsule(a, b, spokeR - 2.5)), .solid(body), glow: nil, clip: nil))
    ops.append(.stroke(capsule(a, b, spokeR - 6.5), RGBA(0x6CB8E6, 0.35 + 0.3 * lit), width: 2, glow: nil, clip: nil))
}

// Ring bands, outside in
func band(_ r: Double, _ paint: Paint) { ops.append(.fill(Shape(segs: circle(C, r)), paint, glow: nil, clip: nil)) }
band(329, .solid(RGBA(0x022B66)))
band(326.5, .linear([(0, RGBA(0x4AA6DE)), (1, RGBA(0x0D4884))], lightFrom, lightTo))
band(322, .linear([(0, RGBA(0x0C86C8)), (0.55, RGBA(0x065A9E)), (1, RGBA(0x032F6A))], lightFrom, lightTo))
band(296, .solid(RGBA(0x00296A)))
band(292.5, .linear([(0, RGBA(0xC4F6FF)), (0.5, RGBA(0x7ACFF5)), (1, RGBA(0x3A86CF))], lightFrom, lightTo))
band(278, .linear([(0, RGBA(0x1A7FC0)), (1, RGBA(0x072E68))], lightFrom, lightTo))
band(266, .radial([(0, RGBA(0x0B4E9C)), (0.55, RGBA(0x032F6E)), (1, RGBA(0x01123C))], P(420, 390), 400))

// Network lines and sparkles inside the disc
let disc = circle(C, 264)
let s1 = P(375, 444), s2 = P(405, 575), s3 = P(633, 385), s4 = P(744, 481), s5 = P(527, 692), s6 = P(434, 268)
let lines: [[CGPoint]] = [
    [P(308, 343), P(426, 393)], [P(308, 343), s1], [P(262, 498), s1], [s1, P(426, 393)], [s1, s2],
    [P(266, 508), s2], [P(287, 636), s2], [s2, P(434, 520)],
    [s3, P(708, 338)], [P(582, 262), P(584, 348)], [P(584, 348), s3], [s3, P(690, 470)],
    [s4, P(760, 432)], [s4, P(685, 628)], [s4, P(603, 526)], [s4, P(776, 486)],
    [s5, P(448, 755)], [s5, P(578, 761)], [s5, P(560, 620)],
    [s6, P(470, 290)]
]
for l in lines {
    ops.append(.stroke([.move(l[0]), .line(l[1])], RGBA(0x86DCFF, 0.62), width: 2.6, glow: (RGBA(0x3FB8FF, 0.9), 9), clip: disc))
}
for (s, size) in [(s1, 1.0), (s2, 1.0), (s3, 0.8), (s4, 1.0), (s5, 0.75), (s6, 0.5)] {
    ops.append(.fill(Shape(segs: circle(s, 38 * size)), .radial([(0, RGBA(0xD6F7FF, 0.95)), (0.18, RGBA(0x7AD6FF, 0.55)), (0.5, RGBA(0x3AA6F0, 0.18)), (1, RGBA(0x2A8FE0, 0))], s, 38 * size), glow: nil, clip: disc))
    ops.append(.fill(Shape(segs: star(s, long: 30 * size, waist: 2.6 * size, rot: 0)), .solid(RGBA(0xF4FCFF, 0.95)), glow: (RGBA(0x7FDBFF, 1), 5), clip: disc))
    ops.append(.fill(Shape(segs: star(s, long: 15 * size, waist: 1.8 * size, rot: .pi / 8)), .solid(RGBA(0xF4FCFF, 0.8)), glow: nil, clip: disc))
}

// The R: stem, chevron bowl and leg, with the counter cut out.
let rOuter = [P(433.7, 283.6), P(682, 444), P(556.7, 535.6), P(676.4, 655.3), P(634.1, 687.6), P(517.3, 561),
              P(486.3, 581.7), P(486.3, 687.6), P(433.7, 718.6)]
let rCounter = [P(486.3, 382.2), P(586.3, 446.9), P(486.3, 514.5)]
let rShape = Shape(segs: roundedPolygon(rOuter, radii: [3, 9, 2, 3, 3, 2, 1.5, 2, 3]) + roundedPolygon(rCounter, radii: [1.5, 3, 1.5]), evenOdd: true)
let rPaint = Paint.linear([(0, RGBA(0xF4FEFF)), (0.35, RGBA(0xB2F0FD)), (0.7, RGBA(0x5CC6F4)), (1, RGBA(0x3A80DA))], P(470, 283), P(560, 719))
ops.append(.fill(rShape, rPaint, glow: (RGBA(0x4CCBFF, 0.9), 30), clip: nil))
// Stem side face, slightly darker, gives the stem its bevel.
ops.append(.fill(Shape(segs: roundedPolygon([P(474, 522), P(486.3, 514.5), P(486.3, 687.6), P(474, 694.8)], radii: [1, 1, 1, 1])),
                 .linear([(0, RGBA(0x2E74C9, 0.0)), (1, RGBA(0x2E74C9, 0.45))], P(474, 520), P(474, 690)), glow: nil, clip: nil))
ops.append(.stroke(rShape.segs, RGBA(0xFFFFFF, 0.45), width: 1.4, glow: nil, clip: nil))

// MARK: Core Graphics backend
func cgPath(_ segs: [Seg]) -> CGPath {
    let p = CGMutablePath()
    for s in segs {
        switch s {
        case .move(let a): p.move(to: a)
        case .line(let a): p.addLine(to: a)
        case .quad(let c, let a): p.addQuadCurve(to: a, control: c)
        case .cubic(let c1, let c2, let a): p.addCurve(to: a, control1: c1, control2: c2)
        case .close: p.closeSubpath()
        }
    }
    return p
}
let cs = CGColorSpace(name: CGColorSpace.sRGB)!
func gradient(_ stops: Stops) -> CGGradient {
    CGGradient(colorsSpace: cs, colors: stops.map { $0.1.cg } as CFArray, locations: stops.map { CGFloat($0.0) })!
}
func render(size: Int) -> CGImage {
    let ctx = CGContext(data: nil, width: size, height: size, bitsPerComponent: 8, bytesPerRow: size * 4, space: cs,
                        bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue)!
    let k = Double(size) / 1024
    ctx.translateBy(x: 0, y: CGFloat(size)); ctx.scaleBy(x: k, y: -k)
    ctx.setShouldAntialias(true)
    func paintFill(_ shape: Shape, _ paint: Paint) {
        ctx.addPath(cgPath(shape.segs))
        switch paint {
        case .solid(let c): ctx.setFillColor(c.cg); ctx.fillPath(using: shape.evenOdd ? .evenOdd : .winding)
        case .linear(let st, let a, let b):
            ctx.saveGState(); ctx.clip(using: shape.evenOdd ? .evenOdd : .winding)
            ctx.drawLinearGradient(gradient(st), start: a, end: b, options: [.drawsBeforeStartLocation, .drawsAfterEndLocation]); ctx.restoreGState()
        case .radial(let st, let c, let r):
            ctx.saveGState(); ctx.clip(using: shape.evenOdd ? .evenOdd : .winding)
            ctx.drawRadialGradient(gradient(st), startCenter: c, startRadius: 0, endCenter: c, endRadius: r, options: [.drawsAfterEndLocation]); ctx.restoreGState()
        }
    }
    for op in ops {
        ctx.saveGState()
        switch op {
        case .shadow(let shape, let color, let dy, let blur):
            // CG shadow offsets ignore the CTM flip, so pass the device-space direction.
            ctx.setShadow(offset: CGSize(width: 0, height: -dy * k), blur: blur * k, color: color.cg)
            ctx.addPath(cgPath(shape.segs)); ctx.setFillColor(RGBA(0xFFFFFF).cg); ctx.fillPath()
        case .fill(let shape, let paint, let glow, let clip):
            if let clip { ctx.addPath(cgPath(clip)); ctx.clip() }
            if let glow {
                ctx.saveGState()
                ctx.setShadow(offset: .zero, blur: glow.1 * k, color: glow.0.cg)
                ctx.beginTransparencyLayer(auxiliaryInfo: nil)
                paintFill(shape, paint)
                ctx.endTransparencyLayer()
                ctx.restoreGState()
            } else { paintFill(shape, paint) }
        case .stroke(let segs, let color, let width, let glow, let clip):
            if let clip { ctx.addPath(cgPath(clip)); ctx.clip() }
            if let glow { ctx.setShadow(offset: .zero, blur: glow.1 * k, color: glow.0.cg) }
            ctx.addPath(cgPath(segs)); ctx.setStrokeColor(color.cg); ctx.setLineWidth(width); ctx.setLineCap(.round); ctx.strokePath()
        }
        ctx.restoreGState()
    }
    return ctx.makeImage()!
}

// MARK: SVG backend
func svgPath(_ segs: [Seg]) -> String {
    func f(_ p: CGPoint) -> String { String(format: "%.2f %.2f", p.x, p.y) }
    return segs.map { s -> String in
        switch s {
        case .move(let a): return "M\(f(a))"
        case .line(let a): return "L\(f(a))"
        case .quad(let c, let a): return "Q\(f(c)) \(f(a))"
        case .cubic(let c1, let c2, let a): return "C\(f(c1)) \(f(c2)) \(f(a))"
        case .close: return "Z"
        }
    }.joined()
}
func svgDocument() -> String {
    var defs: [String] = [], body: [String] = []
    var n = 0
    func id(_ p: String) -> String { n += 1; return "\(p)\(n)" }
    func stopsXML(_ st: Stops) -> String { st.map { String(format: "<stop offset=\"%.3f\" stop-color=\"%@\" stop-opacity=\"%.3f\"/>", $0.0, $0.1.svg, $0.1.a) }.joined() }
    func paintRef(_ p: Paint) -> String {
        switch p {
        case .solid(let c): return String(format: "fill=\"%@\" fill-opacity=\"%.3f\"", c.svg, c.a)
        case .linear(let st, let a, let b):
            let i = id("g"); defs.append(String(format: "<linearGradient id=\"%@\" gradientUnits=\"userSpaceOnUse\" x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\">%@</linearGradient>", i, a.x, a.y, b.x, b.y, stopsXML(st)))
            return "fill=\"url(#\(i))\""
        case .radial(let st, let c, let r):
            let i = id("g"); defs.append(String(format: "<radialGradient id=\"%@\" gradientUnits=\"userSpaceOnUse\" cx=\"%.1f\" cy=\"%.1f\" r=\"%.1f\">%@</radialGradient>", i, c.x, c.y, r, stopsXML(st)))
            return "fill=\"url(#\(i))\""
        }
    }
    func glowFilter(_ g: (RGBA, Double)) -> String {
        let i = id("f")
        defs.append(String(format: "<filter id=\"%@\" x=\"-50%%\" y=\"-50%%\" width=\"200%%\" height=\"200%%\"><feGaussianBlur in=\"SourceAlpha\" stdDeviation=\"%.1f\" result=\"b\"/><feFlood flood-color=\"%@\" flood-opacity=\"%.2f\" result=\"c\"/><feComposite operator=\"in\" in=\"c\" in2=\"b\" result=\"g\"/><feMerge><feMergeNode in=\"g\"/><feMergeNode in=\"SourceGraphic\"/></feMerge></filter>", i, g.1 / 2, g.0.svg, g.0.a))
        return "filter=\"url(#\(i))\""
    }
    func clipRef(_ c: [Seg]?) -> String {
        guard let c else { return "" }
        let i = id("c"); defs.append("<clipPath id=\"\(i)\"><path d=\"\(svgPath(c))\"/></clipPath>")
        return "clip-path=\"url(#\(i))\""
    }
    for op in ops {
        switch op {
        case .shadow(let shape, let color, let dy, let blur):
            let i = id("f")
            defs.append(String(format: "<filter id=\"%@\" x=\"-20%%\" y=\"-20%%\" width=\"140%%\" height=\"140%%\"><feDropShadow dx=\"0\" dy=\"%.1f\" stdDeviation=\"%.1f\" flood-color=\"%@\" flood-opacity=\"%.2f\"/></filter>", i, dy, blur / 2, color.svg, color.a))
            body.append("<path d=\"\(svgPath(shape.segs))\" fill=\"#FFFFFF\" filter=\"url(#\(i))\"/>")
        case .fill(let shape, let paint, let glow, let clip):
            let g = glow.map(glowFilter) ?? ""
            body.append("<g \(clipRef(clip))><path d=\"\(svgPath(shape.segs))\" \(paintRef(paint)) fill-rule=\"\(shape.evenOdd ? "evenodd" : "nonzero")\" \(g)/></g>")
        case .stroke(let segs, let color, let width, let glow, let clip):
            let g = glow.map(glowFilter) ?? ""
            body.append(String(format: "<g %@><path d=\"%@\" fill=\"none\" stroke=\"%@\" stroke-opacity=\"%.3f\" stroke-width=\"%.2f\" stroke-linecap=\"round\" %@/></g>", clipRef(clip), svgPath(segs), color.svg, color.a, width, g))
        }
    }
    return "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"1024\" height=\"1024\" viewBox=\"0 0 1024 1024\">\n<defs>\(defs.joined(separator: "\n"))</defs>\n\(body.joined(separator: "\n"))\n</svg>\n"
}

// MARK: Output
// Usage: swift scripts/generate-rune-icon.swift [assets dir]
// Writes rune_icon.svg, rune_app_iconset_wheel.iconset and rune_wheel.icns.
let outDir = CommandLine.arguments.count > 1 ? CommandLine.arguments[1] : "assets"
func writePNG(_ img: CGImage, _ path: String) {
    let rep = NSBitmapImageRep(cgImage: img)
    try! rep.representation(using: .png, properties: [:])!.write(to: URL(fileURLWithPath: path))
}
try! svgDocument().write(toFile: "\(outDir)/rune_icon.svg", atomically: true, encoding: .utf8)
let iconset = "\(outDir)/rune_app_iconset_wheel.iconset"
try? FileManager.default.removeItem(atPath: iconset)
try! FileManager.default.createDirectory(atPath: iconset, withIntermediateDirectories: true)
for s in [16, 32, 128, 256, 512] {
    writePNG(render(size: s), "\(iconset)/icon_\(s)x\(s).png")
    writePNG(render(size: s * 2), "\(iconset)/icon_\(s)x\(s)@2x.png")
}
let iconutil = Process()
iconutil.executableURL = URL(fileURLWithPath: "/usr/bin/iconutil")
iconutil.arguments = ["-c", "icns", iconset, "-o", "\(outDir)/rune_wheel.icns"]
try! iconutil.run()
iconutil.waitUntilExit()
guard iconutil.terminationStatus == 0 else { fatalError("iconutil failed") }
print("Updated \(outDir)/rune_icon.svg, rune_app_iconset_wheel.iconset and rune_wheel.icns")
