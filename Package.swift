// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "Rune",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .executable(name: "RuneApp", targets: ["RuneApp"]),
        .executable(name: "RuneFakeK8s", targets: ["RuneFakeK8s"]),
        .library(name: "RuneCore", targets: ["RuneCore"]),
        .library(name: "RuneUI", targets: ["RuneUI"]),
        .library(name: "RuneKube", targets: ["RuneKube"]),
        .library(name: "RuneStore", targets: ["RuneStore"]),
        .library(name: "RuneSecurity", targets: ["RuneSecurity"]),
        .library(name: "RuneExport", targets: ["RuneExport"]),
        .library(name: "RuneDiagnostics", targets: ["RuneDiagnostics"])
    ],
    dependencies: [],
    targets: [
        .executableTarget(
            name: "RuneApp",
            dependencies: ["RuneUI"]
        ),
        .executableTarget(
            name: "RuneFakeK8s"
        ),
        .target(name: "RuneCore"),
        .target(
            name: "RuneSecurity",
            dependencies: ["RuneCore"]
        ),
        .target(
            name: "RuneKube",
            dependencies: [
                "RuneCore",
                "RuneSecurity",
                "RuneDiagnostics"
            ]
        ),
        .target(
            name: "RuneStore",
            dependencies: ["RuneCore"]
        ),
        .target(
            name: "RuneExport",
            dependencies: ["RuneCore"]
        ),
        .target(
            name: "RuneDiagnostics",
            dependencies: ["RuneCore"]
        ),
        .target(
            name: "RuneUI",
            dependencies: [
                "RuneCore",
                "RuneKube",
                "RuneDiagnostics",
                "RuneStore",
                "RuneSecurity",
                "RuneExport"
            ],
            resources: [
                .process("Resources")
            ]
        ),
        .testTarget(
            name: "RuneCoreTests",
            dependencies: ["RuneCore"]
        ),
        .testTarget(
            name: "RuneKubeTests",
            dependencies: ["RuneKube", "RuneCore"]
        ),
        .testTarget(
            name: "RuneStoreTests",
            dependencies: ["RuneStore"]
        ),
        .testTarget(
            name: "RuneUITests",
            dependencies: ["RuneUI", "RuneKube", "RuneDiagnostics", "RuneCore", "RuneSecurity", "RuneExport"]
        )
    ]
)
