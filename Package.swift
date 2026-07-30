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
    dependencies: [
        .package(url: "https://github.com/jpsim/Yams.git", exact: "6.2.2")
    ],
    targets: [
        .executableTarget(
            name: "RuneApp",
            dependencies: ["RuneUI", "RuneCore"]
        ),
        .executableTarget(
            name: "RuneFakeK8s",
            dependencies: ["RuneFakeK8sSupport"]
        ),
        .target(
            name: "RuneFakeK8sSupport"
        ),
        .target(name: "RuneCore"),
        .target(
            name: "RuneSecurity",
            dependencies: [
                "RuneCore",
                .product(name: "Yams", package: "Yams")
            ]
        ),
        .target(
            name: "RuneKube",
            dependencies: [
                "RuneCore",
                "RuneSecurity",
                "RuneDiagnostics",
                .product(name: "Yams", package: "Yams")
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
        .target(name: "RuneSharedCore"),
        .target(
            name: "RuneSharedUI",
            dependencies: ["RuneSharedCore"]
        ),
        .target(
            name: "RuneUI",
            dependencies: [
                "RuneSharedCore",
                "RuneSharedUI",
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
            dependencies: ["RuneKube", "RuneCore", "RuneSecurity", "RuneFakeK8sSupport"]
        ),
        .testTarget(
            name: "RuneStoreTests",
            dependencies: ["RuneStore"]
        ),
        .testTarget(
            name: "RuneSecurityTests",
            dependencies: ["RuneSecurity", "RuneCore"]
        ),
        .testTarget(
            name: "RuneUITests",
            dependencies: [
                "RuneSharedCore",
                "RuneUI",
                "RuneKube",
                "RuneDiagnostics",
                "RuneCore",
                "RuneSecurity",
                "RuneExport",
                "RuneStore",
                "RuneFakeK8sSupport"
            ]
        )
    ]
)
