// swift-tools-version: 5.10
import PackageDescription

let package = Package(
    name: "NanoGraph",
    platforms: [
        .macOS(.v13),
    ],
    products: [
        .library(
            name: "NanoGraph",
            targets: ["NanoGraph"]
        ),
    ],
    targets: [
        .target(
            name: "CNanoGraph",
            path: "Sources/CNanoGraph",
            publicHeadersPath: "include"
        ),
        .target(
            name: "NanoGraph",
            dependencies: ["CNanoGraph"],
            path: "Sources/NanoGraph",
            linkerSettings: [
                .linkedLibrary("nanograph_ffi"),
                // Package root is crates/nanograph-ffi/swift
                .unsafeFlags(["-L", "../../../target/debug"], .when(configuration: .debug)),
                .unsafeFlags(
                    ["-Xlinker", "-rpath", "-Xlinker", "../../../target/debug"],
                    .when(configuration: .debug)
                ),
                .unsafeFlags(["-L", "../../../target/release"], .when(configuration: .release)),
                .unsafeFlags(
                    ["-Xlinker", "-rpath", "-Xlinker", "../../../target/release"],
                    .when(configuration: .release)
                ),
            ]
        ),
        .testTarget(
            name: "NanoGraphTests",
            dependencies: ["NanoGraph"],
            path: "Tests/NanoGraphTests"
        ),
    ]
)
