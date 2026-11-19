import XCTest
@testable import RuneKube

final class KubernetesQuantityTests: XCTestCase {
    func testCPUQuantitiesBecomeMillicores() {
        XCTAssertEqual(KubernetesQuantity.cpuMillicores("250m"), 250)
        XCTAssertEqual(KubernetesQuantity.cpuMillicores("2"), 2_000)
        XCTAssertEqual(KubernetesQuantity.cpuMillicores("0.5"), 500)
        XCTAssertEqual(KubernetesQuantity.cpuMillicores("1500000n"), 2)
        XCTAssertEqual(KubernetesQuantity.cpuMillicores("42000000n"), 42)
        XCTAssertEqual(KubernetesQuantity.cpuMillicores("31000u"), 31)
        XCTAssertEqual(KubernetesQuantity.cpuMillicores("1e3"), 1_000_000)
        XCTAssertEqual(KubernetesQuantity.cpuMillicores("2k"), 2_000_000)
        XCTAssertEqual(KubernetesQuantity.cpuMillicores(" 100m "), 100)
    }

    func testMemoryQuantitiesBecomeBytes() {
        XCTAssertEqual(KubernetesQuantity.memoryBytes("96Mi"), 96 * 1024 * 1024)
        XCTAssertEqual(KubernetesQuantity.memoryBytes("16384000Ki"), 16_384_000 * 1024)
        XCTAssertEqual(KubernetesQuantity.memoryBytes("2Gi"), 2 * 1024 * 1024 * 1024)
        XCTAssertEqual(KubernetesQuantity.memoryBytes("134217728"), 134_217_728)
        XCTAssertEqual(KubernetesQuantity.memoryBytes("129e6"), 129_000_000)
        XCTAssertEqual(KubernetesQuantity.memoryBytes("500M"), 500_000_000)
        XCTAssertEqual(KubernetesQuantity.memoryBytes("3Pi"), 3 * 1024 * 1024 * 1024 * 1024 * 1024)
        // Lower-case k is the decimal kilo; milli-bytes come from averaged metrics.
        XCTAssertEqual(KubernetesQuantity.memoryBytes("123k"), 123_000)
        XCTAssertEqual(KubernetesQuantity.memoryBytes("128974848000m"), 128_974_848)
    }

    func testWhatIsNotAQuantityCountsAsZero() {
        for value in [nil, "", "   ", "abc", "m", "Mi", "inf", "infinity", "nan", "-inf", "0x10", "0x1p3", "1,5"] {
            XCTAssertEqual(KubernetesQuantity.cpuMillicores(value), 0, value ?? "nil")
            XCTAssertEqual(KubernetesQuantity.memoryBytes(value), 0, value ?? "nil")
        }
        XCTAssertEqual(KubernetesQuantity.cpuMillicores("infn"), 0)
        XCTAssertEqual(KubernetesQuantity.cpuMillicores("nanm"), 0)
        XCTAssertEqual(KubernetesQuantity.memoryBytes("infKi"), 0)
        XCTAssertEqual(KubernetesQuantity.cpuMillicores("-5"), 0)
        XCTAssertEqual(KubernetesQuantity.memoryBytes("-5Mi"), 0)
    }

    func testValuesFarOutsideAnyClusterStayFiniteAndSummable() {
        // Converted to Int unbounded, any of these ends the process.
        let cpu = ["1e30", "1e300", "9e18", "999999999999999999999999k"].map(KubernetesQuantity.cpuMillicores)
        XCTAssertEqual(cpu.reduce(0, +), cpu.count * 1_000_000_000_000)
        // Too large for a Double at all: not a finite number, so zero.
        XCTAssertEqual(KubernetesQuantity.cpuMillicores("1e400"), 0)
        let memory = ["1e300", "1e308Ei", "9e30Ei", "1e400"].map(KubernetesQuantity.memoryBytes)
        XCTAssertTrue(memory.allSatisfy(\.isFinite))

        XCTAssertEqual(KubernetesQuantity.clampedInt(.nan), 0)
        XCTAssertEqual(KubernetesQuantity.clampedInt(.infinity), 0)
        XCTAssertEqual(KubernetesQuantity.clampedInt(-.infinity), 0)
        XCTAssertEqual(KubernetesQuantity.clampedInt(1e300), 1_000_000_000_000_000_000)
        XCTAssertEqual(KubernetesQuantity.clampedInt(-1e300), -1_000_000_000_000_000_000)
        XCTAssertEqual(KubernetesQuantity.clampedInt(42.4), 42)
    }
}
