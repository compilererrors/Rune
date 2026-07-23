import Foundation
import XCTest
@testable import RuneKube

final class BoundedCredentialCacheTests: XCTestCase {
    func testBoundedCacheEvictsLeastRecentlyUsedEntry() {
        let now = Date(timeIntervalSince1970: 1_000)
        let expiration = now.addingTimeInterval(60)
        var cache = BoundedExpiringLRUCache<String, String>(capacity: 3)

        cache.insert("a", for: "a", expirationDate: expiration, now: now)
        cache.insert("b", for: "b", expirationDate: expiration, now: now)
        cache.insert("c", for: "c", expirationDate: expiration, now: now)
        guard case .hit("a") = cache.lookup(for: "a", now: now) else {
            return XCTFail("Expected the touched entry to remain available")
        }

        cache.insert("d", for: "d", expirationDate: expiration, now: now)

        XCTAssertEqual(cache.count, 3)
        XCTAssertEqual(cache.keys, Set(["a", "c", "d"]))
        guard case .miss = cache.lookup(for: "b", now: now) else {
            return XCTFail("Expected the least recently used entry to be evicted")
        }
    }

    func testBoundedCacheExpiresAndReleasesValuesAtTTL() {
        let now = Date(timeIntervalSince1970: 2_000)
        var cache = BoundedExpiringLRUCache<String, String>(capacity: 4)
        cache.insert(
            "synthetic-credential",
            for: "credential",
            expirationDate: now.addingTimeInterval(10),
            now: now
        )

        guard case .hit("synthetic-credential") = cache.lookup(
            for: "credential",
            now: now.addingTimeInterval(9)
        ) else {
            return XCTFail("Expected the entry to remain cached before its TTL")
        }
        guard case .expired(let value, let expirationDate) = cache.lookup(
            for: "credential",
            now: now.addingTimeInterval(10)
        ) else {
            return XCTFail("Expected the entry to expire exactly at its TTL")
        }

        XCTAssertEqual(value, "synthetic-credential")
        XCTAssertEqual(expirationDate, now.addingTimeInterval(10))
        XCTAssertEqual(cache.count, 0)
        XCTAssertTrue(cache.keys.isEmpty)
    }

    func testBoundedCacheChurnStaysWithinCapacityAndKPI() {
        let now = Date(timeIntervalSince1970: 3_000)
        let expiration = now.addingTimeInterval(60)
        var cache = BoundedExpiringLRUCache<Int, Int>(capacity: 64)
        let clock = ContinuousClock()
        let started = clock.now

        for index in 0..<10_000 {
            cache.insert(index, for: index, expirationDate: expiration, now: now)
            guard case .hit(index) = cache.lookup(for: index, now: now) else {
                return XCTFail("Expected the newest cache entry to remain available")
            }
        }

        let elapsed = started.duration(to: clock.now)
        XCTAssertEqual(cache.count, 64)
        XCTAssertEqual(cache.keys, Set(9_936..<10_000))
        XCTAssertLessThan(
            elapsed,
            .seconds(2),
            "KPI: 10,000 bounded cache rotations should complete within two seconds"
        )
    }

    func testSensitiveCacheKeyIsOpaqueStableAndNamespaceSeparated() {
        let material = "synthetic-token-and-certificate-material"
        let components: [KubernetesSensitiveCacheKeyComponent] = [.string(material)]
        let first = KubernetesSensitiveCacheKey.make(namespace: "exec", components: components)
        let repeated = KubernetesSensitiveCacheKey.make(namespace: "exec", components: components)
        let config = KubernetesSensitiveCacheKey.make(namespace: "config", components: components)

        XCTAssertEqual(first, repeated)
        XCTAssertNotEqual(first, config)
        XCTAssertTrue(first.hasPrefix("exec:"))
        XCTAssertFalse(first.contains(material))
        XCTAssertFalse(first.contains("synthetic-token"))
    }

    func testSensitiveCacheKeyUsesUnambiguousTypedEncoding() {
        let separator = "\u{1e}"
        let embeddedSeparator = KubernetesSensitiveCacheKey.make(
            namespace: "exec",
            components: [.strings(["left\(separator)right"])]
        )
        let twoArguments = KubernetesSensitiveCacheKey.make(
            namespace: "exec",
            components: [.strings(["left", "right"])]
        )
        let nilValue = KubernetesSensitiveCacheKey.make(
            namespace: "exec",
            components: [.string(nil)]
        )
        let emptyValue = KubernetesSensitiveCacheKey.make(
            namespace: "exec",
            components: [.string("")]
        )

        XCTAssertNotEqual(embeddedSeparator, twoArguments)
        XCTAssertNotEqual(nilValue, emptyValue)
    }

    func testEffectiveExecEnvironmentUsesLastDuplicateValue() {
        let duplicated = KubernetesSensitiveCacheKey.effectiveEnvironmentPairs(
            processEnvironment: [:],
            baseEnvironment: [:],
            execEnvironment: [
                ("SYNTHETIC_VALUE", "old"),
                ("SYNTHETIC_VALUE", "current")
            ]
        )
        let effective = KubernetesSensitiveCacheKey.effectiveEnvironmentPairs(
            processEnvironment: [:],
            baseEnvironment: [:],
            execEnvironment: [("SYNTHETIC_VALUE", "current")]
        )

        XCTAssertEqual(
            duplicated.map { "\($0.0)=\($0.1)" },
            effective.map { "\($0.0)=\($0.1)" }
        )
    }

    func testKubeconfigContentDigestDetectsSameSizeSameMtimeRewrite() async throws {
        let fileManager = FileManager.default
        let directory = fileManager.temporaryDirectory
            .appendingPathComponent("rune-config-cache-\(UUID().uuidString)", isDirectory: true)
        try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? fileManager.removeItem(at: directory) }
        let kubeconfig = directory.appendingPathComponent("config.yaml")
        let fixedModificationDate = Date(timeIntervalSince1970: 4_000)
        let first = kubeconfigYAML(namespace: "synthetic-alpha")
        let second = kubeconfigYAML(namespace: "synthetic-bravo")
        XCTAssertEqual(Data(first.utf8).count, Data(second.utf8).count)

        try first.write(to: kubeconfig, atomically: true, encoding: .utf8)
        try fileManager.setAttributes(
            [.modificationDate: fixedModificationDate],
            ofItemAtPath: kubeconfig.path
        )
        let client = KubernetesRESTClient()
        let environment = ["KUBECONFIG": kubeconfig.path]
        let initial = try await client.contextNamespace(
            environment: environment,
            contextName: "synthetic-context"
        )

        try second.write(to: kubeconfig, atomically: true, encoding: .utf8)
        try fileManager.setAttributes(
            [.modificationDate: fixedModificationDate],
            ofItemAtPath: kubeconfig.path
        )
        let rewritten = try await client.contextNamespace(
            environment: environment,
            contextName: "synthetic-context"
        )

        XCTAssertEqual(initial, "synthetic-alpha")
        XCTAssertEqual(rewritten, "synthetic-bravo")
    }

    func testLateStaleInvalidationPreservesFreshCredentialGeneration() async throws {
        let preserved = try await KubernetesRESTClient
            ._testLateExecCredentialInvalidationPreservesFreshGeneration()
        XCTAssertTrue(preserved)
    }

    func testProductionCacheActorsEnforceCapacityAndActiveTTL() async throws {
        let expired = try await KubernetesRESTClient._testBoundedCredentialCachesActivelyExpire()
        XCTAssertTrue(expired)
    }

    private func kubeconfigYAML(namespace: String) -> String {
        """
        apiVersion: v1
        kind: Config
        current-context: synthetic-context
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://127.0.0.1:6443
        contexts:
        - name: synthetic-context
          context:
            cluster: synthetic-cluster
            user: synthetic-user
            namespace: \(namespace)
        users:
        - name: synthetic-user
          user: {}
        """
    }
}
