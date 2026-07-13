import Foundation
import XCTest
@testable import RuneCore
@testable import RuneSecurity

final class NativeKubernetesAuthenticationTests: XCTestCase {
    func testAnalyzerClassifiesKnownExecAndAuthProviderForms() throws {
        let raw = """
        apiVersion: v1
        kind: Config
        current-context: synthetic-eks
        clusters:
        - &eks-cluster
          name: synthetic-eks-cluster
          cluster:
            server: https://synthetic.eks.amazonaws.com
        - name: synthetic-azure-cluster
          cluster:
            server: https://synthetic.azmk8s.io
        - name: synthetic-gke-cluster
          cluster:
            server: https://synthetic.example.invalid
        - name: synthetic-oidc-cluster
          cluster:
            server: https://oidc.example.invalid
        contexts:
        - name: synthetic-eks
          context:
            cluster: synthetic-eks-cluster
            user: synthetic-eks-user
        - name: synthetic-azure
          context:
            cluster: synthetic-azure-cluster
            user: synthetic-azure-user
        - name: synthetic-gke
          context:
            cluster: synthetic-gke-cluster
            user: synthetic-gke-user
        - name: synthetic-oidc-exec
          context:
            cluster: synthetic-oidc-cluster
            user: synthetic-oidc-exec-user
        - name: synthetic-oidc-provider
          context:
            cluster: synthetic-oidc-cluster
            user: synthetic-oidc-provider-user
        users:
        - name: synthetic-eks-user
          user:
            exec:
              apiVersion: client.authentication.k8s.io/v1
              command: aws
              args: [eks, get-token, --cluster-name=synthetic-cluster, --region, eu-north-1]
              interactiveMode: Never
        - name: synthetic-azure-user
          user:
            exec:
              apiVersion: client.authentication.k8s.io/v1
              command: kubelogin
              args: [get-token, --tenant-id=11111111-1111-4111-8111-111111111111, --server-id, 22222222-2222-4222-8222-222222222222, --client-id=33333333-3333-4333-8333-333333333333, -l, spn]
              interactiveMode: Never
        - name: synthetic-gke-user
          user:
            exec:
              apiVersion: client.authentication.k8s.io/v1beta1
              command: gke-gcloud-auth-plugin
        - name: synthetic-oidc-exec-user
          user:
            exec:
              apiVersion: client.authentication.k8s.io/v1
              command: kubelogin
              args: [get-token, --oidc-issuer-url=https://issuer.example.invalid, --oidc-client-id, synthetic-public-client]
              interactiveMode: Never
        - name: synthetic-oidc-provider-user
          user:
            auth-provider:
              name: oidc
              config:
                idp-issuer-url: https://issuer.example.invalid
                client-id: synthetic-public-client
                id-token: synthetic-sensitive-id-token
                refresh-token: synthetic-sensitive-refresh-token
        """

        let analysis = try KubeConfigNativeAuthAnalyzer().analyze(raw: raw)
        let byName = Dictionary(uniqueKeysWithValues: analysis.contexts.map { ($0.contextName, $0) })

        XCTAssertEqual(analysis.currentContext, "synthetic-eks")
        XCTAssertEqual(byName["synthetic-eks"]?.provider, .awsEKS)
        XCTAssertEqual(byName["synthetic-azure"]?.provider, .azureKubelogin)
        XCTAssertEqual(byName["synthetic-gke"]?.provider, .googleGKE)
        XCTAssertNil(byName["synthetic-oidc-exec"]?.provider)
        XCTAssertEqual(byName["synthetic-oidc-provider"]?.provider, .oidc)
        XCTAssertEqual(
            byName["synthetic-eks"]?.exec?.optionValue(for: "--cluster-name"),
            "synthetic-cluster"
        )
        XCTAssertEqual(
            byName["synthetic-eks"]?.exec?.optionValue(for: "--region"),
            "eu-north-1"
        )
        XCTAssertEqual(
            byName["synthetic-azure"]?.exec?.optionValue(for: "--tenant-id"),
            "11111111-1111-4111-8111-111111111111"
        )
        XCTAssertEqual(
            byName["synthetic-azure"]?.exec?.optionValue(for: "--server-id"),
            "22222222-2222-4222-8222-222222222222"
        )
        XCTAssertEqual(
            byName["synthetic-oidc-provider"]?.authProvider?.displayValue(for: "id-token"),
            "<redacted>"
        )
        XCTAssertEqual(
            byName["synthetic-oidc-provider"]?.authProvider?.displayValue(for: "refresh-token"),
            "<redacted>"
        )
        XCTAssertNotNil(byName["synthetic-eks"]?.credentialRequest)
        XCTAssertTrue(analysis.issues.isEmpty)
    }

    func testAnalyzerResolvesRelativePathsAndReportsMissingReferencesWithoutSecrets() throws {
        let sourceURL = URL(fileURLWithPath: "/synthetic/input/config.yaml")
        let raw = """
        current-context: synthetic-context
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://cluster.example.invalid
            certificate-authority: materials/ca.pem
        contexts:
        - name: synthetic-context
          context:
            cluster: synthetic-cluster
            user: synthetic-user
        - name: synthetic-broken
          context:
            cluster: missing-cluster
            user: missing-user
        users:
        - name: synthetic-user
          user:
            exec:
              command: ./plugins/oidc-login
              args: [--issuer-url, https://issuer.example.invalid, --client-id=synthetic-client]
              env:
              - name: SYNTHETIC_REFRESH_TOKEN
                value: synthetic-sensitive-value
        """

        let analysis = try KubeConfigNativeAuthAnalyzer().analyze(raw: raw, sourceURL: sourceURL)
        let descriptor = try XCTUnwrap(analysis.contexts.first)

        XCTAssertEqual(descriptor.cluster.certificateAuthorityPath, "/synthetic/input/materials/ca.pem")
        XCTAssertEqual(descriptor.exec?.command, "/synthetic/input/plugins/oidc-login")
        XCTAssertEqual(descriptor.exec?.environment.first?.displayValue, "<redacted>")
        XCTAssertNil(descriptor.provider)
        XCTAssertEqual(analysis.issues.count, 1)
        XCTAssertEqual(analysis.issues.first?.contextName, "synthetic-broken")
        XCTAssertFalse(analysis.issues.first?.message.contains("synthetic-sensitive-value") == true)
    }

    func testAnalyzerMergesContextClusterAndUserAcrossSourcesUsingEntrySourcePaths() throws {
        let directory = FileManager.default.temporaryDirectory
            .appendingPathComponent("rune-native-merged-\(UUID().uuidString)", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let contextsURL = directory.appendingPathComponent("contexts.yaml")
        let identitiesDirectory = directory.appendingPathComponent("identities", isDirectory: true)
        try FileManager.default.createDirectory(at: identitiesDirectory, withIntermediateDirectories: true)
        let identitiesURL = identitiesDirectory.appendingPathComponent("identities.yaml")
        try """
        apiVersion: v1
        current-context: synthetic-merged
        contexts:
        - name: synthetic-merged
          context:
            cluster: synthetic-cluster
            user: synthetic-user
        """.write(to: contextsURL, atomically: true, encoding: .utf8)
        try """
        apiVersion: v1
        clusters:
        - name: synthetic-cluster
          cluster:
            server: https://cluster.example.invalid
            certificate-authority: materials/ca.pem
        users:
        - name: synthetic-user
          user:
            exec:
              command: ../bin/aws
              args: [eks, get-token, --cluster-name, synthetic-cluster, --region, eu-north-1]
        """.write(to: identitiesURL, atomically: true, encoding: .utf8)

        let analysis = try KubeConfigNativeAuthAnalyzer().analyze(sources: [
            KubeConfigSource(url: contextsURL),
            KubeConfigSource(url: identitiesURL)
        ])
        let descriptor = try XCTUnwrap(analysis.contexts.first)

        XCTAssertEqual(descriptor.provider, .awsEKS)
        XCTAssertEqual(descriptor.exec?.command, directory.appendingPathComponent("bin/aws").path)
        XCTAssertEqual(
            descriptor.cluster.certificateAuthorityPath,
            identitiesDirectory.appendingPathComponent("materials/ca.pem").path
        )
        XCTAssertNotNil(descriptor.credentialRequest)
        XCTAssertTrue(analysis.issues.isEmpty)
    }

    func testBindingFingerprintNormalizesFlagFormsAndIgnoresSecretValues() {
        let cluster = KubernetesNativeAuthClusterDescriptor(
            name: "synthetic-cluster",
            server: "HTTPS://CLUSTER.EXAMPLE.INVALID:443"
        )
        let separated = KubernetesNativeAuthExecDescriptor(
            command: "aws",
            arguments: [
                "eks", "get-token",
                "--cluster-name", "synthetic-cluster",
                "--region", "synthetic-region",
                "--client-secret", "synthetic-secret-one"
            ]
        )
        let equals = KubernetesNativeAuthExecDescriptor(
            command: "/synthetic/bin/aws",
            arguments: [
                "eks", "get-token",
                "--cluster-name=synthetic-cluster",
                "--region=synthetic-region",
                "--client-secret=synthetic-secret-two"
            ]
        )

        let first = KubernetesNativeAuthBindingFingerprint.make(
            contextName: "synthetic-context",
            cluster: cluster,
            userName: "synthetic-user",
            provider: .awsEKS,
            exec: separated,
            authProvider: nil
        )
        let second = KubernetesNativeAuthBindingFingerprint.make(
            contextName: "synthetic-context",
            cluster: cluster,
            userName: "synthetic-user",
            provider: .awsEKS,
            exec: equals,
            authProvider: nil
        )
        let otherRegion = KubernetesNativeAuthBindingFingerprint.make(
            contextName: "synthetic-context",
            cluster: cluster,
            userName: "synthetic-user",
            provider: .awsEKS,
            exec: KubernetesNativeAuthExecDescriptor(
                command: "aws",
                arguments: ["eks", "get-token", "--cluster-name=synthetic-cluster", "--region=other-region"]
            ),
            authProvider: nil
        )

        XCTAssertEqual(first, second)
        XCTAssertNotEqual(first, otherRegion)
        XCTAssertTrue(first.hasPrefix("native-k8s-v1:"))
        XCTAssertEqual(first.count, "native-k8s-v1:".count + 64)
        XCTAssertFalse(first.contains("synthetic-secret"))

        let environmentRegion = KubernetesNativeAuthBindingFingerprint.make(
            contextName: "synthetic-context",
            cluster: cluster,
            userName: "synthetic-user",
            provider: .awsEKS,
            exec: KubernetesNativeAuthExecDescriptor(
                command: "aws",
                arguments: ["eks", "get-token", "--cluster-name=synthetic-cluster"],
                environment: [KubernetesNativeAuthEnvironmentEntry(name: "AWS_REGION", value: "other-region")]
            ),
            authProvider: nil
        )
        XCTAssertNotEqual(first, environmentRegion)
    }

    func testClassifierOnlyAdvertisesExecShapesImplementedByDefaultProvider() throws {
        let unsupported: [KubernetesNativeAuthExecDescriptor] = [
            KubernetesNativeAuthExecDescriptor(command: "aws-iam-authenticator", arguments: ["token", "-i", "synthetic"]),
            KubernetesNativeAuthExecDescriptor(command: "az", arguments: ["aks", "get-credentials"]),
            KubernetesNativeAuthExecDescriptor(command: "kubectl", arguments: ["oidc-login", "get-token"]),
            KubernetesNativeAuthExecDescriptor(command: "oidc-login", arguments: ["get-token"]),
            KubernetesNativeAuthExecDescriptor(command: "kubelogin", arguments: ["get-token", "-l", "devicecode"])
        ]
        for exec in unsupported {
            XCTAssertNil(KubernetesNativeAuthProviderClassifier.classify(
                exec: exec,
                authProvider: nil,
                clusterServer: "https://cluster.example.invalid"
            ), "Unexpected native classification for \(exec.executableName)")
        }

        let supportedAWS = KubernetesNativeAuthExecDescriptor(
            command: "aws",
            arguments: ["eks", "get-token", "--cluster-name", "synthetic", "--region", "eu-north-1"]
        )
        XCTAssertEqual(KubernetesNativeAuthProviderClassifier.classify(
            exec: supportedAWS,
            authProvider: nil,
            clusterServer: "https://cluster.example.invalid"
        ), .awsEKS)
    }

    func testKeychainBackedProfileStoreKeepsSecretsSeparateAndReplacesBindingAtomically() async throws {
        let secretStore = LockedInMemorySecretStore()
        let store = KeychainKubernetesNativeAuthProfileStore(
            secretStore: secretStore,
            keyPrefix: "synthetic.native-auth"
        )
        let firstID = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!
        let secondID = UUID(uuidString: "00000000-0000-0000-0000-000000000002")!
        let first = KubernetesNativeAuthProfile(
            id: firstID,
            bindingID: "native-k8s-v1:synthetic-binding",
            provider: .oidc,
            displayName: "Synthetic profile"
        )
        let second = KubernetesNativeAuthProfile(
            id: secondID,
            bindingID: first.bindingID,
            provider: .oidc,
            displayName: "Replacement profile"
        )
        let firstSecret = Data("synthetic-refresh-material-one".utf8)
        let secondSecret = Data("synthetic-refresh-material-two".utf8)

        try await store.save(profile: first, secret: firstSecret)
        let firstStored = try await store.storedProfile(for: first.bindingID)
        XCTAssertEqual(firstStored?.secret, firstSecret)
        let index = try XCTUnwrap(secretStore.data(for: "synthetic.native-auth.profiles"))
        XCTAssertFalse(String(decoding: index, as: UTF8.self).contains("synthetic-refresh-material-one"))

        try await store.save(profile: second, secret: secondSecret)
        let replacement = try await store.storedProfile(for: first.bindingID)
        let stored = try XCTUnwrap(replacement)
        XCTAssertEqual(stored.profile.id, secondID)
        XCTAssertEqual(stored.secret, secondSecret)
        XCTAssertNil(secretStore.data(for: "synthetic.native-auth.secret.\(firstID.uuidString.lowercased())"))
        let profiles = try await store.profiles()
        XCTAssertEqual(profiles.count, 1)

        try await store.removeProfile(for: first.bindingID)
        let removedProfile = try await store.storedProfile(for: first.bindingID)
        XCTAssertNil(removedProfile)
        XCTAssertNil(secretStore.data(for: "synthetic.native-auth.secret.\(secondID.uuidString.lowercased())"))
    }

    func testProfileStoreRejectsCorruptedIndex() async throws {
        let secretStore = LockedInMemorySecretStore()
        try secretStore.set(Data("not-json".utf8), for: "synthetic.native-auth.profiles")
        let store = KeychainKubernetesNativeAuthProfileStore(
            secretStore: secretStore,
            keyPrefix: "synthetic.native-auth"
        )

        do {
            _ = try await store.profiles()
            XCTFail("Expected a corrupted index error")
        } catch let error as KubernetesNativeAuthProfileStoreError {
            XCTAssertEqual(error, .corruptedIndex)
        }
    }
}

private final class LockedInMemorySecretStore: SecretStore, @unchecked Sendable {
    private let lock = NSLock()
    private var values: [String: Data] = [:]

    func set(_ value: Data, for key: String) throws {
        lock.withLock { values[key] = value }
    }

    func get(for key: String) throws -> Data? {
        lock.withLock { values[key] }
    }

    func delete(for key: String) throws {
        _ = lock.withLock { values.removeValue(forKey: key) }
    }

    func data(for key: String) -> Data? {
        lock.withLock { values[key] }
    }
}
