import CryptoKit
import Foundation
@testable import RuneSecurity
import XCTest

final class AWSEKSNativeAuthTests: XCTestCase {
    private let accessKeyID = "AKIASYNTHETIC000001"
    private let secretAccessKey = "syntheticSecretKeyForOfflineGoldenTestOnly0001"

    func testParserRecognizesGeneratedAWSExecShapeAndInlineFlags() throws {
        let descriptor = try XCTUnwrap(AWSEKSExecDescriptor.parseIfSupported(
            command: "/opt/synthetic/bin/aws",
            arguments: [
                "--region=eu-north-1",
                "--profile", "synthetic-profile",
                "eks", "get-token",
                "--cluster-name=synthetic-cluster",
                "--output", "json",
                "--no-cli-pager"
            ]
        ))

        XCTAssertEqual(descriptor.region, "eu-north-1")
        XCTAssertEqual(descriptor.clusterIdentifier.kind, .name)
        XCTAssertEqual(descriptor.clusterIdentifier.value, "synthetic-cluster")
        XCTAssertEqual(descriptor.profileHint, "synthetic-profile")
    }

    func testParserUsesExecEnvironmentAndSupportsOutpostsClusterID() throws {
        let descriptor = try XCTUnwrap(AWSEKSExecDescriptor.parseIfSupported(
            command: "aws",
            arguments: ["eks", "get-token", "--cluster-id", "synthetic-cluster-id"],
            environment: [
                "AWS_REGION": "us-gov-west-1",
                "AWS_DEFAULT_REGION": "eu-west-1",
                "AWS_PROFILE": "synthetic-environment-profile"
            ]
        ))

        XCTAssertEqual(descriptor.region, "us-gov-west-1")
        XCTAssertEqual(descriptor.clusterIdentifier.kind, .id)
        XCTAssertEqual(descriptor.clusterIdentifier.value, "synthetic-cluster-id")
        XCTAssertEqual(descriptor.profileHint, "synthetic-environment-profile")
    }

    func testCommandOptionsOverrideExecEnvironment() throws {
        let descriptor = try XCTUnwrap(AWSEKSExecDescriptor.parseIfSupported(
            command: "aws.exe",
            arguments: [
                "eks", "get-token",
                "--cluster-name", "synthetic-cluster",
                "--region", "ap-southeast-2",
                "--profile=synthetic-argument-profile"
            ],
            environment: [
                "AWS_REGION": "eu-west-1",
                "AWS_PROFILE": "synthetic-environment-profile"
            ]
        ))

        XCTAssertEqual(descriptor.region, "ap-southeast-2")
        XCTAssertEqual(descriptor.profileHint, "synthetic-argument-profile")
    }

    func testParserReturnsNilForOtherExecutablesAndAWSOperations() throws {
        XCTAssertNil(try AWSEKSExecDescriptor.parseIfSupported(
            command: "kubelogin",
            arguments: ["eks", "get-token"]
        ))
        XCTAssertNil(try AWSEKSExecDescriptor.parseIfSupported(
            command: "aws",
            arguments: ["s3", "ls", "--region"]
        ))
    }

    func testParserFailsClosedForRoleAndCustomEndpoint() {
        assertParseError(
            arguments: [
                "eks", "get-token",
                "--cluster-name", "synthetic-cluster",
                "--region", "eu-north-1",
                "--role-arn", "arn:aws:iam::000000000000:role/SyntheticRole"
            ],
            expected: .unsupportedRoleAssumption
        )
        assertParseError(
            arguments: [
                "eks", "get-token",
                "--cluster-name", "synthetic-cluster",
                "--region", "eu-north-1",
                "--endpoint-url=https://synthetic.invalid"
            ],
            expected: .customEndpointUnsupported
        )
        assertParseError(
            arguments: [
                "eks", "get-token",
                "--cluster-name", "synthetic-cluster",
                "--region", "eu-north-1",
                "--future-auth-mode=synthetic-value"
            ],
            expected: .unsupportedOption("--future-auth-mode")
        )
    }

    func testParserValidatesRequiredAndMutuallyExclusiveInputs() {
        assertParseError(
            arguments: ["eks", "get-token", "--cluster-name", "synthetic-cluster"],
            expected: .missingRegion
        )
        assertParseError(
            arguments: ["eks", "get-token", "--region", "eu-north-1"],
            expected: .missingClusterIdentifier
        )
        assertParseError(
            arguments: [
                "eks", "get-token",
                "--region", "eu-north-1",
                "--cluster-name", "synthetic-cluster",
                "--cluster-id", "synthetic-cluster-id"
            ],
            expected: .conflictingClusterIdentifiers
        )
        assertParseError(
            arguments: [
                "eks", "get-token",
                "--region", "eu-north-1",
                "--cluster-name", "synthetic-cluster",
                "--region", "eu-west-1"
            ],
            expected: .duplicateOption("--region")
        )
        assertParseError(
            arguments: ["eks", "get-token", "--cluster-name", "--region", "eu-north-1"],
            expected: .missingOptionValue("--cluster-name")
        )
    }

    func testCredentialsValidateAndNeverDescribeSecrets() throws {
        let credentials = try AWSEKSCredentials(
            accessKeyID: accessKeyID,
            secretAccessKey: secretAccessKey,
            sessionToken: "synthetic-session-token"
        )

        XCTAssertEqual(credentials.description, "AWSEKSCredentials(<redacted>)")
        XCTAssertEqual(credentials.debugDescription, "AWSEKSCredentials(<redacted>)")
        XCTAssertFalse(credentials.description.contains(accessKeyID))
        XCTAssertFalse(credentials.description.contains(secretAccessKey))
        XCTAssertThrowsError(try AWSEKSCredentials(accessKeyID: "", secretAccessKey: secretAccessKey)) {
            XCTAssertEqual($0 as? AWSEKSNativeAuthError, .invalidCredentials)
        }
        XCTAssertThrowsError(try AWSEKSCredentials(accessKeyID: accessKeyID, secretAccessKey: "line\nbreak")) {
            XCTAssertEqual($0 as? AWSEKSNativeAuthError, .invalidCredentials)
        }
    }

    func testSigV4GoldenSignatureMatchesIndependentFixture() throws {
        let descriptor = try descriptor(region: "eu-north-1")
        let credentials = try AWSEKSCredentials(
            accessKeyID: accessKeyID,
            secretAccessKey: secretAccessKey
        )
        let date = try fixedDate()
        let artifacts = try AWSEKSTokenSigner().signingArtifacts(
            descriptor: descriptor,
            credentials: credentials,
            signingDate: date
        )

        XCTAssertEqual(
            artifacts.signature,
            "07fffa58681a704f14a4e35e726d45c116bb0773fc5a756ff1b34ff7756140b0"
        )
        XCTAssertEqual(
            artifacts.stringToSign,
            """
            AWS4-HMAC-SHA256
            20250102T030405Z
            20250102/eu-north-1/sts/aws4_request
            5d055d79a381702891bce657a040e1cee4cb4330752881ce6a119deb58f2b4e9
            """
        )
        XCTAssertEqual(artifacts.expiration, date.addingTimeInterval(14 * 60))
    }

    func testGoldenTokenDecodesToExpectedPresignedURLAndHeaderBinding() throws {
        let artifacts = try AWSEKSTokenSigner().signingArtifacts(
            descriptor: descriptor(region: "eu-north-1"),
            credentials: AWSEKSCredentials(
                accessKeyID: accessKeyID,
                secretAccessKey: secretAccessKey
            ),
            signingDate: fixedDate()
        )

        XCTAssertTrue(artifacts.token.hasPrefix("k8s-aws-v1."))
        XCTAssertFalse(artifacts.token.contains("="))
        XCTAssertEqual(try decodedTokenURL(artifacts.token), artifacts.presignedURL)
        let components = try XCTUnwrap(URLComponents(string: artifacts.presignedURL))
        let query = Dictionary(uniqueKeysWithValues: try XCTUnwrap(components.queryItems).map { item in
            (item.name, item.value ?? "")
        })
        XCTAssertEqual(components.host, "sts.eu-north-1.amazonaws.com")
        XCTAssertEqual(query["Action"], "GetCallerIdentity")
        XCTAssertEqual(query["Version"], "2011-06-15")
        XCTAssertEqual(query["X-Amz-Expires"], "60")
        XCTAssertEqual(query["X-Amz-SignedHeaders"], "host;x-k8s-aws-id")
        XCTAssertEqual(query["X-Amz-Signature"], artifacts.signature)
        XCTAssertTrue(artifacts.canonicalRequest.contains("x-k8s-aws-id:synthetic-cluster\n"))
    }

    func testSessionTokenIsSignedAndPercentEncoded() throws {
        let sessionToken = "synthetic/token+with=characters"
        let artifacts = try AWSEKSTokenSigner().signingArtifacts(
            descriptor: descriptor(region: "ap-southeast-2"),
            credentials: AWSEKSCredentials(
                accessKeyID: accessKeyID,
                secretAccessKey: secretAccessKey,
                sessionToken: sessionToken
            ),
            signingDate: fixedDate()
        )

        XCTAssertTrue(artifacts.canonicalRequest.contains(
            "X-Amz-Security-Token=synthetic%2Ftoken%2Bwith%3Dcharacters"
        ))
        let components = try XCTUnwrap(URLComponents(string: artifacts.presignedURL))
        XCTAssertEqual(
            components.queryItems?.first(where: { $0.name == "X-Amz-Security-Token" })?.value,
            sessionToken
        )
    }

    func testRegionalEndpointsCoverCommercialGovCloudAndChina() throws {
        let cases = [
            ("us-west-2", "sts.us-west-2.amazonaws.com"),
            ("us-gov-west-1", "sts.us-gov-west-1.amazonaws.com"),
            ("cn-north-1", "sts.cn-north-1.amazonaws.com.cn")
        ]
        let credentials = try AWSEKSCredentials(
            accessKeyID: accessKeyID,
            secretAccessKey: secretAccessKey
        )
        for (region, expectedHost) in cases {
            let token = try AWSEKSTokenSigner().signingArtifacts(
                descriptor: descriptor(region: region),
                credentials: credentials,
                signingDate: fixedDate()
            )
            XCTAssertEqual(URL(string: token.presignedURL)?.host, expectedHost)
        }
    }

    func testUnknownSovereignPartitionFailsClosedWithoutCredentialDisclosure() throws {
        let credentials = try AWSEKSCredentials(
            accessKeyID: accessKeyID,
            secretAccessKey: secretAccessKey
        )
        XCTAssertThrowsError(try AWSEKSTokenSigner().token(
            for: descriptor(region: "us-iso-east-1"),
            credentials: credentials,
            at: fixedDate()
        )) { error in
            XCTAssertEqual(error as? AWSEKSNativeAuthError, .unsupportedPartition)
            XCTAssertFalse(error.localizedDescription.contains(accessKeyID))
            XCTAssertFalse(error.localizedDescription.contains(secretAccessKey))
        }
    }

    func testCredentialExpirationCapsTokenAndRejectsExpiredSession() throws {
        let date = try fixedDate()
        let shortExpiration = date.addingTimeInterval(5 * 60)
        let shortCredentials = try AWSEKSCredentials(
            accessKeyID: accessKeyID,
            secretAccessKey: secretAccessKey,
            sessionToken: "synthetic-session-token",
            expiration: shortExpiration
        )
        let token = try AWSEKSTokenSigner().token(
            for: descriptor(region: "eu-north-1"),
            credentials: shortCredentials,
            at: date
        )
        XCTAssertEqual(token.expiration, shortExpiration.addingTimeInterval(-30))

        let expired = try AWSEKSCredentials(
            accessKeyID: accessKeyID,
            secretAccessKey: secretAccessKey,
            sessionToken: "synthetic-session-token",
            expiration: date
        )
        XCTAssertThrowsError(try AWSEKSTokenSigner().token(
            for: descriptor(region: "eu-north-1"),
            credentials: expired,
            at: date
        )) {
            XCTAssertEqual($0 as? AWSEKSNativeAuthError, .expiredCredentials)
        }
    }

    func testOversizedGeneratedTokenFailsClosed() throws {
        let credentials = try AWSEKSCredentials(
            accessKeyID: accessKeyID,
            secretAccessKey: secretAccessKey,
            sessionToken: String(repeating: "s", count: 8_000)
        )
        XCTAssertThrowsError(try AWSEKSTokenSigner().token(
            for: descriptor(region: "eu-north-1"),
            credentials: credentials,
            at: fixedDate()
        )) {
            XCTAssertEqual($0 as? AWSEKSNativeAuthError, .tokenTooLarge)
        }
    }

    func testKPISignsOneThousandTokensWithinFiveSeconds() throws {
        let descriptor = try descriptor(region: "eu-north-1")
        let credentials = try AWSEKSCredentials(
            accessKeyID: accessKeyID,
            secretAccessKey: secretAccessKey,
            sessionToken: "synthetic-session-token"
        )
        let signer = AWSEKSTokenSigner()
        let date = try fixedDate()
        let start = ContinuousClock.now
        var totalBytes = 0

        for index in 0..<1_000 {
            let token = try signer.token(
                for: descriptor,
                credentials: credentials,
                at: date.addingTimeInterval(TimeInterval(index % 60))
            )
            totalBytes += token.value.utf8.count
        }

        let elapsed = start.duration(to: .now)
        XCTAssertGreaterThan(totalBytes, 0)
        XCTAssertLessThan(elapsed, .seconds(5), "AWS EKS SigV4 KPI exceeded 5 seconds for 1,000 tokens")
    }

    private func descriptor(region: String) throws -> AWSEKSExecDescriptor {
        try AWSEKSExecDescriptor(
            region: region,
            clusterIdentifier: AWSEKSClusterIdentifier(kind: .name, value: "synthetic-cluster")
        )
    }

    private func fixedDate() throws -> Date {
        let components = DateComponents(
            calendar: Calendar(identifier: .gregorian),
            timeZone: TimeZone(secondsFromGMT: 0),
            year: 2025,
            month: 1,
            day: 2,
            hour: 3,
            minute: 4,
            second: 5
        )
        return try XCTUnwrap(components.date)
    }

    private func decodedTokenURL(_ token: String) throws -> String {
        let prefix = "k8s-aws-v1."
        XCTAssertTrue(token.hasPrefix(prefix))
        var encoded = String(token.dropFirst(prefix.count))
            .replacingOccurrences(of: "-", with: "+")
            .replacingOccurrences(of: "_", with: "/")
        encoded.append(String(repeating: "=", count: (4 - encoded.count % 4) % 4))
        let data = try XCTUnwrap(Data(base64Encoded: encoded))
        return try XCTUnwrap(String(data: data, encoding: .utf8))
    }

    private func assertParseError(arguments: [String], expected: AWSEKSNativeAuthError) {
        XCTAssertThrowsError(try AWSEKSExecDescriptor.parseIfSupported(
            command: "aws",
            arguments: arguments
        )) { error in
            XCTAssertEqual(error as? AWSEKSNativeAuthError, expected)
        }
    }
}
