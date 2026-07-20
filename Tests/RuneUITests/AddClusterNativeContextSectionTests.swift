import AppKit
import SwiftUI
import XCTest
@testable import RuneSecurity
@testable import RuneUI

@MainActor
final class AddClusterNativeContextSectionTests: XCTestCase {
    func testContextSectionFitsProviderSheetWithConnectedAndUnselectedStates() {
        let options = [
            option(contextName: "synthetic-development-context"),
            option(contextName: "synthetic-production-context-with-a-long-display-name")
        ]

        for selectedID in [String?.none, Optional(options[1].id)] {
            let view = AddClusterNativeContextSection(
                options: options,
                selectedBindingID: .constant(selectedID),
                connectedBindingIDs: [options[1].id],
                isCheckingProfiles: false,
                analysisMessage: nil
            )
            .frame(width: 528)
            let host = NSHostingView(rootView: view)
            host.frame = NSRect(x: 0, y: 0, width: 528, height: 180)
            host.layoutSubtreeIfNeeded()

            XCTAssertLessThanOrEqual(host.fittingSize.width, 528.5)
            XCTAssertGreaterThan(host.fittingSize.height, 28)
            XCTAssertLessThan(host.fittingSize.height, 180)
        }
    }

    func testContextSectionEmptyAndCheckingStatesRemainCompact() {
        for isChecking in [false, true] {
            let view = AddClusterNativeContextSection(
                options: [],
                selectedBindingID: .constant(nil),
                connectedBindingIDs: [],
                isCheckingProfiles: isChecking,
                analysisMessage: "No compatible synthetic context was found."
            )
            .frame(width: 528)
            let host = NSHostingView(rootView: view)
            host.frame = NSRect(x: 0, y: 0, width: 528, height: 160)
            host.layoutSubtreeIfNeeded()

            XCTAssertLessThanOrEqual(host.fittingSize.width, 528.5)
            XCTAssertGreaterThan(host.fittingSize.height, 28)
            XCTAssertLessThan(host.fittingSize.height, 160)
        }
    }

    private func option(contextName: String) -> AddClusterNativeContextOption {
        AddClusterNativeContextOption(request: KubernetesNativeCredentialRequest(
            bindingID: "binding-\(contextName)",
            provider: .awsEKS,
            contextName: contextName,
            clusterName: "synthetic-cluster",
            userName: "synthetic-user",
            server: "https://cluster.example.invalid",
            exec: KubernetesNativeAuthExecDescriptor(
                command: "aws",
                arguments: ["eks", "get-token", "--cluster-name", "synthetic-cluster"]
            ),
            authProvider: nil
        ))
    }
}
