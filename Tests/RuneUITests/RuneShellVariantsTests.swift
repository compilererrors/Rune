import XCTest
@testable import RuneUI

final class RuneShellVariantsTests: XCTestCase {
    private var originalInlineEditorEnv: String?

    override func setUp() {
        super.setUp()
        originalInlineEditorEnv = ProcessInfo.processInfo.environment["RUNE_MANIFEST_INLINE_EDITOR"]
    }

    override func tearDown() {
        if let originalInlineEditorEnv {
            setenv("RUNE_MANIFEST_INLINE_EDITOR", originalInlineEditorEnv, 1)
        } else {
            unsetenv("RUNE_MANIFEST_INLINE_EDITOR")
        }
        super.tearDown()
    }

    func testManifestInlineEditorDefaultsToAppKitTextView() {
        unsetenv("RUNE_MANIFEST_INLINE_EDITOR")

        XCTAssertEqual(
            ManifestInlineEditorImplementation.resolved(override: nil),
            .appKitTextView
        )
    }

    func testManifestInlineEditorFallsBackToAppKitTextViewForUnknownEnvValue() {
        setenv("RUNE_MANIFEST_INLINE_EDITOR", "unknown-editor", 1)

        XCTAssertEqual(
            ManifestInlineEditorImplementation.resolved(override: nil),
            .appKitTextView
        )
    }

    func testManifestInlineEditorUsesEnvOverrideWhenRecognized() {
        setenv("RUNE_MANIFEST_INLINE_EDITOR", ManifestInlineEditorImplementation.swiftUITextEditor.rawValue, 1)

        XCTAssertEqual(
            ManifestInlineEditorImplementation.resolved(override: nil),
            .swiftUITextEditor
        )
    }
}
