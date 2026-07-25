import RuneCore
import RuneKube
import XCTest

final class EventIdentityTests: XCTestCase {
    func testParsedEventsUseStableMetadataUIDInsteadOfMutableDisplayFields() throws {
        let parser = KubernetesOutputParser()
        let initial = try XCTUnwrap(parser.parseEvents(from: eventListJSON(
            uid: "event-uid-1",
            name: "event-name-1",
            timestamp: "2026-01-01T00:00:00Z",
            message: "Initial message"
        )).first)
        let updated = try XCTUnwrap(parser.parseEvents(from: eventListJSON(
            uid: "event-uid-1",
            name: "event-name-1",
            timestamp: "2026-01-01T00:01:00Z",
            message: "Updated message"
        )).first)

        XCTAssertEqual(initial.eventIdentifier, "event-uid-1")
        XCTAssertEqual(initial.id, updated.id)
    }

    func testDistinctMetadataUIDsKeepVisuallyIdenticalEventsDistinct() throws {
        let raw = #"""
        {
          "items": [
            {
              "metadata": {
                "name": "event-name-1",
                "namespace": "sample-namespace",
                "uid": "event-uid-1"
              },
              "type": "Warning",
              "reason": "SyntheticReason",
              "message": "Synthetic message",
              "lastTimestamp": "2026-01-01T00:00:00Z",
              "involvedObject": {
                "kind": "Pod",
                "name": "sample-pod",
                "namespace": "sample-namespace"
              }
            },
            {
              "metadata": {
                "name": "event-name-2",
                "namespace": "sample-namespace",
                "uid": "event-uid-2"
              },
              "type": "Warning",
              "reason": "SyntheticReason",
              "message": "Synthetic message",
              "lastTimestamp": "2026-01-01T00:00:00Z",
              "involvedObject": {
                "kind": "Pod",
                "name": "sample-pod",
                "namespace": "sample-namespace"
              }
            }
          ]
        }
        """#

        let events = try KubernetesOutputParser().parseEvents(from: raw)

        XCTAssertEqual(events.count, 2)
        XCTAssertEqual(Set(events.map(\.id)).count, 2)
    }

    func testParsedEventFallsBackToMetadataNameWhenUIDIsMissing() throws {
        let raw = #"""
        {
          "items": [
            {
              "metadata": {
                "name": "event-name-only",
                "namespace": "sample-namespace"
              },
              "type": "Normal",
              "reason": "SyntheticReason",
              "message": "Synthetic message",
              "involvedObject": {
                "kind": "Pod",
                "name": "sample-pod",
                "namespace": "sample-namespace"
              }
            }
          ]
        }
        """#

        let event = try XCTUnwrap(KubernetesOutputParser().parseEvents(from: raw).first)

        XCTAssertEqual(event.eventIdentifier, "event-name-only")
        XCTAssertEqual(event.eventNamespace, "sample-namespace")
        XCTAssertTrue(event.id.hasPrefix("event|"))
    }

    func testMetadataNameFallbackIsScopedByTheEventNamespace() throws {
        let raw = #"""
        {
          "items": [
            {
              "metadata": {
                "name": "shared-event-name",
                "namespace": "scope-a"
              },
              "type": "Normal",
              "reason": "SyntheticReason",
              "message": "Synthetic message",
              "involvedObject": {
                "kind": "Node",
                "name": "sample-node"
              }
            },
            {
              "metadata": {
                "name": "shared-event-name",
                "namespace": "scope-b"
              },
              "type": "Normal",
              "reason": "SyntheticReason",
              "message": "Synthetic message",
              "involvedObject": {
                "kind": "Node",
                "name": "sample-node"
              }
            }
          ]
        }
        """#

        let events = try KubernetesOutputParser().parseEvents(from: raw)

        XCTAssertEqual(events.count, 2)
        XCTAssertEqual(Set(events.map(\.id)).count, 2)
    }

    func testSyntheticEventFallbackIdentityIsDeterministic() {
        let first = EventSummary(
            type: "Normal",
            reason: "SyntheticReason",
            objectName: "sample-pod",
            message: "Synthetic message",
            lastTimestamp: "2026-01-01T00:00:00Z",
            involvedKind: "Pod",
            involvedNamespace: "sample-namespace"
        )
        let second = EventSummary(
            type: "Normal",
            reason: "SyntheticReason",
            objectName: "sample-pod",
            message: "Synthetic message",
            lastTimestamp: "2026-01-01T00:00:00Z",
            involvedKind: "Pod",
            involvedNamespace: "sample-namespace"
        )

        XCTAssertEqual(first.id, second.id)
        XCTAssertTrue(first.id.hasPrefix("event-fallback|"))
    }

    private func eventListJSON(
        uid: String,
        name: String,
        timestamp: String,
        message: String
    ) -> String {
        #"""
        {
          "items": [
            {
              "metadata": {
                "name": "\#(name)",
                "namespace": "sample-namespace",
                "uid": "\#(uid)"
              },
              "type": "Warning",
              "reason": "SyntheticReason",
              "message": "\#(message)",
              "lastTimestamp": "\#(timestamp)",
              "involvedObject": {
                "kind": "Pod",
                "name": "sample-pod",
                "namespace": "sample-namespace"
              }
            }
          ]
        }
        """#
    }
}
