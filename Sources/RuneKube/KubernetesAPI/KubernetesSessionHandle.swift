import Foundation

protocol RunningCommandControlling: Sendable {
    var id: UUID { get }
    func terminate()
    func writeToStdin(_ data: Data) throws
}
