import Foundation

protocol RunningCommandControlling: Sendable {
    var id: UUID { get }
    func terminate()
    func writeToStdin(_ data: Data) throws
    func resizeTerminal(columns: Int, rows: Int) throws
}

extension RunningCommandControlling {
    func resizeTerminal(columns _: Int, rows _: Int) throws {}
}
