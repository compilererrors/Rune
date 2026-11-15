import Foundation

extension Array where Element: Identifiable {
    /// Moves the element with `id` into the slot of the element with
    /// `targetID`, as dropping a tab onto another tab does: moving right
    /// lands after the target, moving left lands before it.
    @discardableResult
    public mutating func moveElement(id: Element.ID, onto targetID: Element.ID) -> Bool {
        guard id != targetID,
              let from = firstIndex(where: { $0.id == id }),
              let to = firstIndex(where: { $0.id == targetID }) else {
            return false
        }
        let element = remove(at: from)
        insert(element, at: to)
        return true
    }
}
