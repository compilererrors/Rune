import Foundation

public struct AuthDoctorRBACPreflightResult: Sendable {
    public let target: AuthDoctorRBACPreflightTarget
    public let namespace: String?
    public let allowed: Bool?
    public let errorMessage: String?

    public init(
        target: AuthDoctorRBACPreflightTarget,
        namespace: String?,
        allowed: Bool?,
        errorMessage: String?
    ) {
        self.target = target
        self.namespace = namespace
        self.allowed = allowed
        self.errorMessage = errorMessage
    }
}

public enum AuthDoctorRBACPreflightRunner {
    public static func run(
        targets: [AuthDoctorRBACPreflightTarget],
        activeNamespace: String,
        maxConcurrentChecks: Int = 4,
        check: @Sendable @escaping (_ target: AuthDoctorRBACPreflightTarget, _ namespace: String?) async throws -> Bool
    ) async -> [AuthDoctorRBACPreflightResult] {
        guard !targets.isEmpty else { return [] }

        let limit = max(1, min(maxConcurrentChecks, targets.count))
        var nextIndex = 0
        var results = Array<AuthDoctorRBACPreflightResult?>(repeating: nil, count: targets.count)

        await withTaskGroup(of: (Int, AuthDoctorRBACPreflightResult)?.self) { group in
            func enqueueNext() {
                guard !Task.isCancelled, nextIndex < targets.count else { return }
                let index = nextIndex
                nextIndex += 1
                let target = targets[index]
                let namespace = target.namespace(activeNamespace: activeNamespace)
                group.addTask {
                    do {
                        try Task.checkCancellation()
                        let allowed = try await check(target, namespace)
                        try Task.checkCancellation()
                        return (index, AuthDoctorRBACPreflightResult(
                            target: target,
                            namespace: namespace,
                            allowed: allowed,
                            errorMessage: nil
                        ))
                    } catch is CancellationError {
                        return nil
                    } catch {
                        return (index, AuthDoctorRBACPreflightResult(
                            target: target,
                            namespace: namespace,
                            allowed: nil,
                            errorMessage: error.localizedDescription
                        ))
                    }
                }
            }

            for _ in 0..<limit {
                enqueueNext()
            }

            while let completed = await group.next() {
                guard !Task.isCancelled else {
                    group.cancelAll()
                    break
                }
                guard let (index, result) = completed else {
                    enqueueNext()
                    continue
                }
                results[index] = result
                enqueueNext()
            }
        }

        return results.compactMap { $0 }
    }
}
