import Foundation

actor RuneSingleFlightLatestPendingWorkLane<Output: Sendable> {
    private struct Request {
        let generation: UInt64
        let priority: TaskPriority
        let operation: @Sendable () throws -> Output
        let continuation: CheckedContinuation<Output?, Never>
    }

    private var generation: UInt64 = 0
    private var pendingRequest: Request?
    private var drainTask: Task<Void, Never>?
    private var currentWorkTask: Task<Output, Error>?

    func submit(
        priority: TaskPriority,
        operation: @escaping @Sendable () throws -> Output
    ) async -> Output? {
        await withCheckedContinuation { continuation in
            if let pendingRequest {
                pendingRequest.continuation.resume(returning: nil)
            }
            pendingRequest = Request(
                generation: generation,
                priority: priority,
                operation: operation,
                continuation: continuation
            )
            startDrainIfNeeded()
        }
    }

    func cancel() {
        generation &+= 1
        if let pendingRequest {
            pendingRequest.continuation.resume(returning: nil)
            self.pendingRequest = nil
        }
        currentWorkTask?.cancel()
        drainTask?.cancel()
    }

    private func startDrainIfNeeded() {
        guard drainTask == nil else { return }
        drainTask = Task {
            await drain()
        }
    }

    private func drain() async {
        while !Task.isCancelled, let request = pendingRequest {
            pendingRequest = nil

            let workTask = Task.detached(priority: request.priority) {
                try Task.checkCancellation()
                let output = try request.operation()
                try Task.checkCancellation()
                return output
            }
            currentWorkTask = workTask
            let result = await workTask.result
            currentWorkTask = nil

            guard request.generation == generation, !Task.isCancelled else {
                request.continuation.resume(returning: nil)
                continue
            }
            switch result {
            case let .success(output):
                request.continuation.resume(returning: output)
            case .failure:
                request.continuation.resume(returning: nil)
            }
        }

        drainTask = nil
        if pendingRequest != nil {
            startDrainIfNeeded()
        }
    }
}
