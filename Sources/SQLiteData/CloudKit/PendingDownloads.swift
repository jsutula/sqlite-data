#if canImport(CloudKit)
  public import Foundation
  import CloudKit
  import ConcurrencyExtras
  import Dependencies
  import StructuredQueries
  import os

  /// The state of records received from iCloud that could not yet be applied locally.
  public struct PendingDownloadStatus: Equatable, Sendable {
    public enum State: Equatable, Sendable {
      case idle, recovering, waitingForChanges, waitingToRetry, blocked, stopped
    }

    public var pendingCount: Int
    public var state: State
    public var lastProgress: Date?
    public var nextRetry: Date?
    public var lastFailure: String?

    public init(
      pendingCount: Int = 0,
      state: State = .idle,
      lastProgress: Date? = nil,
      nextRetry: Date? = nil,
      lastFailure: String? = nil
    ) {
      self.pendingCount = pendingCount
      self.state = state
      self.lastProgress = lastProgress
      self.nextRetry = nextRetry
      self.lastFailure = lastFailure
    }
  }

  @available(iOS 17, macOS 14, tvOS 17, watchOS 10, *)
  extension SyncEngine {
    /// An empty deferred queue does not imply that the server's change stream is caught up.
    public var downloadStatus: PendingDownloadStatus {
      observationRegistrar.access(self, keyPath: \.downloadStatus)
      return pendingDownloadStatus.withValue { $0 }
    }

    /// Joins the current recovery, including its server-imposed backoff. Throws if records
    /// remain blocked. Cancellation of one caller does not cancel other callers' recovery.
    public func retryPendingDownloads() async throws {
      await startTaskForPendingDownloads()
      guard isRunning else { throw PendingDownloadError.stopped }
      try await pendingDownloadRecovery.run { [weak self] generation in
        guard let self else { throw CancellationError() }
        try await self.recoverPendingDownloads(generation: generation)
      }
    }

    func updatePendingDownloadStatus(_ update: @Sendable (inout PendingDownloadStatus) -> Void) {
      observationRegistrar.withMutation(of: self, keyPath: \.downloadStatus) {
        pendingDownloadStatus.withValue(update)
      }
    }

    func schedulePendingDownloadRecovery() {
      Task { [weak self] in
        do {
          try await self?.retryPendingDownloads()
        } catch {
          // The worker publishes its failure. A later delta, launch or explicit retry can
          // retry blocked records without requiring any new server changes.
        }
      }
    }

    func refreshPendingDownloadCount() async {
      let generation = pendingDownloadRecovery.generation
      do {
        let count = try await metadatabase.read { try UnsyncedRecordID.count().fetchOne($0) ?? 0 }
        try pendingDownloadRecovery.withGeneration(generation) {
          updatePendingDownloadStatus {
            $0.pendingCount = count
            if $0.state == .idle || $0.state == .waitingForChanges {
              $0.state = count == 0 ? .idle : .waitingForChanges
            }
          }
        }
      } catch is CancellationError {
      } catch {
        logger.error(
          "Could not read pending iCloud downloads; \(Self.downloadFailureSummary(error), privacy: .public)"
        )
      }
    }

    func recordPendingDownloadThrottle(_ error: CKError?) async {
      guard let error, let seconds = error.retryAfterSeconds, seconds > 0 else { return }
      let generation = pendingDownloadRecovery.generation
      let retryAt =
        downloadDate.wrappedValue.now.timeIntervalSince1970 + seconds
        + downloadJitter.wrappedValue()
      do {
        try await userDatabase.write { db in
          try self.pendingDownloadRecovery.withGeneration(generation) {
            let previous = try DownloadRetry.find(1).fetchOne(db)
            if retryAt > (previous?.retryAt ?? 0) {
              try DownloadRetry.upsert {
                DownloadRetry(retryAt: retryAt, attempt: previous?.attempt ?? 0)
              }.execute(db)
            }
          }
        }
        try pendingDownloadRecovery.withGeneration(generation) {
          updatePendingDownloadStatus { $0.lastFailure = Self.downloadFailureSummary(error) }
        }
      } catch is CancellationError {
      } catch {
        logger.error(
          "Could not persist iCloud download backoff; \(Self.downloadFailureSummary(error), privacy: .public)"
        )
      }
    }

    private func recoverPendingDownloads(generation: Int) async throws {
      do {
        while true {
          try pendingDownloadRecovery.check(generation)
          if isFetchingChanges {
            try await downloadClock.wrappedValue.sleep(for: .milliseconds(200))
            continue
          }
          let pending = try await metadatabase.read { db in
            try UnsyncedRecordID.all.fetchAll(db).map(CKRecord.ID.init(unsyncedRecordID:))
          }
          try pendingDownloadRecovery.withGeneration(generation) {
            updatePendingDownloadStatus { $0.pendingCount = pending.count }
          }
          guard !pending.isEmpty else {
            try await saveDownloadRetry(nil, generation: generation)
            try pendingDownloadRecovery.withGeneration(generation) {
              updatePendingDownloadStatus {
                $0.state = .idle
                $0.nextRetry = nil
                $0.lastFailure = nil
              }
            }
            return
          }

          if let retry = try await metadatabase.read({ try DownloadRetry.find(1).fetchOne($0) }),
            let retryAt = retry.retryAt
          {
            let deadline = Date(timeIntervalSince1970: retryAt)
            let delay = deadline.timeIntervalSince(downloadDate.wrappedValue.now)
            if delay > 0 {
              try pendingDownloadRecovery.withGeneration(generation) {
                updatePendingDownloadStatus {
                  $0.state = .waitingToRetry
                  $0.nextRetry = deadline
                }
              }
              try await downloadClock.wrappedValue.sleep(for: .seconds(delay))
            }
            try await userDatabase.write { db in
              try self.pendingDownloadRecovery.withGeneration(generation) {
                if try DownloadRetry.find(1).fetchOne(db)?.retryAt == retryAt {
                  try DownloadRetry.upsert { DownloadRetry(attempt: retry.attempt) }.execute(db)
                }
              }
            }
            continue
          }

          try pendingDownloadRecovery.withGeneration(generation) {
            updatePendingDownloadStatus {
              $0.state = .recovering
              $0.nextRetry = nil
            }
          }
          let ordered = pending.sorted {
            if $0.zoneID != $1.zoneID {
              return ($0.zoneID.ownerName, $0.zoneID.zoneName)
                < ($1.zoneID.ownerName, $1.zoneID.zoneName)
            }
            if $0.tableName != $1.tableName {
              return topologicallyAscending(
                lhsTableName: $0.tableName, rhsTableName: $1.tableName, rootFirst: true
              )
            }
            return $0.recordName < $1.recordName
          }
          var madeProgress = false
          var retryErrors: [any Error] = []
          var blockingError: (any Error)?
          var offset = 0
          while offset < ordered.count {
            try pendingDownloadRecovery.check(generation)
            if isFetchingChanges { break }
            let batch = Array(
              ordered[offset...].prefix(150).prefix { $0.zoneID == ordered[offset].zoneID })
            offset += batch.count
            do {
              guard try await container.accountStatus() == .available else {
                throw CKError(.notAuthenticated)
              }
              let results = try await container.database(for: batch[0]).records(for: batch)
              try pendingDownloadRecovery.check(generation)
              var records: [CKRecord] = []
              var missing: [CKRecord.ID] = []
              for id in batch {
                guard let result = results[id] else {
                  blockingError = PendingDownloadError.incompleteResponse
                  continue
                }
                switch result {
                case .success(let record):
                  records.append(record)
                case .failure(let error as CKError) where error.code == .unknownItem:
                  missing.append(id)
                case .failure(let error):
                  if Self.downloadRetryDelay(error) != nil {
                    retryErrors.append(error)
                  } else {
                    blockingError = error
                  }
                }
              }
              records.sort {
                topologicallyAscending(
                  lhsTableName: $0.recordType, rhsTableName: $1.recordType, rootFirst: true
                )
              }
              let downloaded = records
              let missingIDs = missing
              let remaining = try await userDatabase.write { db in
                try self.pendingDownloadRecovery.withGeneration(generation) {
                  for record in downloaded {
                    self.upsertFromServerRecord(record, db: db)
                  }
                  if !missingIDs.isEmpty {
                    try UnsyncedRecordID.findAll(missingIDs).delete().execute(db)
                  }
                  return try UnsyncedRecordID.count().fetchOne(db) ?? 0
                }
              }
              let before = downloadStatus.pendingCount
              if remaining < before {
                madeProgress = true
                try pendingDownloadRecovery.withGeneration(generation) {
                  updatePendingDownloadStatus {
                    $0.pendingCount = remaining
                    $0.lastProgress = downloadDate.wrappedValue.now
                  }
                }
                logger.info(
                  "Deferred iCloud downloads applied: \(before - remaining), remaining: \(remaining)"
                )
              }
            } catch is CancellationError {
              throw CancellationError()
            } catch {
              if Self.downloadRetryDelay(error) != nil {
                retryErrors.append(error)
              } else {
                blockingError = error
              }
            }
            if !retryErrors.isEmpty { break }
          }
          if !retryErrors.isEmpty {
            let previousAttempt = try await metadatabase.read {
              try DownloadRetry.find(1).fetchOne($0)?.attempt ?? 0
            }
            let exponential = min(60.0, pow(2, Double(min(previousAttempt, 6))))
            let serverDelay = retryErrors.compactMap(Self.downloadRetryDelay).max() ?? 0
            let delay = max(exponential, serverDelay) + downloadJitter.wrappedValue()
            let retry = DownloadRetry(
              retryAt: downloadDate.wrappedValue.now.addingTimeInterval(delay)
                .timeIntervalSince1970,
              attempt: min(previousAttempt + 1, 6)
            )
            try await saveDownloadRetry(retry, generation: generation)
            let failure = Self.downloadFailureSummary(retryErrors[0])
            try pendingDownloadRecovery.withGeneration(generation) {
              updatePendingDownloadStatus { $0.lastFailure = failure }
            }
            logger.notice(
              "Deferred iCloud download retry in \(delay) seconds; \(failure, privacy: .public)")
            continue
          }
          if isFetchingChanges { continue }
          if madeProgress { continue }
          throw blockingError ?? PendingDownloadError.incomplete(downloadStatus.pendingCount)
        }
      } catch is CancellationError {
        throw CancellationError()
      } catch {
        try pendingDownloadRecovery.withGeneration(generation) {
          updatePendingDownloadStatus {
            $0.state = .blocked
            $0.nextRetry = nil
            $0.lastFailure = Self.downloadFailureSummary(error)
          }
        }
        logger.error(
          "Deferred iCloud downloads blocked; \(Self.downloadFailureSummary(error), privacy: .public)"
        )
        throw error
      }
    }

    private func saveDownloadRetry(_ retry: DownloadRetry?, generation: Int) async throws {
      try await userDatabase.write { db in
        try self.pendingDownloadRecovery.withGeneration(generation) {
          if let retry {
            try DownloadRetry.upsert { retry }.execute(db)
          } else {
            try DownloadRetry.delete().execute(db)
          }
        }
      }
    }

    static func downloadFailureSummary(_ error: any Error) -> String {
      if let error = error as? CKError {
        return "CloudKit error \(error.code.rawValue)."
      }
      if let error = error as? PendingDownloadError { return error.localizedDescription }
      return
        "Could not apply downloaded records (\((error as NSError).domain), code \((error as NSError).code))."
    }

    static func downloadRetryDelay(_ error: any Error) -> Double? {
      guard let error = error as? CKError else { return nil }
      switch error.code {
      case .networkFailure, .networkUnavailable, .serviceUnavailable, .requestRateLimited,
        .zoneBusy, .notAuthenticated, .accountTemporarilyUnavailable, .serverResponseLost,
        .operationCancelled:
        return max(0, error.retryAfterSeconds ?? 0)
      case .partialFailure:
        return error.partialErrorsByItemID?.values.compactMap(downloadRetryDelay).max()
      default:
        return nil
      }
    }
  }

  enum PendingDownloadError: LocalizedError {
    case stopped
    case incomplete(Int)
    case incompleteResponse

    var errorDescription: String? {
      switch self {
      case .stopped: "iCloud sync is stopped."
      case .incomplete(let count):
        "\(count) downloaded records are waiting for required related records."
      case .incompleteResponse: "iCloud did not return a result for every requested record."
      }
    }
  }

  private enum DownloadJitterKey: DependencyKey {
    static var liveValue: @Sendable () -> Double { { Double.random(in: 0.1...0.5) } }
    static var testValue: @Sendable () -> Double { { 0.25 } }
  }

  extension DependencyValues {
    var downloadRetryJitter: @Sendable () -> Double {
      get { self[DownloadJitterKey.self] }
      set { self[DownloadJitterKey.self] = newValue }
    }
  }

  @Table("sqlitedata_icloud_downloadRetry")
  struct DownloadRetry {
    var id: Int = 1
    var retryAt: Double?
    var attempt: Int = 0
  }

  final class PendingDownloadRecovery: @unchecked Sendable {
    private struct State {
      var generation = 0
      var enabled = true
      var request = 0
      var identifier = 0
      var task: Task<Void, any Error>?
    }
    private let lock = NSRecursiveLock()
    private var state = State()

    private func withState<T>(_ operation: (inout State) throws -> T) rethrows -> T {
      lock.lock()
      defer { lock.unlock() }
      return try operation(&state)
    }

    var generation: Int {
      withState { $0.generation }
    }

    func enable() {
      withState { $0.enabled = true }
    }

    func cancel() {
      withState {
        $0.enabled = false
        $0.generation += 1
        $0.task?.cancel()
        $0.task = nil
      }
    }

    func check(_ generation: Int) throws {
      try Task.checkCancellation()
      try withGeneration(generation) {}
    }

    func withGeneration<T>(_ generation: Int, operation: () throws -> T) throws -> T {
      try withState {
        guard $0.enabled, $0.generation == generation else { throw CancellationError() }
        return try operation()
      }
    }

    func run(_ operation: @escaping @Sendable (Int) async throws -> Void) async throws {
      let task = try withState { value in
        guard value.enabled else { throw CancellationError() }
        value.request += 1
        if let task = value.task { return task }
        value.identifier += 1
        let identifier = value.identifier
        let generation = value.generation
        let task = Task { [self] in
          while true {
            let request = withState { $0.request }
            let result = await Result { try await operation(generation) }
            let repeatRequested = withState { value in
              guard value.identifier == identifier, value.generation == generation else {
                return false
              }
              if value.enabled, value.request != request { return true }
              value.task = nil
              return false
            }
            if repeatRequested { continue }
            try result.get()
            return
          }
        }
        value.task = task
        return task
      }
      try await task.value
    }
  }
#endif
