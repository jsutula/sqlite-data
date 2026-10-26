#if canImport(CloudKit)
  import CloudKit
  import ConcurrencyExtras
  import CustomDump
  import DependenciesTestSupport
  import Foundation
  @testable import SQLiteData
  import Testing
  import TestLocals

  extension BaseCloudKitTests {
    @MainActor
    final class PendingDownloadsTests: BaseCloudKitTests, @unchecked Sendable {
      private func seed(_ count: Int, zoneID: CKRecordZone.ID = SyncEngine.defaultTestZone.zoneID)
        async throws -> CKRecord
      {
        try await syncEngine.retryPendingDownloads()
        let database =
          container.database(for: CKRecord.ID(recordName: "routing", zoneID: zoneID))
          as! MockCloudDatabase
        _ = try database.modifyRecordZones(saving: [CKRecordZone(zoneID: zoneID)])
        let parent = CKRecord(
          recordType: RemindersList.tableName,
          recordID: RemindersList.recordID(for: 1, zoneID: zoneID))
        parent.setValue(1, forKey: "id", at: now)
        parent.setValue("List", forKey: "title", at: now)
        if zoneID.ownerName == CKCurrentUserDefaultName {
          _ = try database.modifyRecords(saving: [parent])
        } else {
          database.state.withValue { $0.storage[zoneID]?.records[parent.recordID] = parent }
        }
        for offset in stride(from: 0, to: count, by: 150) {
          let records = (offset..<min(count, offset + 150)).map { index in
            let record = CKRecord(
              recordType: Reminder.tableName,
              recordID: Reminder.recordID(for: index + 1, zoneID: zoneID))
            record.setValue(index + 1, forKey: "id", at: now)
            record.setValue("Reminder", forKey: "title", at: now)
            record.setValue(1, forKey: "remindersListID", at: now)
            record.parent = CKRecord.Reference(record: parent, action: .none)
            return record
          }
          _ = try database.modifyRecords(saving: records)
          await syncEngine.handleFetchedRecordZoneChanges(
            modifications: records, syncEngine: syncEngine.private)
        }
        return parent
      }

      private func applyParent(_ parent: CKRecord) async {
        await syncEngine.handleFetchedRecordZoneChanges(
          modifications: [parent], syncEngine: syncEngine.private)
      }

      private func waitFor(_ condition: @MainActor () -> Bool) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(10))
        while !condition() {
          guard ContinuousClock.now < deadline else {
            Issue.record("Timed out waiting for download recovery")
            throw PendingDownloadError.incomplete(-1)
          }
          try await Task.sleep(for: .milliseconds(1))
        }
      }

      private func reminderCount() throws -> Int {
        try userDatabase.read { try Reminder.count().fetchOne($0) ?? 0 }
      }

      private func pendingCount() throws -> Int {
        try syncEngine.metadatabase.read { try UnsyncedRecordID.count().fetchOne($0) ?? 0 }
      }

      @Test func multiplePagesAndEmptyDelta() async throws {
        let parent = try await seed(301)
        expectNoDifference(try pendingCount(), 301)
        await applyParent(parent)
        let requests = LockIsolated<[Int]>([])
        container.privateCloudDatabase.recordsOverride.withValue { operation in
          operation = { [database = container.privateCloudDatabase] ids in
            requests.withValue { $0.append(ids.count) }
            return try database.readRecords(for: ids, desiredKeys: nil)
          }
        }
        // The mock delta cursor is already caught up; only the persistent queue remains.
        syncEngine.private.state.changeTag.withValue { $0 = Int.max }
        try await syncEngine.fetchChanges()
        expectNoDifference(requests.withValue { $0 }, [150, 150, 1])
        expectNoDifference(try reminderCount(), 301)
        expectNoDifference(try pendingCount(), 0)
        expectNoDifference(syncEngine.downloadStatus.state, .idle)
      }

      @Test(arguments: [3.0, 22.0, 28.0])
      func preservesEarlierBatchesAndRespectsBackoff(_ seconds: Double) async throws {
        let parent = try await seed(301)
        await applyParent(parent)
        let requests = LockIsolated(0)
        container.privateCloudDatabase.recordsOverride.withValue { operation in
          operation = { [database = container.privateCloudDatabase] ids in
            let request = requests.withValue {
              $0 += 1
              return $0
            }
            if request == 2 {
              throw CKError(.serviceUnavailable, userInfo: [CKErrorRetryAfterKey: seconds])
            }
            return try database.readRecords(for: ids, desiredKeys: nil)
          }
        }
        let recovery = Task { try await syncEngine.retryPendingDownloads() }
        try await waitFor { syncEngine.downloadStatus.state == .waitingToRetry }
        expectNoDifference(try reminderCount(), 150)
        expectNoDifference(try pendingCount(), 151)
        expectNoDifference(requests.withValue { $0 }, 2)
        let joined = Task { try await syncEngine.retryPendingDownloads() }
        await testClock.advance(by: .seconds(seconds))
        expectNoDifference(requests.withValue { $0 }, 2)
        await testClock.advance(by: .seconds(0.25))
        try await recovery.value
        try await joined.value
        expectNoDifference(requests.withValue { $0 }, 4)
        expectNoDifference(try pendingCount(), 0)
      }

      @Test func perRecordFailurePreservesSuccesses() async throws {
        let parent = try await seed(3)
        await applyParent(parent)
        let requests = LockIsolated(0)
        container.privateCloudDatabase.recordsOverride.withValue { operation in
          operation = { [database = container.privateCloudDatabase] ids in
            let request = requests.withValue {
              $0 += 1
              return $0
            }
            var results = try database.readRecords(for: ids, desiredKeys: nil)
            if request == 1 {
              results[Reminder.recordID(for: 2)] = .failure(
                CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 3.0]))
              results[Reminder.recordID(for: 3)] = .failure(CKError(.unknownItem))
            }
            return results
          }
        }
        let recovery = Task { try await syncEngine.retryPendingDownloads() }
        try await waitFor { syncEngine.downloadStatus.state == .waitingToRetry }
        expectNoDifference(try pendingCount(), 1)
        expectNoDifference(try reminderCount(), 1)
        await testClock.advance(by: .seconds(3.25))
        try await recovery.value
        expectNoDifference(try reminderCount(), 2)
        expectNoDifference(try pendingCount(), 0)
      }

      @Test func restartRespectsPersistedDeadlineWithoutNewChanges() async throws {
        let parent = try await seed(1)
        await applyParent(parent)
        let requests = LockIsolated(0)
        container.privateCloudDatabase.recordsOverride.withValue { operation in
          operation = { [database = container.privateCloudDatabase] ids in
            requests.withValue { $0 += 1 }
            return try database.readRecords(for: ids, desiredKeys: nil)
          }
        }
        let deadline = Date(timeIntervalSince1970: 28)
        try await userDatabase.write {
          try DownloadRetry.upsert {
            DownloadRetry(retryAt: deadline.timeIntervalSince1970, attempt: 3)
          }.execute($0)
        }
        syncEngine.stop()
        try await syncEngine.start()
        try await waitFor { syncEngine.downloadStatus.state == .waitingToRetry }
        expectNoDifference(requests.withValue { $0 }, 0)
        expectNoDifference(syncEngine.downloadStatus.nextRetry, deadline)
        await testClock.advance(by: .seconds(28))
        try await syncEngine.retryPendingDownloads()
        expectNoDifference(requests.withValue { $0 }, 1)
        expectNoDifference(try pendingCount(), 0)
        // Restarting enqueues a zone save as part of the normal engine lifecycle.
        try await syncEngine.processPendingDatabaseChanges(scope: .private)
      }

      @Test func unresolvedDependenciesBlockFetchReadiness() async throws {
        let parent = try await seed(1)
        syncEngine.private.state.changeTag.withValue { $0 = Int.max }
        await #expect(throws: (any Error).self) { try await syncEngine.fetchChanges() }
        expectNoDifference(syncEngine.downloadStatus.state, .blocked)
        expectNoDifference(try pendingCount(), 1)
        await applyParent(parent)
        try await syncEngine.retryPendingDownloads()
        expectNoDifference(try pendingCount(), 0)
      }

      @Test func sharedQueueUsesSharedDatabase() async throws {
        let zoneID = CKRecordZone.ID(zoneName: "shared", ownerName: "otherUser")
        let parent = try await seed(1, zoneID: zoneID)
        await applyParent(parent)
        let privateCalls = LockIsolated(0)
        container.privateCloudDatabase.recordsOverride.withValue { operation in
          operation = { ids in
            privateCalls.withValue { $0 += 1 }
            throw CKError(.zoneNotFound)
          }
        }
        try await syncEngine.retryPendingDownloads()
        expectNoDifference(privateCalls.withValue { $0 }, 0)
        expectNoDifference(try reminderCount(), 1)
        expectNoDifference(try pendingCount(), 0)
      }

      @Test func cancellationRejectsLateResult() async throws {
        let parent = try await seed(1)
        await applyParent(parent)
        let response = LockIsolated<CheckedContinuation<Void, Never>?>(nil)
        container.privateCloudDatabase.recordsOverride.withValue { operation in
          operation = { [database = container.privateCloudDatabase] ids in
            await withCheckedContinuation { continuation in
              response.withValue { $0 = continuation }
            }
            return try database.readRecords(for: ids, desiredKeys: nil)
          }
        }
        let recovery = Task { try await syncEngine.retryPendingDownloads() }
        try await waitFor { response.withValue { $0 != nil } }
        syncEngine.stop()
        response.withValue {
          $0?.resume()
          $0 = nil
        }
        await #expect(throws: CancellationError.self) { try await recovery.value }
        expectNoDifference(try reminderCount(), 0)
        expectNoDifference(try pendingCount(), 1)
        expectNoDifference(syncEngine.downloadStatus.state, .stopped)
        // A later start can still apply the untouched queue.
        container.privateCloudDatabase.recordsOverride.withValue { $0 = nil }
        try await syncEngine.start()
        try await syncEngine.retryPendingDownloads()
        try await syncEngine.processPendingDatabaseChanges(scope: .private)
        expectNoDifference(try pendingCount(), 0)
      }

      @Test func engineThrottleAlsoDelaysRecovery() async throws {
        let parent = try await seed(1)
        await applyParent(parent)
        let requests = LockIsolated(0)
        container.privateCloudDatabase.recordsOverride.withValue { operation in
          operation = { [database = container.privateCloudDatabase] ids in
            requests.withValue { $0 += 1 }
            return try database.readRecords(for: ids, desiredKeys: nil)
          }
        }
        await syncEngine.handleEvent(.willFetchChanges, syncEngine: syncEngine.private)
        await syncEngine.handleEvent(
          .willFetchRecordZoneChanges(zoneID: parent.recordID.zoneID),
          syncEngine: syncEngine.private)
        await syncEngine.handleEvent(
          .didFetchRecordZoneChanges(
            zoneID: parent.recordID.zoneID,
            error: CKError(.requestRateLimited, userInfo: [CKErrorRetryAfterKey: 3.0])
          ), syncEngine: syncEngine.private
        )
        await syncEngine.handleEvent(.didFetchChanges, syncEngine: syncEngine.private)
        try await waitFor { syncEngine.downloadStatus.state == .waitingToRetry }
        let recovery = Task { try await syncEngine.retryPendingDownloads() }
        await testClock.advance(by: .seconds(3))
        expectNoDifference(requests.withValue { $0 }, 0)
        await testClock.advance(by: .seconds(0.25))
        try await recovery.value
        expectNoDifference(requests.withValue { $0 }, 1)
        expectNoDifference(try pendingCount(), 0)
      }

      @Test func permanentFailureKeepsQueueAndReportsBlocked() async throws {
        let parent = try await seed(1)
        await applyParent(parent)
        container.privateCloudDatabase.recordsOverride.withValue { operation in
          operation = { _ in throw CKError(.permissionFailure) }
        }
        await #expect(throws: CKError.self) { try await syncEngine.retryPendingDownloads() }
        expectNoDifference(try pendingCount(), 1)
        expectNoDifference(syncEngine.downloadStatus.state, .blocked)
        expectNoDifference(syncEngine.downloadStatus.lastFailure, "CloudKit error 10.")
        container.privateCloudDatabase.recordsOverride.withValue { $0 = nil }
        try await syncEngine.retryPendingDownloads()
        expectNoDifference(try reminderCount(), 1)
      }

      @Test(arguments: [false, true])
      func accountChangeRejectsLateResult(switchAccounts: Bool) async throws {
        let parent = try await seed(1)
        await applyParent(parent)
        let response = LockIsolated<CheckedContinuation<Void, Never>?>(nil)
        container.privateCloudDatabase.recordsOverride.withValue { operation in
          operation = { [database = container.privateCloudDatabase] ids in
            let result = try database.readRecords(for: ids, desiredKeys: nil)
            await withCheckedContinuation { continuation in
              response.withValue { $0 = continuation }
            }
            return result
          }
        }
        let recovery = Task { try await syncEngine.retryPendingDownloads() }
        try await waitFor { response.withValue { $0 != nil } }
        if switchAccounts {
          await syncEngine.handleAccountChange(
            changeType: .switchAccounts(
              previousUser: CKRecord.ID(recordName: "oldUser"),
              currentUser: CKRecord.ID(recordName: "newUser")
            ), syncEngine: syncEngine.private
          )
        } else {
          await signOut()
        }
        response.withValue {
          $0?.resume()
          $0 = nil
        }
        await #expect(throws: CancellationError.self) { try await recovery.value }
        expectNoDifference(try reminderCount(), 0)
        expectNoDifference(try pendingCount(), 0)
        syncEngine.stop()
      }

      @Test func transientFailureWithoutRetryAfterUsesBackoff() async throws {
        let parent = try await seed(1)
        await applyParent(parent)
        let requests = LockIsolated(0)
        container.privateCloudDatabase.recordsOverride.withValue { operation in
          operation = { [database = container.privateCloudDatabase] ids in
            let request = requests.withValue {
              $0 += 1
              return $0
            }
            if request == 1 { throw CKError(.networkFailure) }
            return try database.readRecords(for: ids, desiredKeys: nil)
          }
        }
        let recovery = Task { try await syncEngine.retryPendingDownloads() }
        try await waitFor { syncEngine.downloadStatus.state == .waitingToRetry }
        expectNoDifference(syncEngine.downloadStatus.nextRetry, Date(timeIntervalSince1970: 1.25))
        await testClock.advance(by: .seconds(1))
        expectNoDifference(requests.withValue { $0 }, 1)
        await testClock.advance(by: .seconds(0.25))
        try await recovery.value
        expectNoDifference(requests.withValue { $0 }, 2)
        expectNoDifference(try pendingCount(), 0)
      }
    }
  }
#endif
