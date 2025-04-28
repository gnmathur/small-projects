package tester.ratedefined

// Statistics tracking
class Stats {
  @volatile var rowsProduced: Int = 0
  @volatile var rowsConsumed: Int = 0
  @volatile var batchesProcessed: Int = 0
  @volatile var futuresSucceeded: Int = 0
  @volatile var futuresFailed: Int = 0
  // Sync-only
  @volatile var rowsWritten: Int = 0
  @volatile var errorCount: Int = 0
}
