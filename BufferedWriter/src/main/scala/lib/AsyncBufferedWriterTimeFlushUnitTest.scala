package lib

import io.prometheus.client.exporter.HTTPServer

import java.time.LocalDateTime
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success}
import scala.collection.mutable.ListBuffer
import org.slf4j.{Logger, LoggerFactory}

/**
 * The significant interplay in this test is between minDelayMs, maxDelayMs, and the 200 ms flush timeout. The
 * minDelayMs and maxDelayMs help ensure that the time based flush is triggered once in a while by artificially
 * delaying the insert calls. The 200 ms flush timeout is set to a value that is less than the maxDelayMs to ensure
 * that the time based flush is triggered at all
 */
object AsyncBufferedWriterTimeFlushUnitTest extends App {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val es: ExecutorService = Executors.newCachedThreadPool()
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(es)

  private def runUseCase(): Unit = {
    // Configuration
    val totalRows = 1000000  // Total rows to insert
    val minBatchSize = 20
    val maxBatchSize = 10000
    val minDelayMs = 10
    val maxDelayMs = 2000
    val rateReportingIntervalMs = 10000  // Report rate every 10 seconds

    // Specify the insert string
    val insertSql = "INSERT INTO users (id, name, balance, created_date) VALUES (?, ?, ?, ?)"

    // Create an instance of BufferedWriterPromise with a large batch size
    // Since we're varying batch sizes in our test, set this to a large value
    val bufferedWriter = new AsyncBufferedWriter(
      5000,  // Large batch size since we're controlling it in our test
      200,   // Flush timeout
      "192.168.52.194",
      5432,
      "postgres",
      "postgres",
      "postgres"
    )

    // Track all futures for monitoring completion
    val futures = ListBuffer[Future[AsyncBufferedWriter#InsertResult]]()

    var rowsInserted = 0
    val startTime = System.currentTimeMillis()
    var lastRateReportTime = startTime
    var lastRateReportRows = 0
    var insertCallCount = 0
    var insertCallsSinceLastReport = 0

    logger.info(s"Starting to insert $totalRows rows...")

    while (rowsInserted < totalRows) {
      // Randomly determine batch size for this iteration
      val batchSize = minBatchSize + Random.nextInt(maxBatchSize - minBatchSize + 1)

      // Ensure we don't exceed total rows
      val actualBatchSize = Math.min(batchSize, totalRows - rowsInserted)

      // Generate rows for this batch
      val batchRows = (1 to actualBatchSize).map { i =>
        val rowId = rowsInserted + i
        Seq(
          rowId,
          s"User_$rowId",
          Random.nextDouble() * 10000,  // Random balance
          LocalDateTime.now()
        )
      }

      // Insert the batch
      val future = bufferedWriter.insert(insertSql, batchRows)
      futures += future
      insertCallCount += 1
      insertCallsSinceLastReport += 1

      // Log the insertion
      val currentBatchNumber = futures.size
      future.onComplete {
        case Success(at) =>
          logger.debug(s"Batch #$currentBatchNumber: Successfully inserted $actualBatchSize rows at $at")
        case Failure(ex) =>
          logger.error(s"Batch #$currentBatchNumber: Failed to insert $actualBatchSize rows - ${ex.getMessage}", ex)
      }

      rowsInserted += actualBatchSize

      // Random delay between writes
      if (rowsInserted < totalRows) {
        val delayMs = minDelayMs + Random.nextInt(maxDelayMs - minDelayMs + 1)
        Thread.sleep(delayMs)
      }

      // Periodic rate reporting
      val currentTime = System.currentTimeMillis()
      if (currentTime - lastRateReportTime >= rateReportingIntervalMs) {
        val elapsedSeconds = (currentTime - lastRateReportTime) / 1000.0
        val rowsSinceLastReport = rowsInserted - lastRateReportRows
        val rowsPerSecond = rowsSinceLastReport / elapsedSeconds
        val insertsPerSecond = insertCallsSinceLastReport / elapsedSeconds

        logger.info(s"Rate Report: ${rowsPerSecond.toInt} rows/sec, ${insertsPerSecond.formatted("%.2f")} insert calls/sec")
        logger.info(s"Current progress: $rowsInserted/$totalRows rows (${(rowsInserted.toDouble / totalRows * 100).formatted("%.1f")}%)")

        lastRateReportTime = currentTime
        lastRateReportRows = rowsInserted
        insertCallsSinceLastReport = 0
      }

      // Progress update every 100,000 rows
      if (rowsInserted % 100000 == 0) {
        val elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000.0
        logger.info(s"Progress: $rowsInserted/$totalRows rows inserted (${elapsedSeconds}s elapsed)")
      }
    }

    logger.info(s"All insert operations submitted. Waiting for completion...")

    // Wait for all futures to complete
    val allFutures = Future.sequence(futures)
    Await.ready(allFutures, 10.minutes)

    val endTime = System.currentTimeMillis()
    val totalTimeSeconds = (endTime - startTime) / 1000.0

    // Count successful and failed operations
    val completedFutures = futures.map(_.value)
    val successCount = completedFutures.count(_.exists(_.isSuccess))
    val failureCount = completedFutures.count(_.exists(_.isFailure))

    logger.info(s"\n=== Test Summary ===")
    logger.info(s"Total rows inserted: $totalRows")
    logger.info(s"Total batches: ${futures.size}")
    logger.info(s"Total insert calls: $insertCallCount")
    logger.info(s"Average batch size: ${totalRows.toDouble / insertCallCount}")
    logger.info(s"Successful batches: $successCount")
    logger.info(s"Failed batches: $failureCount")
    logger.info(s"Total time: $totalTimeSeconds seconds")
    logger.info(s"Average throughput: ${totalRows / totalTimeSeconds} rows/second")
    logger.info(s"Average insert call rate: ${insertCallCount / totalTimeSeconds} calls/second")

    bufferedWriter.shutdown()
  }

  // Start HTTP server to expose Prometheus metrics on port 8080
  val server = new HTTPServer.Builder()
    .withPort(8989)
    .build()

  runUseCase()
  es.shutdown()
  server.close()
}