package tester.legacy

import lib.AsyncBufferedWriter
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try} // Import Try

/**
 * A version of BufferedWriterScaleTester that uses a different approach to handle insert futures. This one waits for
 * all insert futures to complete before proceeding, and uses a latch to synchronize the completion of all worker threads.
 */
object BufferedWriterScaleTester2 extends App { // Renamed to avoid conflicts if running side-by-side
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // Configuration
  private val ROWS_PER_THREAD = 100_000
  private val THREAD_COUNT = 1
  private val ROWS_PER_BATCH = 1000

  // --- Statistics ---
  // Tracks rows *attempted* by each worker thread loop
  private val threadQueuedCounts = Array.fill(THREAD_COUNT)(0)
  // Tracks individual insert Future outcomes across all threads
  private val totalIndividualInsertsSucceeded = new AtomicInteger(0)
  private val totalIndividualInsertsFailed = new AtomicInteger(0)
  // Tracks overall worker outcomes (did the worker Future complete successfully?)
  @volatile private var totalWorkersSucceeded = 0
  @volatile private var totalWorkersFailed = 0
  @volatile private var totalRowsAttemptedAcrossWorkers = 0

  // --- Synchronization ---
  // Latch waits for all worker Futures (including their internal inserts) to complete
  private val latch = new CountDownLatch(THREAD_COUNT)
  // Dedicated thread pool for the workers
  private val executor = Executors.newFixedThreadPool(THREAD_COUNT)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executor)

  // Random data generator
  private val random = new Random()

  /**
   * Runs a worker thread inserting rows.
   * The returned Future completes only after all insert futures initiated by this worker
   * have completed (successfully or not).
   *
   * @param threadId The ID of the thread (0-based)
   * @param startId The first ID to use for this thread
   * @param bufferedWriter The BufferedWriter instance
   * @return Future containing the number of rows *attempted* by this worker.
   *         The Future fails if the worker encounters a critical setup error OR
   *         if Future.sequence fails (meaning at least one insertFuture failed).
   */
  def runTestWorker(threadId: Int, startId: Int, bufferedWriter: AsyncBufferedWriter): Future[Int] = {

    // Phase 1: Initiate all inserts and collect their Futures
    val initiationFuture: Future[(List[Future[Any]], Int, Long)] = Future {
      // This block runs asynchronously on the ExecutionContext
      logger.info(s"Thread $threadId starting: ID range $startId to ${startId + ROWS_PER_THREAD - 1}")
      val startTime = System.currentTimeMillis()
      val insertSql = "INSERT INTO users (id, name, balance, created_date) VALUES (?, ?, ?, ?)"

      var rowsQueuedInLoop = 0
      var batchFutures = List.empty[Future[Any]] // Collect Futures from bufferedWriter.insert

      try {
        for (batchStart <- startId until (startId + ROWS_PER_THREAD) by ROWS_PER_BATCH) {
          val batchEnd = Math.min(batchStart + ROWS_PER_BATCH, startId + ROWS_PER_THREAD)
          val rowsData = (batchStart until batchEnd).map { id =>
            val balance = 1000.0 + random.nextDouble() * 9000.0
            val name = s"User-${threadId}-${id}"
            Seq(id, name, balance, LocalDateTime.now())
          }

          // Initiate the insert - assumes bufferedWriter.insert returns Future[Something]
          val insertFuture = bufferedWriter.insert(insertSql, rowsData)

          // Add callback for *individual* insert future statistics (optional but useful)
          insertFuture.onComplete {
            case Success(_) => totalIndividualInsertsSucceeded.incrementAndGet()
            case Failure(ex) =>
              totalIndividualInsertsFailed.incrementAndGet()
              // Log individual failures, but don't fail the whole worker yet
              logger.warn(s"Thread $threadId: Individual insert batch future failed: ${ex.getMessage}")
          }(ExecutionContext.parasitic) // Use parasitic context for lightweight callbacks

          batchFutures = insertFuture :: batchFutures
          rowsQueuedInLoop += rowsData.size

          if (rowsQueuedInLoop > 0 && rowsQueuedInLoop % (ROWS_PER_THREAD / 5) == 0) { // Log progress periodically
            logger.debug(s"Thread $threadId queued $rowsQueuedInLoop / $ROWS_PER_THREAD rows")
          }
        }
        val duration = System.currentTimeMillis() - startTime
        logger.info(s"Thread $threadId finished QUEUING $rowsQueuedInLoop rows in ${duration}ms. Waiting for inserts...")
        // Return collected futures, count, and start time for the next phase
        (batchFutures.reverse, rowsQueuedInLoop, startTime) // Reverse maintains insertion order if relevant
      } catch {
        case ex: Throwable =>
          // Catch errors during the synchronous loop/initiation phase
          logger.error(s"Thread $threadId CRITICAL FAILURE during insert initiation: ${ex.getMessage}", ex)
          throw ex // Rethrow to fail the initiationFuture
      }
    }

    // Phase 2: Wait for all collected insert Futures to complete using Future.sequence
    initiationFuture.flatMap { case (pendingInsertFutures, rowsQueuedCount, workerStartTime) =>
      // Future.sequence aggregates results. Completes when all futures in the list are done.
      val allInsertsCompleteFuture: Future[List[Any]] = Future.sequence(pendingInsertFutures)

      // Now, handle the outcome of waiting for all inserts
      allInsertsCompleteFuture.map { _ =>
        // SUCCESS CASE: All futures in pendingInsertFutures completed successfully.
        val duration = System.currentTimeMillis() - workerStartTime
        logger.info(s"Thread $threadId COMPLETED ALL ${pendingInsertFutures.size} insert batches successfully. Total worker time: ${duration}ms")
        threadQueuedCounts(threadId) = rowsQueuedCount // Record final count for this thread
        latch.countDown() // Signal this worker is fully done
        rowsQueuedCount // Return the count as the successful result of the worker future
      }.recover {
        case ex =>
          // FAILURE CASE: Future.sequence failed because at least one insertFuture failed.
          val duration = System.currentTimeMillis() - workerStartTime
          logger.error(s"Thread $threadId FAILED: At least one insert operation failed after ${duration}ms. First error: ${ex.getMessage}", ex)
          threadQueuedCounts(threadId) = rowsQueuedCount // Record attempted count even on failure
          latch.countDown() // IMPORTANT: Count down latch even on failure!
          // Rethrow the exception to make this worker's Future fail.
          throw new RuntimeException(s"Thread $threadId failed because one or more underlying inserts failed.", ex)
      }
    }.recover {
      // FAILURE CASE: Catches failures from the initiationFuture itself (Phase 1)
      case ex =>
        logger.error(s"Thread $threadId FAILED during initiation phase: ${ex.getMessage}", ex)
        threadQueuedCounts(threadId) = 0 // Or some partial count if applicable
        latch.countDown() // Ensure latch counts down if initiation fails critically
        throw ex // Propagate the failure
    }
  }

  /**
   * Main test method.
   */
  def runTest(): Unit = {
    val overallStartTime = System.currentTimeMillis()
    logger.info(s"Starting test: $THREAD_COUNT threads, $ROWS_PER_THREAD rows/thread, $ROWS_PER_BATCH rows/batch")

    // --- Initialize BufferedWriter ---
    // Ensure BufferedWriter is thread-safe OR create one per thread if needed.
    // Assuming a single shared instance is intended and thread-safe:
    val bufferedWriter = try {
      new AsyncBufferedWriter(500, 5000, "192.168.52.194", 5432, "scaling-tests", "postgres", "postgres")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to initialize BufferedWriter: ${e.getMessage}", e)
        executor.shutdown()
        return // Cannot proceed
    }

    // --- Launch Worker Threads ---
    val workerFutures: Seq[Future[Int]] = (0 until THREAD_COUNT).map { threadId =>
      val startId = threadId * ROWS_PER_THREAD
      runTestWorker(threadId, startId, bufferedWriter)
    }

    // --- Aggregate Worker Results ---
    // Use transform + sequence to get a Future[Seq[Try[Int]]]
    // This future completes only when ALL worker futures have completed (successfully or with failure).
    val aggregatedWorkersFuture: Future[Seq[Try[Int]]] =
      Future.sequence(workerFutures.map(_.transform(Success(_)))) // Transforms Future[X] -> Future[Try[X]]

    // Add a callback to the aggregated future for final reporting *after* everything is done.
    aggregatedWorkersFuture.onComplete { resultTry =>
      // This block executes once, after all workers have finished or failed.
      // The latch should already be 0 if waited upon, but this is for processing the final results.

      totalRowsAttemptedAcrossWorkers = threadQueuedCounts.sum // Sum attempts from each thread

      resultTry match {
        case Success(results: Seq[Try[Int]]) =>
          totalWorkersSucceeded = results.count(_.isSuccess)
          totalWorkersFailed = results.count(_.isFailure) // Or THREAD_COUNT - totalWorkersSucceeded
          logger.info(s"All $THREAD_COUNT workers finished. Success: $totalWorkersSucceeded, Failed: $totalWorkersFailed")

        case Failure(ex) =>
          // This shouldn't typically happen with transform(Success(_)) unless sequence itself fails badly
          logger.error(s"Aggregated future monitoring failed unexpectedly: ${ex.getMessage}", ex)
          // Attempt fallback calculation based on individual future states (less reliable)
          totalWorkersSucceeded = workerFutures.count(f => f.isCompleted && f.value.exists(_.isSuccess))
          totalWorkersFailed = THREAD_COUNT - totalWorkersSucceeded
          logger.warn(s"Using fallback worker counts. Success: $totalWorkersSucceeded, Failed: $totalWorkersFailed")
      }
    }(ExecutionContext.parasitic) // Parasitic context is fine for this final calculation

    // --- Wait for Completion ---
    logger.info("Main thread waiting for all workers (and their inserts) to complete...")
    try {
      // Wait for all workers to call latch.countDown()
      latch.await(10, TimeUnit.MINUTES) // Add a timeout to prevent indefinite blocking
      if (latch.getCount > 0) {
        logger.warn(s"Timeout reached! ${latch.getCount} worker(s) did not complete.")
      } else {
        logger.info("All workers have signaled completion via latch.")
      }
    } catch {
      case ie: InterruptedException =>
        logger.warn("Main thread interrupted while waiting for workers.")
        Thread.currentThread().interrupt() // Preserve interrupt status
    }

    // --- Print Final Results ---
    val overallDuration = System.currentTimeMillis() - overallStartTime
    println("\n----- FINAL TEST RESULTS -----")
    println(s"Test Duration: ${overallDuration}ms")
    println(s"Total Workers Launched: $THREAD_COUNT")
    println(s"  Workers Succeeded (Future completed OK): $totalWorkersSucceeded")
    println(s"  Workers Failed (Future completed Failure): $totalWorkersFailed")
    println(s"Total Rows Attempted Across All Workers: $totalRowsAttemptedAcrossWorkers")
    println(s"Individual Insert Batch Futures:")
    println(s"  Succeeded: ${totalIndividualInsertsSucceeded.get()}")
    println(s"  Failed:    ${totalIndividualInsertsFailed.get()}")

    if (overallDuration > 0 && totalRowsAttemptedAcrossWorkers > 0) {
      // Calculate throughput based on rows *attempted* by workers that *finished* (success or fail)
      val throughput = totalRowsAttemptedAcrossWorkers * 1000.0 / overallDuration
      println(f"Overall Attempted Throughput: $throughput%.2f rows/sec")
    } else {
      println("Overall Attempted Throughput: N/A (duration or rows is zero)")
    }

    println("Rows attempted per thread:")
    threadQueuedCounts.zipWithIndex.foreach { case (count, i) =>
      println(s"  Thread $i: $count rows")
    }

    // The explicit Thread.sleep is NO LONGER needed here.

    // --- Clean up ---
    logger.info("Shutting down...")
    try {
      bufferedWriter.shutdown() // Shutdown your writer resource
    } catch {
      case e: Exception => logger.error(s"Error shutting down BufferedWriter: ${e.getMessage}", e)
    }
    executor.shutdown() // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
        logger.warn("Executor pool did not terminate gracefully after 30 seconds, forcing shutdown...")
        executor.shutdownNow() // Cancel currently executing tasks
        // Wait a little more for tasks to respond to being cancelled
        if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
          logger.error("Executor pool did not terminate even after forcing.")
        }
      }
      logger.info("Executor shutdown complete.")
    } catch {
      case ie: InterruptedException =>
        logger.warn("Interrupted during executor shutdown.")
        executor.shutdownNow()
        Thread.currentThread().interrupt()
    }
  }

  // --- Run the test ---
  try {
    runTest()
    println("\nTest execution finished.")
  } catch {
    case ex: Exception =>
      logger.error(s"Unhandled exception in main test execution: ${ex.getMessage}", ex)
  } finally {
    // Final check to ensure executor is shut down if something went very wrong early on
    if (!executor.isShutdown) {
      logger.warn("Executor was not shut down in main flow, forcing shutdown in finally block.")
      executor.shutdownNow()
    }
  }
}