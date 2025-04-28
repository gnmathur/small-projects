package tester.legacy

import lib.AsyncBufferedWriter
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDateTime
import java.util.concurrent.{CountDownLatch, Executors}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

/**
 * Test module for BufferedWriterPromise
 * This is an independent test that uses the BufferedWriter API
 */
object BufferedWriterScaleTester extends App {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // Configuration
  private val ROWS_PER_THREAD = 10_000 // 100K rows per thread
  private val THREAD_COUNT = 2
  private val ROWS_PER_BATCH = 100 // How many rows to insert in a single batch

  // Statistics
  @volatile private var totalRowsInserted = 0
  @volatile private var totalFuturesSucceeded = 0
  @volatile private var totalFuturesFailed = 0
  private val threadResults = Array.fill(THREAD_COUNT)(0)

  // For synchronization
  private val latch = new CountDownLatch(THREAD_COUNT)
  private val executor = Executors.newFixedThreadPool(THREAD_COUNT)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executor)

  // Create a random generator for balance values
  private val random = new Random()

  /**
   * Test function that runs a worker thread inserting rows with IDs in a specific range
   *
   * @param threadId The ID of the thread (0-based)
   * @param startId The first ID to use for this thread
   * @return Future containing the number of rows inserted
   */
  def runTestWorker(threadId: Int, startId: Int, bufferedWriterPromise: AsyncBufferedWriter): Future[Int] = Future {
    println(s"Thread $threadId starting with ID range: $startId to ${startId + ROWS_PER_THREAD - 1}")
    val startTime = System.currentTimeMillis()

    // Insert SQL statement
    val insertSql = "INSERT INTO users (id, name, balance, created_date) VALUES (?, ?, ?, ?)"

    try {
      var rowsInserted = 0
      var pendingFutures = List.empty[Future[Any]]

      // Process in smaller batches
      for (batchStart <- startId until (startId + ROWS_PER_THREAD) by ROWS_PER_BATCH) {
        val batchEnd = Math.min(batchStart + ROWS_PER_BATCH, startId + ROWS_PER_THREAD)
        val rows = (batchStart until batchEnd).map { id =>
          val balance = 1000.0 + random.nextDouble() * 9000.0
          val name = s"User-${threadId}-${id}"
          Seq(id, name, balance, LocalDateTime.now())
        }

        try {
          // Call the BufferedWriter API - but don't wait for it to complete
          val insertFuture = bufferedWriterPromise.insert(insertSql, rows)

          // Track the future for later result reporting without blocking
          insertFuture.onComplete {
            case Success(result) =>
              println(s"Completed $totalFuturesSucceeded successfully")
              synchronized {
                totalFuturesSucceeded += 1
              }
            case Failure(ex) =>
              println(s"Future failed: $ex.getMessage")
              synchronized {
                totalFuturesFailed += 1
                if (ex.getMessage == null) {
                  ex.printStackTrace()
                }
              }
          }

          // Add to our list of futures
          pendingFutures = insertFuture :: pendingFutures

          // Record the fact that we've queued the rows
          rowsInserted += rows.size
          if (rowsInserted % 10000 == 0) {
            logger.debug("Thread {} queued {} rows", threadId, rowsInserted)
          }
        } catch {
          case ex: Exception =>
            logger.error(s"Thread $threadId failed to insert batch: ${ex.getMessage}")
            if (ex.getMessage == null) {
              ex.printStackTrace()
            }
          // Continue with next batch rather than failing the entire thread
        }

        // wait for a random 1 - 10ms delay
        val delay = 1 + random.nextInt(10)
        Thread.sleep(delay)
      }

      val duration = System.currentTimeMillis() - startTime
      println(s"Thread $threadId completed. Queued $rowsInserted rows in ${duration}ms (${rowsInserted * 1000.0 / duration} rows/sec)")

      // Store result and count down latch
      threadResults(threadId) = rowsInserted
      latch.countDown()

      rowsInserted
    } catch {
      case ex: Exception =>
        logger.error(s"Thread $threadId encountered an error: ${ex.getMessage}")
        if (ex.getMessage == null) {
          ex.printStackTrace()
        }
        latch.countDown()
        throw ex
    }
  }

  /**
   * Main test method that creates and runs all test threads
   */
  def runTest(): Unit = {
    val startTime = System.currentTimeMillis()
    println(s"Starting test with $THREAD_COUNT threads, each inserting $ROWS_PER_THREAD rows")

    // Create an instance of BufferedWriterPromise
    val bufferedWriter = new AsyncBufferedWriter(2000, "192.168.52.194", 5432, "scaling-tests", "postgres", "postgres")

    // Launch all worker threads
    val futures = (0 until THREAD_COUNT).map { threadId =>
      val startId = threadId * ROWS_PER_THREAD
      runTestWorker(threadId, startId, bufferedWriter)
    }

    // Wait for all worker threads to complete
    futures.foreach { future =>
      future.onComplete {
        case Success(count) =>
          synchronized { totalRowsInserted += count }
        case Failure(ex) =>
          println(s"A thread failed: ${ex.getMessage}")
          if (ex.getMessage == null) {
            ex.printStackTrace()
          }
      }
    }

    // Wait for all threads to finish
    println("Waiting for all threads to complete...")
    latch.await()

    // Print results
    val totalDuration = System.currentTimeMillis() - startTime
    println("\n----- TEST RESULTS -----")
    println(s"Total rows queued: $totalRowsInserted")
    println(s"Total futures completed successfully: $totalFuturesSucceeded")
    println(s"Total futures failed: $totalFuturesFailed")
    println(s"Total duration: ${totalDuration}ms")
    println(s"Overall throughput: ${totalRowsInserted * 1000.0 / totalDuration} rows/sec")

    for (i <- 0 until THREAD_COUNT) {
      println(s"Thread $i queued ${threadResults(i)} rows")
    }

    // Wait for a moment to allow more futures to complete and print final stats. This is really for the thread
    // Futures created via insert to complete. I have not been able to find out a way to wait for all futures to complete
    println("\nWaiting 10 seconds for remaining futures to complete...")
    Thread.sleep(20_000)
    println(s"Final count - Futures completed successfully: $totalFuturesSucceeded")
    println(s"Final count - Futures failed: $totalFuturesFailed")

    // Clean up
    executor.shutdown()
    bufferedWriter.shutdown()
  }

  // Run the test
  try {
    runTest()
  } catch {
    case ex: Exception => println(s"Test failed with error: ${ex.getMessage}")
  } finally {
    if (!executor.isShutdown)
      executor.shutdown()
  }
}