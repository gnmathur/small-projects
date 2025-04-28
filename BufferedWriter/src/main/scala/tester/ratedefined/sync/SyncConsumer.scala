package tester.ratedefined.sync

import lib.SyncBufferedWriter
import tester.ratedefined.{Stats, UserRow}

import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, TimeUnit}

/**
 * The Consumer class - takes batches from the queue and inserts them using SynchronousBufferedWriter
 */
class SyncConsumer(
                    id: Int,
                    queue: ArrayBlockingQueue[List[UserRow]],
                    bufferedWriter: SyncBufferedWriter,
                    stats: Stats,
                    producerDone: CountDownLatch,
                    consumersDone: CountDownLatch
              ) extends Runnable {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    val startTime = System.currentTimeMillis()
    logger.info(s"Consumer $id starting")

    // Insert SQL statement
    val insertSql = "INSERT INTO users (id, name, balance, created_date) VALUES (?, ?, ?, ?)"

    try {
      var rowsConsumed = 0
      var keepRunning = true

      while (keepRunning) {
        // Check if should finish (producer is done and queue is empty)
        if (producerDone.getCount == 0 && queue.isEmpty) {
          keepRunning = false
        } else {
          // Try to get a batch with timeout
          val batch = queue.poll(100, TimeUnit.MILLISECONDS)

          if (batch != null) {
            try {
              // Convert the UserRow objects to sequences for the BufferedWriter
              val rows = batch.map(row => Seq(row.id, row.name, row.balance, row.createdDate))

              // Call the SynchronousBufferedWriter API - no more futures
              val result = bufferedWriter.insert(insertSql, rows)

              // Determine if we need to make an explicit flush call
              if (result.rowsWritten == 0) {
                // Optional: decide if we want to force a flush based on some condition
                // For example, flush every 10 batches or if queue is nearly empty
                if (queue.size() < 5 || stats.batchesProcessed % 10 == 0) {
                  val flushResult = bufferedWriter.flush()
                  logger.debug(s"Explicit flush performed, wrote ${flushResult.rowsWritten} rows")
                }
              } else {
                logger.debug(s"Inserted ${result.rowsWritten} rows at ${result.writtenAt}")
              }

              // Update statistics
              rowsConsumed += batch.size
              synchronized {
                stats.rowsConsumed += batch.size
                stats.batchesProcessed += 1
                stats.rowsWritten += result.rowsWritten
              }

              if (rowsConsumed % 10000 == 0) {
                val elapsed = System.currentTimeMillis() - startTime
                val rate = if (elapsed > 0) rowsConsumed * 1000.0 / elapsed else 0
                logger.info(s"Consumer $id progress: $rowsConsumed rows, rate: ${rate.toInt} rows/sec")
              }
            } catch {
              case ex: Exception =>
                logger.error(s"Consumer $id failed to process batch: ${ex.getMessage}", ex)
                synchronized { stats.errorCount += 1 }
            }
          }
        }
      }

      // Make sure all remaining rows are written before exiting
      try {
        val finalFlushResult = bufferedWriter.flush()
        logger.info(s"Final flush performed, wrote ${finalFlushResult.rowsWritten} rows")
        synchronized { stats.rowsWritten += finalFlushResult.rowsWritten }
      } catch {
        case ex: Exception =>
          logger.error(s"Final flush failed: ${ex.getMessage}", ex)
          synchronized { stats.errorCount += 1 }
      }

      val duration = System.currentTimeMillis() - startTime
      val rate = if (duration > 0) rowsConsumed * 1000.0 / duration else 0
      logger.info(s"Consumer $id completed. Processed $rowsConsumed rows in ${duration}ms (${rate.toInt} rows/sec)")
    } catch {
      case ex: Exception =>
        logger.error(s"Consumer $id encountered an error: ${ex.getMessage}", ex)
    } finally {
      consumersDone.countDown()
    }
  }
}
