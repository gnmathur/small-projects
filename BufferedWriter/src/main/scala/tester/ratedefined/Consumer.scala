package tester.ratedefined

import lib.BufferedWriter

import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, TimeUnit}
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * The Consumer class - takes batches from the queue and inserts them using BufferedWriter
 */
class Consumer(
                id: Int,
                queue: ArrayBlockingQueue[List[UserRow]],
                bufferedWriter: BufferedWriter,
                stats: Stats,
                producerDone: CountDownLatch,
                consumersDone: CountDownLatch
              )(implicit ec: ExecutionContext) extends Runnable {

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

              // Call the BufferedWriter API
              val insertFuture = bufferedWriter.insert(insertSql, rows)

              // Track the future for result reporting
              insertFuture.onComplete {
                case Success(_) =>
                  logger.debug(s"Inserted $insertFuture")
                  synchronized { stats.futuresSucceeded += 1 }
                case Failure(ex) =>
                  synchronized { stats.futuresFailed += 1 }
                  logger.error(s"Insert failed: ${ex.getMessage}", ex)
              }

              // Update statistics
              rowsConsumed += batch.size
              synchronized {
                stats.rowsConsumed += batch.size
                stats.batchesProcessed += 1
              }

              if (rowsConsumed % 10000 == 0) {
                val elapsed = System.currentTimeMillis() - startTime
                val rate = if (elapsed > 0) rowsConsumed * 1000.0 / elapsed else 0
                logger.info(s"Consumer $id progress: $rowsConsumed rows, rate: ${rate.toInt} rows/sec")
              }
            } catch {
              case ex: Exception =>
                logger.error(s"Consumer $id failed to process batch: ${ex.getMessage}", ex)
            }
          }
        }
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

