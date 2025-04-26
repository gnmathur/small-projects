package tester.ratedefined

import java.time.LocalDateTime
import java.util.concurrent.{ArrayBlockingQueue, TimeUnit}
import scala.util.Random

/**
 * The Producer class - generates rows at a specified rate and adds them to the queue
 */
class Producer(
                queue: ArrayBlockingQueue[List[UserRow]],
                stats: Stats,
                ratePerSec: Int,
                totalRows: Int,
                batchSize: Int
              ) extends Runnable {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)
  // Create a random generator for data values
  private val random = new Random()


  override def run(): Unit = {
    val startTime = System.currentTimeMillis()
    logger.info(s"Producer starting: target rate $ratePerSec rows/sec, total rows: $totalRows")

    try {
      var rowsProduced = 0
      val intervalMs = 1000.0 / (ratePerSec.toDouble / batchSize.toDouble)

      while (rowsProduced < totalRows) {
        val batchStart = rowsProduced
        val batchEnd = math.min(batchStart + batchSize, totalRows)
        val batch = (batchStart until batchEnd).map { id =>
          val balance = 1000.0 + random.nextDouble() * 9000.0
          val name = s"User-$id"
          UserRow(id, name, balance, LocalDateTime.now())
        }.toList

        val batchTime = System.currentTimeMillis()

        // Add batch to queue, waiting if necessary
        if (!queue.offer(batch, 30, TimeUnit.SECONDS)) {
          logger.warn("Queue full for 30 seconds, skipping batch")
        } else {
          rowsProduced += batch.size
          stats.rowsProduced = rowsProduced

          if (rowsProduced % 10000 == 0) {
            val elapsed = System.currentTimeMillis() - startTime
            val rate = if (elapsed > 0) rowsProduced * 1000.0 / elapsed else 0
            logger.info(s"Producer progress: $rowsProduced rows, rate: ${rate.toInt} rows/sec")
          }
        }

        // Sleep to maintain the desired rate
        val processingTime = System.currentTimeMillis() - batchTime
        val sleepTime = math.max(0, intervalMs.toLong - processingTime)
        if (sleepTime > 0) {
          Thread.sleep(sleepTime)
        }
      }

      logger.info(s"Producer finished: $rowsProduced rows produced")
    } catch {
      case ex: Exception =>
        logger.error(s"Producer failed: ${ex.getMessage}", ex)
    }
  }
}
