package tester.ratedefined

import lib.BufferedWriter

import java.util.concurrent.{ArrayBlockingQueue, CountDownLatch, Executors, TimeUnit}
import scala.concurrent.ExecutionContext

/**
 * Main test driver that sets up and coordinates the producer and consumers
 */
class TestDriver(config: TestConfig) {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  private val stats = new Stats()
  private val queue = new ArrayBlockingQueue[List[UserRow]](config.queueCapacity)
  private val producerDone = new CountDownLatch(1)
  private val consumersDone = new CountDownLatch(config.consumerCount)

  private val producerExecutor = Executors.newSingleThreadExecutor()
  private val consumerExecutor = Executors.newFixedThreadPool(config.consumerCount)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool())

  private val bufferedWriter = new BufferedWriter(
    config.batchSizeForBufferedWriter,
    config.dbHost,
    config.dbPort,
    config.dbName,
    config.dbUser,
    config.dbPassword
  )

  def run(): Unit = {
    val startTime = System.currentTimeMillis()
    logger.info(s"Starting test with producer rate: ${config.producerRatePerSec} rows/sec, " +
      s"consumers: ${config.consumerCount}, queue capacity: ${config.queueCapacity}")

    // Start the producer
    val producer = new Producer(
      queue,
      stats,
      config.producerRatePerSec,
      config.totalRows,
      config.rowsPerBatch
    )
    producerExecutor.submit(producer)

    // Start the consumers
    for (i <- 0 until config.consumerCount) {
      val consumer = new Consumer(i, queue, bufferedWriter, stats, producerDone, consumersDone)
      consumerExecutor.submit(consumer)
    }

    // Start monitoring thread
    monitorProgress(startTime)

    // Wait for producer to finish
    producerExecutor.shutdown()
    producerExecutor.awaitTermination(1, TimeUnit.HOURS)
    producerDone.countDown()

    logger.info("Producer finished, waiting for consumers to complete...")

    // Wait for consumers to finish
    consumersDone.await(5, TimeUnit.MINUTES)
    consumerExecutor.shutdown()

    // Print final results
    printResults(startTime)

    // Wait for a moment for all futures to complete
    logger.info("Waiting 20 seconds for pending futures to complete...")
    Thread.sleep(20000)

    // Print final statistics
    logger.info(s"Final count - Futures completed successfully: ${stats.futuresSucceeded}")
    logger.info(s"Final count - Futures failed: ${stats.futuresFailed}")

    // Cleanup
    producerExecutor.shutdown()
    consumerExecutor.shutdown()
    bufferedWriter.shutdown()
  }

  private def monitorProgress(startTime: Long): Unit = {
    val monitorThread = new Thread(() => {
      try {
        while (!producerDone.await(10, TimeUnit.SECONDS) || !consumersDone.await(0, TimeUnit.MILLISECONDS)) {
          val elapsed = System.currentTimeMillis() - startTime
          val producerRate = if (elapsed > 0) stats.rowsProduced * 1000.0 / elapsed else 0
          val consumerRate = if (elapsed > 0) stats.rowsConsumed * 1000.0 / elapsed else 0

          logger.info(s"Progress: " +
            s"Queue size: ${queue.size()}, " +
            s"Produced: ${stats.rowsProduced} (${producerRate.toInt} rows/sec), " +
            s"Consumed: ${stats.rowsConsumed} (${consumerRate.toInt} rows/sec), " +
            s"Batches: ${stats.batchesProcessed}, " +
            s"Futures success/fail: ${stats.futuresSucceeded}/${stats.futuresFailed}")
        }
      } catch {
        case _: InterruptedException => // Ignore
      }
    })
    monitorThread.setDaemon(true)
    monitorThread.start()
  }

  private def printResults(startTime: Long): Unit = {
    val totalDuration = System.currentTimeMillis() - startTime
    logger.info("\n----- TEST RESULTS -----")
    logger.info(s"Total rows produced: ${stats.rowsProduced}")
    logger.info(s"Total rows consumed: ${stats.rowsConsumed}")
    logger.info(s"Total batches processed: ${stats.batchesProcessed}")
    logger.info(s"Futures succeeded: ${stats.futuresSucceeded}")
    logger.info(s"Futures failed: ${stats.futuresFailed}")
    logger.info(s"Total duration: ${totalDuration}ms")

    val producerRate = if (totalDuration > 0) stats.rowsProduced * 1000.0 / totalDuration else 0
    val consumerRate = if (totalDuration > 0) stats.rowsConsumed * 1000.0 / totalDuration else 0
    logger.info(s"Overall producer throughput: ${producerRate.toInt} rows/sec")
    logger.info(s"Overall consumer throughput: ${consumerRate.toInt} rows/sec")
  }
}
