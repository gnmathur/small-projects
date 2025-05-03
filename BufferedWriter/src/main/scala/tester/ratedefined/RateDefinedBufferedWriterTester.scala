package tester.ratedefined

import org.slf4j.{Logger, LoggerFactory}
import tester.ratedefined.TestConfig._
import tester.ratedefined.async.AsyncDriver
import tester.ratedefined.sync.SyncDriver

/**
 * Modular implementation of BufferedWriterScaleTester using a producer-consumer pattern
 * with a blocking queue for better workload management
 */
object RateDefinedBufferedWriterTester extends App {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private def main(): Unit = {
    import io.prometheus.client.exporter.HTTPServer

    val mode = parseArgs(args)

    // Start HTTP server to expose Prometheus metrics on port 8080
    val server = new HTTPServer.Builder()
      .withPort(8989)
      .build()

    try {
      // Create configuration - can be loaded from properties file or command line args
      val config = TestConfig(
        queueCapacity = DEFAULT_QUEUE_CAPACITY,
        maxTimeInBufferedWriterQueue = DEFAULT_MAX_TIME_IN_BUFFERED_WRITER_QUEUE,
        producerRatePerSec = DEFAULT_PRODUCER_RATE_PER_SEC,
        consumerCount = DEFAULT_CONSUMER_COUNT,
        rowsPerBatch = DEFAULT_ROWS_PER_BATCH,
        totalRows = DEFAULT_TOTAL_ROWS_TO_PRODUCE,
        batchSizeForBufferedWriter = DEFAULT_BATCH_SIZE_FOR_BUFFERED_WRITER
      )

      logger.info(s"Starting test with config: $config")

      // Choose driver based on mode
      mode match {
        case "sync" =>
          logger.info("Running in synchronous mode")
          new SyncDriver(config).run()
        case "async" =>
          logger.info("Running in asynchronous mode")
          new AsyncDriver(config).run()
      }
      server.close()
    } catch {
      case ex: Exception =>
        logger.error(s"Test failed with error: ${ex.getMessage}", ex)
    }
  }

  /**
   * Parse command line arguments to determine sync or async mode
   * @param args Command line arguments
   * @return String representing the mode ("sync" or "async")
   */
  private def parseArgs(args: Array[String]): String = {
    if (args.isEmpty) {
      logger.info("No mode specified, defaulting to async mode")
      "async"
    } else {
      val mode = args(0).toLowerCase
      mode match {
        case "sync" | "async" =>
          mode
        case _ =>
          logger.warn(s"Invalid mode: $mode. Valid options are 'sync' or 'async'. Defaulting to async mode.")
          "async"
      }
    }
  }

  // Run the test
  main()
}
