package tester.ratedefined

import org.slf4j.{Logger, LoggerFactory}
import tester.ratedefined.TestConfig.{DEFAULT_BATCH_SIZE_FOR_BUFFERED_WRITER, DEFAULT_CONSUMER_COUNT, DEFAULT_PRODUCER_RATE_PER_SEC, DEFAULT_QUEUE_CAPACITY, DEFAULT_ROWS_PER_BATCH, DEFAULT_TOTAL_ROWS_TO_PRODUCE}

/**
 * Modular implementation of BufferedWriterScaleTester using a producer-consumer pattern
 * with a blocking queue for better workload management
 */
object RateDefinedBufferedWriterTester extends App {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Main entry point - create config and run the test
   */
  private def main(): Unit = {
    import io.prometheus.client.exporter.HTTPServer

    // Start HTTP server to expose metrics on port 8080
    val server = new HTTPServer.Builder()
      .withPort(8989)
      .build()

    try {
      // Create configuration - can be loaded from properties file or command line args
      val config = TestConfig(
        queueCapacity = DEFAULT_QUEUE_CAPACITY,
        producerRatePerSec = DEFAULT_PRODUCER_RATE_PER_SEC,
        consumerCount = DEFAULT_CONSUMER_COUNT,
        rowsPerBatch = DEFAULT_ROWS_PER_BATCH,
        totalRows = DEFAULT_TOTAL_ROWS_TO_PRODUCE,
        batchSizeForBufferedWriter = DEFAULT_BATCH_SIZE_FOR_BUFFERED_WRITER
      )

      logger.info(s"Starting test with config: $config")

      val driver = new TestDriver(config)
      driver.run()

      server.stop()
    } catch {
      case ex: Exception =>
        logger.error(s"Test failed with error: ${ex.getMessage}", ex)
    }
  }

  // Run the test
  main()
}
