package tester.ratedefined

case class TestConfig(
                       queueCapacity: Int, // Producer Queue capacity
                       producerRatePerSec: Int,
                       consumerCount: Int,
                       rowsPerBatch: Int, // How many rows to insert in a single batch to the producer queue
                       totalRows: Int, // Total rows to produce to the queue
                       batchSizeForBufferedWriter: Int, // Batch size for BufferedWriter
                       dbHost: String = "192.168.52.194",
                       dbPort: Int = 5432,
                       dbName: String = "postgres",
                       dbUser: String = "postgres",
                       dbPassword: String = "postgres"
                     )


object TestConfig {
  // Default configuration values
  val DEFAULT_QUEUE_CAPACITY: Int = 1000
  val DEFAULT_PRODUCER_RATE_PER_SEC: Int = 10000
  val DEFAULT_CONSUMER_COUNT: Int = 5
  val DEFAULT_ROWS_PER_BATCH: Int = 100
  val DEFAULT_TOTAL_ROWS_TO_PRODUCE: Int = 100_000
  val DEFAULT_BATCH_SIZE_FOR_BUFFERED_WRITER = 500
}