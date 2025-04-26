package tester.ratedefined

case class TestConfig(
                       queueCapacity: Int = 10000, // Producer Queue capacity
                       producerRatePerSec: Int = 5000,
                       consumerCount: Int = 5,
                       rowsPerBatch: Int = 100, // How many rows to insert in a single batch to the producer queue
                       totalRows: Int = 500_000, // Total rows to produce to the queue
                       batchSize: Int = 1000, // Batch size for BufferedWriter
                       dbHost: String = "192.168.52.194",
                       dbPort: Int = 5432,
                       dbName: String = "scaling-tests",
                       dbUser: String = "postgres",
                       dbPassword: String = "postgres"
                     )


object TestConfig {
  // Default configuration values
  val DEFAULT_QUEUE_CAPACITY: Int = 10000
  val DEFAULT_PRODUCER_RATE_PER_SEC: Int = 5000
  val DEFAULT_CONSUMER_COUNT: Int = 5
  val DEFAULT_ROWS_PER_BATCH: Int = 100
  val DEFAULT_TOTAL_ROWS_TO_PRODUCE: Int = 500_000
}