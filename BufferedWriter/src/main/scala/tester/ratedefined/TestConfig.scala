package tester.ratedefined

case class TestConfig(
                       queueCapacity: Int, // Producer Queue capacity
                       producerRatePerSec: Int,
                       consumerCount: Int,
                       rowsPerBatch: Int, // How many rows to insert in a single batch to the producer queue
                       totalRows: Int, // Total rows to produce to the queue
                       batchSizeForBufferedWriter: Int, // Batch size for BufferedWriter
                       maxTimeInBufferedWriterQueue: Int, // Max time in BufferedWriter queue
                       dbHost: String = "192.168.52.194",
                       dbPort: Int = 5432,
                       dbName: String = "postgres",
                       dbUser: String = "postgres",
                       dbPassword: String = "postgres"
                     )


object TestConfig {
  // Producer queue capacity
  val DEFAULT_QUEUE_CAPACITY: Int = 1000
  // Producer produces rows at this rate - this is the high-level rate that the tester is testing the BufferedWriter at
  val DEFAULT_PRODUCER_RATE_PER_SEC: Int = 10000
  // Number of consumer threads to consume the rows from the producer queue. A consumer simulates multiple application
  // threads writing to the database. This needs to be set to a realistic number of threads that the application will use
  val DEFAULT_CONSUMER_COUNT: Int = 5
  // Number of rows to produce to the producer queue in a single batch. This is the number of rows that the producer
  val DEFAULT_ROWS_PER_BATCH: Int = 100
  // Number of rows to produce in total. This is the number of rows that the producer will produce to the queue
  val DEFAULT_TOTAL_ROWS_TO_PRODUCE: Int = 10_000_000
  // The batch size for the BufferedWriter. This is the number of rows that the BufferedWriter will write to the database
  // in a single batch.
  val DEFAULT_BATCH_SIZE_FOR_BUFFERED_WRITER = 500
  val DEFAULT_MAX_TIME_IN_BUFFERED_WRITER_QUEUE = 5000 // 5 second
}