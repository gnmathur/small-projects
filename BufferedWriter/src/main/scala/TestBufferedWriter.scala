import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
import java.time.LocalDateTime
import java.util.concurrent.{CountDownLatch, ExecutorService, Executors, ScheduledExecutorService, ThreadLocalRandom, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object TestBufferedWriter extends App {
  // Create the users table if it doesn't exist
  createUsersTable()

  // Number of threads
  val NUM_THREADS = 10

  // Records per thread
  val MAX_RECORDS_PER_THREAD = 100000

  // Records per insert operation
  val RECORDS_PER_INSERT = 550

  // Wait time between inserts (ms)
  val MIN_WAIT_TIME = 10
  val MAX_WAIT_TIME = 20

  // Countdown latch to wait for all threads to complete
  val latch = new CountDownLatch(NUM_THREADS)

  // Counters for statistics
  val totalInserts = new AtomicInteger(0)
  val totalRecordsInserted = new AtomicLong(0)
  val successfulInserts = new AtomicInteger(0)
  val failedInserts = new AtomicInteger(0)
  val startTime = System.currentTimeMillis()

  // ExecutionContext for callbacks
  implicit val ec: ExecutionContext = ExecutionContext.global

  println(s"Starting test with $NUM_THREADS threads, each inserting $MAX_RECORDS_PER_THREAD records")

  // Start the worker threads
  for (threadId <- 0 until NUM_THREADS) {
    val thread = new Thread(new InsertWorker(threadId))
    thread.setName(s"Worker-$threadId")
    thread.start()
  }

  // Wait for all threads to complete
  latch.await()

  val endTime = System.currentTimeMillis()
  val totalTime = (endTime - startTime) / 1000.0
  val totalRecords = totalRecordsInserted.get()

  println(s"Test completed in $totalTime seconds")
  println(s"Total records inserted: $totalRecords")
  println(s"Average insert rate: ${totalRecords / totalTime} records/second")
  println(s"Total insert operations: ${totalInserts.get()}")
  println(s"Successful insert operations: ${successfulInserts.get()}")
  println(s"Failed insert operations: ${failedInserts.get()}")

  // Shutdown the BufferedWriter
  // BufferedWriterPromise.shutdown()

  // Worker class for inserting records
  class InsertWorker(threadId: Int) extends Runnable {
    // Track progress for this thread
    private val recordsSubmitted = new AtomicInteger(0)
    private val recordsCompleted = new AtomicInteger(0)

    // This latch ensures the thread doesn't exit until all its submissions are completed
    private val threadCompletionLatch = new CountDownLatch(1)

    override def run(): Unit = {
      try {
        println(s"Thread $threadId starting")

        // Calculate ID range for this thread
        val startId = threadId * MAX_RECORDS_PER_THREAD + 1
        val endId = (threadId + 1) * MAX_RECORDS_PER_THREAD

        // SQL statement for inserts
        val insertSql = "INSERT INTO users (id, name, balance, created_date) VALUES (?, ?, ?, ?)"

        // Random generator for this thread
        val random = ThreadLocalRandom.current()

        // Submit all batches without waiting for completion
        while (recordsSubmitted.get() < MAX_RECORDS_PER_THREAD) {
          // Calculate how many records to insert in this batch
          val recordsRemaining = MAX_RECORDS_PER_THREAD - recordsSubmitted.get()
          val batchSize = Math.min(RECORDS_PER_INSERT, recordsRemaining)

          // Create the batch of records
          val currentSubmittedCount = recordsSubmitted.get()
          val batch = (0 until batchSize).map { i =>
            val id = startId + currentSubmittedCount + i
            val name = getRandomName(random)
            val balance = getRandomBalance(random)
            Seq(id, name, balance, LocalDateTime.now())
          }

          // Update the count of submitted records
          recordsSubmitted.addAndGet(batchSize)
          totalInserts.incrementAndGet()

          // Submit the batch with callbacks
          val future = BufferedWriterPromise2.insert(insertSql, batch)
          future.onComplete {
            case Success(result) => {
              val currentCompleted = recordsCompleted.addAndGet(batchSize)
              totalRecordsInserted.addAndGet(batchSize)
              successfulInserts.incrementAndGet()

              if (currentCompleted % 10000 == 0 || currentCompleted == MAX_RECORDS_PER_THREAD) {
                println(s"Thread $threadId: Completed $currentCompleted records")
              }

              // If all records are completed, signal this thread is done
              if (currentCompleted >= MAX_RECORDS_PER_THREAD) {
                threadCompletionLatch.countDown()
              }
            }
            case Failure(ex) => {
              failedInserts.incrementAndGet()
              println(s"Thread $threadId: Failed to insert batch: ${ex.getMessage}")

              // In a real application, you might implement retry logic here
              // For simplicity, we'll still consider these records "completed" to avoid hanging
              val currentCompleted = recordsCompleted.addAndGet(batchSize)
              if (currentCompleted >= MAX_RECORDS_PER_THREAD) {
                threadCompletionLatch.countDown()
              }
            }
          }

          // Random wait between inserts
          val waitTime = random.nextInt(MIN_WAIT_TIME, MAX_WAIT_TIME + 1)
          Thread.sleep(waitTime)

          // Periodically report progress
          if (recordsSubmitted.get() % 10000 == 0) {
            println(s"Thread $threadId: Submitted ${recordsSubmitted.get()} records")
          }
        }

        // Wait for all submitted records to complete
        println(s"Thread $threadId: Submitted all ${recordsSubmitted.get()} records, waiting for completion")
        threadCompletionLatch.await()
        println(s"Thread $threadId: All records completed")
      } catch {
        case ex: Exception =>
          println(s"Thread $threadId failed with error: ${ex.getMessage}")
      } finally {
        latch.countDown()
      }
    }

    // Generate a random name
    private def getRandomName(random: ThreadLocalRandom): String = {
      val firstNames = Array("James", "John", "Robert", "Michael", "William", "David",
        "Richard", "Joseph", "Thomas", "Charles", "Mary", "Patricia",
        "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica",
        "Sarah", "Karen")

      val lastNames = Array("Smith", "Johnson", "Williams", "Jones", "Brown", "Davis",
        "Miller", "Wilson", "Moore", "Taylor", "Anderson", "Thomas",
        "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia",
        "Martinez", "Robinson")

      val firstName = firstNames(random.nextInt(firstNames.length))
      val lastName = lastNames(random.nextInt(lastNames.length))

      s"$firstName $lastName"
    }

    // Generate a random balance
    private def getRandomBalance(random: ThreadLocalRandom): Double = {
      // Random balance between 100 and 10000 with two decimal places
      Math.round(random.nextDouble(100.0, 10000.0) * 100) / 100.0
    }
  }

  // Create the users table if it doesn't exist
  def createUsersTable(): Unit = {
    var conn: Connection = null
    var stmt: PreparedStatement = null

    try {
      conn = getConnection

      // Drop table if exists (for testing purposes)
      stmt = conn.prepareStatement("DROP TABLE IF EXISTS users")
      stmt.execute()
      stmt.close()

      // Create the table
      val createTableSql = """
        CREATE TABLE users (
          id INTEGER PRIMARY KEY,
          name VARCHAR(100) NOT NULL,
          balance DECIMAL(10, 2) NOT NULL,
          created_date TIMESTAMP NOT NULL
        )
      """

      stmt = conn.prepareStatement(createTableSql)
      stmt.execute()

      println("Users table created successfully")
    } catch {
      case ex: SQLException =>
        println(s"Error creating users table: ${ex.getMessage}")
        throw ex
    } finally {
      if (stmt != null) Try(stmt.close())
      if (conn != null) Try(conn.close())
    }
  }

  // Get database connection
  def getConnection: Connection = {
    Class.forName("org.postgresql.Driver")
    DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres")
  }
}
