import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.time.LocalDateTime
import java.util.concurrent.{CountDownLatch, Executors}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}
import scala.concurrent.duration._

/**
 * Test module for BufferedWriterPromise
 * This is an independent test that uses the BufferedWriter API
 */
object BufferedWriterTest extends App {
  // Configuration
  private val ROWS_PER_THREAD = 100 // 100K rows per thread
  private val THREAD_COUNT = 3
  private val ROWS_PER_BATCH = 10 // How many rows to insert in a single batch

  // Statistics
  @volatile private var totalRowsInserted = 0
  private val threadResults = Array.fill(THREAD_COUNT)(0)

  // For synchronization
  private val latch = new CountDownLatch(THREAD_COUNT)
  private val executor = Executors.newFixedThreadPool(THREAD_COUNT)
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(executor)

  // Create a random generator for balance values
  private val random = new Random()

  /**
   * Test function that runs a worker thread inserting rows with IDs in a specific range
   *
   * @param threadId The ID of the thread (0-based)
   * @param startId The first ID to use for this thread
   * @return Future containing the number of rows inserted
   */
  def runTestWorker(threadId: Int, startId: Int): Future[Int] = Future {
    println(s"Thread $threadId starting with ID range: $startId to ${startId + ROWS_PER_THREAD - 1}")
    val startTime = System.currentTimeMillis()

    // Insert SQL statement
    val insertSql = "INSERT INTO users (id, name, balance, created_date) VALUES (?, ?, ?, ?)"

    try {
      var rowsInserted = 0

      // Process in smaller batches
      for (batchStart <- startId until (startId + ROWS_PER_THREAD) by ROWS_PER_BATCH) {
        val batchEnd = Math.min(batchStart + ROWS_PER_BATCH, startId + ROWS_PER_THREAD)
        val rows = (batchStart until batchEnd).map { id =>
          val balance = 1000.0 + random.nextDouble() * 9000.0
          val name = s"User-${threadId}-${id}"
          Seq(id, name, balance, LocalDateTime.now())
        }

        try {
          // Call the BufferedWriter API
          val insertFuture = BufferedWriterPromise.insert(insertSql, rows)

          // Wait for this batch to complete before continuing
          val result = Await.result(insertFuture, 1.minute)
          rowsInserted += rows.size
          if (rowsInserted % 10000 == 0) {
            println(s"Thread $threadId progress: $rowsInserted rows inserted")
          }
        } catch {
          case ex: Exception =>
            println(s"Thread $threadId batch insert failed: ${ex.getMessage}")
            if (ex.getMessage == null) {
              ex.printStackTrace()
            }
          // Continue with next batch rather than failing the entire thread
        }
      }

      val duration = System.currentTimeMillis() - startTime
      println(s"Thread $threadId completed. Inserted $rowsInserted rows in ${duration}ms (${rowsInserted * 1000.0 / duration} rows/sec)")

      // Store result and count down latch
      threadResults(threadId) = rowsInserted
      latch.countDown()

      rowsInserted
    } catch {
      case ex: Exception =>
        println(s"Thread $threadId failed with error: ${ex.getMessage}")
        if (ex.getMessage == null) {
          ex.printStackTrace()
        }
        latch.countDown()
        throw ex
    }
  }

  /**
   * Main test method that creates and runs all test threads
   */
  def runTest(): Unit = {
    val startTime = System.currentTimeMillis()
    println(s"Starting test with $THREAD_COUNT threads, each inserting $ROWS_PER_THREAD rows")

    // Test database connection first
    testDatabaseConnection()

    // Create DDL if needed (this would happen outside the test in real scenarios)
    createTestTable()

    // Check BufferedWriter is accessible
    try {
      val testClass = Class.forName("BufferedWriterPromise$")
      println(s"Successfully loaded BufferedWriterPromise: ${testClass.getName}")
    } catch {
      case ex: ClassNotFoundException =>
        println(s"ERROR: Could not find BufferedWriterPromise class. Make sure it's in the classpath: ${ex.getMessage}")
        return
    }

    // Launch all worker threads
    val futures = (0 until THREAD_COUNT).map { threadId =>
      val startId = threadId * ROWS_PER_THREAD
      runTestWorker(threadId, startId)
    }

    // Wait for all worker threads to complete
    futures.foreach { future =>
      future.onComplete {
        case Success(count) =>
          synchronized { totalRowsInserted += count }
        case Failure(ex) =>
          println(s"A thread failed: ${ex.getMessage}")
          if (ex.getMessage == null) {
            ex.printStackTrace()
          }
      }
    }

    // Wait for all threads to finish
    println("Waiting for all threads to complete...")
    latch.await()

    // Print results
    val totalDuration = System.currentTimeMillis() - startTime
    println("\n----- TEST RESULTS -----")
    println(s"Total rows inserted: $totalRowsInserted")
    println(s"Total duration: ${totalDuration}ms")
    println(s"Overall throughput: ${totalRowsInserted * 1000.0 / totalDuration} rows/sec")

    for (i <- 0 until THREAD_COUNT) {
      println(s"Thread $i inserted ${threadResults(i)} rows")
    }

    // Clean up
    executor.shutdown()
  }

  /**
   * Test database connection to diagnose issues early
   */
  private def testDatabaseConnection(): Unit = {
    var conn: Connection = null
    try {
      Class.forName("org.postgresql.Driver")
      println("PostgreSQL JDBC Driver loaded successfully")

      conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres")
      println("Database connection test successful")
    } catch {
      case e: ClassNotFoundException =>
        println("ERROR: PostgreSQL JDBC Driver not found")
        e.printStackTrace()
        throw e
      case e: SQLException =>
        println(s"ERROR: Failed to connect to database: ${e.getMessage}")
        e.printStackTrace()
        throw e
    } finally {
      try {
        if (conn != null) conn.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
  }

  /**
   * Utility method to create the test table if it doesn't exist
   */
  private def createTestTable(): Unit = {
    var conn: Connection = null
    var stmt: Statement = null

    try {
      conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres")
      stmt = conn.createStatement()

      // Drop table if exists
      stmt.execute("DROP TABLE IF EXISTS users")

      // Create table
      stmt.execute("""
        CREATE TABLE users (
          id INTEGER PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          balance NUMERIC(10, 2) NOT NULL,
          created_date TIMESTAMP NOT NULL
        )
      """)

      // Add some test data to verify we can read/write
      stmt.execute("INSERT INTO users VALUES (0, 'TestUser', 100.0, now())")

      // Verify the connection works by reading back the test data
      val rs = stmt.executeQuery("SELECT * FROM users WHERE id = 0")
      if (rs.next()) {
        println(s"Successfully read test record: id=${rs.getInt("id")}, name=${rs.getString("name")}")
      } else {
        println("WARNING: Could not read test record")
      }
      rs.close()

      println("Test table created and verified successfully")
    } catch {
      case ex: Exception =>
        println(s"Failed to create test table: ${ex.getMessage}")
        ex.printStackTrace()
        throw ex
    } finally {
      try {
        if (stmt != null) stmt.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }

      try {
        if (conn != null) conn.close()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  // Run the test
  try {
    runTest()
  } catch {
    case ex: Exception => println(s"Test failed with error: ${ex.getMessage}")
  } finally {
    // Make sure everything is cleaned up
    if (!executor.isShutdown) {
      executor.shutdownNow()
    }
  }
}