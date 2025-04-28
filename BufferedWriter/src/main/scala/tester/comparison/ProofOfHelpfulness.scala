package tester.comparison

import lib.AsyncBufferedWriter

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Random, Success}
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException, Timestamp}
import java.math.BigDecimal
import java.time.{Instant, LocalDateTime} // Use LocalDateTime for the InsertResult

// --- Database Configuration (Used by BlockingJdbcDbClient and DbSetup) ---
// NOTE: Your BufferedWriter uses its OWN internal config from its constructor
object TestDbConfig {
  val url = "jdbc:postgresql://192.168.52.194:5432/postgres" // Replace with your DB details
  val user = "postgres"                                  // Replace with your DB user
  val password = "postgres"                          // Replace with your DB password
  val host = "192.168.52.194"
  val port = 5432
  val dbName = "postgres"

  // Ensure the JDBC driver is loaded
  Class.forName("org.postgresql.Driver")

  def getConnection: Connection = DriverManager.getConnection(url, user, password)
}

// --- Blocking Database Client (for the synchronous baseline test) ---
// MODIFIED: This client now reuses a single connection.
class BlockingJdbcDbClient {
  private var connection: Connection = _

  /** Opens the database connection. Must be called before insert. */
  def connect(): Unit = {
    if (connection == null || connection.isClosed) {
      connection = TestDbConfig.getConnection
      // Optional: Set auto-commit to false once here if you want to manage transactions externally
      // For this test, we'll manage transactions per insert call inside the method
    }
  }

  /** Closes the database connection. Should be called after all inserts are done. */
  def disconnect(): Unit = {
    if (connection != null) {
      try {
        if (!connection.getAutoCommit) {
          // Attempt to commit any pending work before closing
          connection.commit()
        }
        connection.close()
        println("Blocking client connection closed.")
      } catch {
        case ex: SQLException => println(s"Error closing blocking connection: ${ex.getMessage}")
      } finally {
        connection = null // Ensure it's nulled out
      }
    }
  }

  /**
   * Performs a blocking insert of a sequence of rows using JDBC batching.
   * Reuses the connection managed by this client instance.
   * This method blocks the calling thread until the DB operation completes.
   */
  def insert(sql: String, rows: Seq[Seq[Any]]): Int = { // Return row count
    if (connection == null || connection.isClosed) {
      throw new IllegalStateException("Connection is not open. Call connect() first.")
    }
    if (rows.isEmpty) return 0

    var pstmt: PreparedStatement = null
    var insertedCount = 0
    try {
      // Use the existing connection
      connection.setAutoCommit(false) // Start transaction for this insert batch

      pstmt = connection.prepareStatement(sql)

      for (row <- rows) {
        if (row.size != 4) throw new IllegalArgumentException(s"Expected 4 columns, got ${row.size}")
        // Map Seq[Any] to JDBC types for the users_test table
        pstmt.setLong(1, row(0).asInstanceOf[Long])              // id: bigint
        pstmt.setString(2, row(1).asInstanceOf[String])         // name: varchar
        pstmt.setBigDecimal(3, row(2).asInstanceOf[BigDecimal]) // balance: numeric
        // created_date: timestamp with time zone (accept Instant and convert)
        row(3) match {
          case inst: Instant => pstmt.setTimestamp(4, Timestamp.from(inst))
          case ts: Timestamp => pstmt.setTimestamp(4, ts)
          case other => throw new IllegalArgumentException(s"Unsupported type for created_date: ${other.getClass}")
        }

        pstmt.addBatch() // Add to JDBC batch
      }

      val batchResult: Array[Int] = pstmt.executeBatch() // Execute the batch (BLOCKING)
      connection.commit() // Commit the transaction (BLOCKING)
      insertedCount = batchResult.sum // Sum of rows affected by batch statements

      insertedCount

    } catch {
      case ex: SQLException =>
        if (connection != null) {
          try {
            connection.rollback()
          } catch {
            case rbex: SQLException => println(s"Sync Rollback failed: ${rbex.getMessage}")
          }
        }
        println(s"SYNC JDBC Insert failed: ${ex.getMessage}")
        // Rethrow or return a failure indicator. We'll print and return 0 for simplicity in this test.
        0
      case ex: Throwable =>
        println(s"Unexpected error during SYNC insert: ${ex.getMessage}")
        0
    } finally {
      // Do NOT close the connection here
      if (pstmt != null) { try { pstmt.close() } catch { case _: SQLException => } }
    }
  }
}


// --- Database Setup/Teardown (Used by Test) ---
object TestDbSetup {
  private val createTableSql = """
      CREATE TABLE IF NOT EXISTS public.users_test (
          id bigint PRIMARY KEY,
          name character varying(255) NOT NULL,
          balance numeric(18,2) NOT NULL DEFAULT 0.00,
          created_date timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP
      );
    """
  private val truncateTableSql = "TRUNCATE TABLE public.users_test;"
  private val dropTableSql = "DROP TABLE IF EXISTS public.users_test;"


  def setup(): Unit = {
    var conn: Connection = null
    try {
      conn = TestDbConfig.getConnection
      val stmt = conn.createStatement()
      stmt.execute(createTableSql)
      println("Ensured 'users_test' table exists.")
      stmt.close()
    } catch {
      case ex: SQLException => println(s"DB Setup failed: ${ex.getMessage}"); throw ex
    } finally {
      if (conn != null) try { conn.close() } catch { case _: SQLException => }
    }
  }

  def cleanup(): Unit = {
    var conn: Connection = null
    try {
      conn = TestDbConfig.getConnection
      val stmt = conn.createStatement()
      stmt.execute(dropTableSql)
      println("Dropped 'users_test' table.")
      stmt.close()
    } catch {
      case ex: SQLException => println(s"DB Cleanup failed: ${ex.getMessage}"); throw ex
    } finally {
      if (conn != null) try { conn.close() } catch { case _: SQLException => }
    }
  }

  def truncate(): Unit = {
    var conn: Connection = null
    try {
      conn = TestDbConfig.getConnection
      val stmt = conn.createStatement()
      stmt.execute(truncateTableSql)
      println("Truncated 'users_test' table.")
      stmt.close()
    } catch {
      case ex: SQLException => println(s"DB Truncate failed: ${ex.getMessage}"); throw ex
    } finally {
      if (conn != null) try { conn.close() } catch { case _: SQLException => }
    }
  }
}


// --- The Test Implementation ---

object BufferedWriterProofOfHelpfulnessPG {

  // Execution context for the test code's Future operations (e.g., awaiting flush)
  implicit val ec: ExecutionContext = ExecutionContext.global

  def main(args: Array[String]): Unit = {
    // --- Test Configuration ---
    val totalIndividualRowsToAttempt = 10000 // Total number of individual rows we will try to insert
    // The `insert` API takes Seq[Seq[Any]]. Let's simulate sending 1 to 5 rows per call.
    val minRowsPerInsertCall = 1
    val maxRowsPerInsertCall = 5
    val totalInsertCalls = 2000 // Number of times we call insert/blocking insert (e.g., 10000 rows / avg 5 rows/call = 2000 calls)

    val bufferBatchSize = 100 // BufferedWriter batch size (total individual rows)

    val sqlTemplate = "INSERT INTO users_test (id, name, balance, created_date) VALUES (?, ?, ?, ?)" // SQL for users_test table

    // --- DB Setup ---
    TestDbSetup.setup() // Ensure table exists

    println(s"--- Testing Database Writer Performance (PostgreSQL) ---")
    println(s"Total individual rows to attempt inserting: $totalIndividualRowsToAttempt (approx based on calls * avg rows/call)")
    println(s"Rows per insert() call: min=$minRowsPerInsertCall, max=$maxRowsPerInsertCall")
    println(s"Total insert() calls simulation: $totalInsertCalls") // This is the number of times we call the insert method
    println(s"BufferedWriter batch size (individual rows): $bufferBatchSize")
    println("-" * 30)


    // --- Scenario 1: Synchronous, Blocking Inserts (Connection Reused) ---
    TestDbSetup.truncate() // Clear table before test

    println("Scenario 1: Synchronous, Blocking Inserts (Connection Reused)")
    val blockingDbClient = new BlockingJdbcDbClient()
    blockingDbClient.connect() // Open connection once

    val startTimeSync = System.currentTimeMillis()

    var syncInsertedRowsCount = 0
    var currentIdSync: Long = 1L // Start ID for sync test
    var syncAttemptedRows = 0

    (1 to totalInsertCalls).foreach { i =>
      val numRowsForCall = Random.nextInt(maxRowsPerInsertCall - minRowsPerInsertCall + 1) + minRowsPerInsertCall
      val rowsForCall: Seq[Seq[Any]] = (1 to numRowsForCall).map { j =>
        // Generate data for a single row in the users_test table
        val id = currentIdSync
        val name = s"sync_user_$id"
        val balance = BigDecimal.valueOf(1000.00 + id * 0.5)
        val createdDate = Instant.now() // Use Instant
        currentIdSync += 1
        Seq(id, name, balance, createdDate)
      }
      syncAttemptedRows += rowsForCall.map(_.size).sum


      if (rowsForCall.nonEmpty) {
        // This call BLOCKS until the database finishes the insert, but reuses the connection
        val insertedCount = blockingDbClient.insert(sqlTemplate, rowsForCall)
        syncInsertedRowsCount += insertedCount
      }
    }

    blockingDbClient.disconnect() // Close connection after the loop

    val endTimeSync = System.currentTimeMillis()
    val durationSync = endTimeSync - startTimeSync

    println(s"Scenario 1 Complete.")
    println(s"  Total individual rows attempted: $syncAttemptedRows")
    println(s"  Successful rows reported by client: $syncInsertedRowsCount")
    println(s"  Total time: $durationSync ms")
    if (durationSync > 0) {
      println(s"  Throughput: ${syncInsertedRowsCount.toDouble * 1000 / durationSync} rows/sec")
    }
    println("-" * 30)


    // --- Scenario 2: Using BufferedWriter (Asynchronous, Buffered) ---
    TestDbSetup.truncate() // Clear table before test

    println("Scenario 2: Using BufferedWriter (Asynchronous, Buffered)")

    // Instantiate *your* BufferedWriter class
    val writer = new AsyncBufferedWriter(
      batchSize = bufferBatchSize,
      pgHost = TestDbConfig.host,
      pgPort = TestDbConfig.port,
      pgDb = TestDbConfig.dbName,
      pgUser = TestDbConfig.user,
      pgPassword = TestDbConfig.password
    )

    val startTimeBuffered = System.currentTimeMillis()

    var currentIdBuffered: Long = 1L // Start ID for buffered test
    var totalBufferedRowsAttempted = 0

    var allFutures = List.empty[Future[Any]] // Optional: Track futures for completion

    (1 to totalInsertCalls).foreach { i =>
      val numRowsForCall = Random.nextInt(maxRowsPerInsertCall - minRowsPerInsertCall + 1) + minRowsPerInsertCall
      val rowsForCall: Seq[Seq[Any]] = (1 to numRowsForCall).map { j =>
        // Generate data for a single row in the users_test table
        val id = currentIdBuffered
        val name = s"buffered_user_$id"
        val balance = BigDecimal.valueOf(2000.00 + id * 0.75)
        val createdDate =  LocalDateTime.now()
        currentIdBuffered += 1
        Seq(id, name, balance, createdDate)
      }

      totalBufferedRowsAttempted += rowsForCall.map(_.size).sum


      if (rowsForCall.nonEmpty) {
        // Calling insert returns a Future immediately - the thread is NOT blocked
        val futureResult = writer.insert(sqlTemplate, rowsForCall)
        allFutures = futureResult :: allFutures // Optional: Track the future for later completion check

        // Optional: Attach callbacks to see results per insert() call (i.e., per batch completion)
        // futureResult.onComplete {
        //   case Success(result) => // println(s" -> Future for buffered insert ${i} completed: $result")
        //   case Failure(ex) => println(s" -> Future for buffered insert ${i} failed: ${ex.getMessage}")
        // }
      }
    }

    // The loop finishes quickly because insert() is non-blocking.
    val endTimeLoopBuffered = System.currentTimeMillis()
    println(s"All ${totalInsertCalls} insert() calls submitted in: ${endTimeLoopBuffered - startTimeBuffered} ms (Non-blocking submission time)")

    // Now, flush any remaining buffered data and wait for all batches to complete.
    // writer.shutdown() internally calls flush and waits.
    writer.shutdown() // Shutdown flushes and waits for batches

    // wait for all futures to complete

    val allFuturesResult = Future.sequence(allFutures)
    allFuturesResult.onComplete {
      case Success(results) =>
        println(s"All buffered insert futures completed successfully.")
      case Failure(ex) =>
        println(s"Buffered insert futures failed: ${ex.getMessage}")
    }
    Await.result(allFuturesResult, scala.concurrent.duration.Duration.Inf) // Wait for all futures to complete

    val endTimeBuffered = System.currentTimeMillis()
    val durationBuffered = endTimeBuffered - startTimeBuffered

    // Note: With InsertResult only having `writtenAt`, we can't easily sum
    // successful/failed rows from the results themselves without the
    // BufferedWriter's internal logic. We'll rely on the total rows attempted
    // vs checking the DB count after the test if needed.
    // For this proof, the primary metric is the total time vs attempted rows.

    println(s"Scenario 2 Complete.")
    println(s"  Total individual rows attempted (approx): $totalBufferedRowsAttempted")
    // We cannot easily report success/failure counts here without assuming internal
    // details of the BufferedWriter's batch results mapping to InsertResult.
    println(s"  Total time (including buffering and async writes): $durationBuffered ms")
    if (durationBuffered > 0) {
      println(s"  Throughput: ${totalBufferedRowsAttempted.toDouble * 1000 / durationBuffered} rows/sec")
    }
    println("-" * 30)

    // Compare and conclude
    println("\n--- Comparison ---")
    println(f"Synchronous (Blocking) Time: ${durationSync} ms")
    println(f"Buffered (Async) Time:    ${durationBuffered} ms")
    val speedup = if (durationBuffered > 0) durationSync.toDouble / durationBuffered else Double.PositiveInfinity
    println(f"Performance Speedup (Sync Time / Buffered Time): ${speedup}x")
    println(s"(Higher speedup indicates BufferedWriter is faster)")

    Thread.sleep(30)
    writer.shutdown()

    // Optional: Verify row counts in DB (requires separate DB query)
    // val conn = TestDbConfig.getConnection
    // val countStmt = conn.createStatement()
    // val rs = countStmt.executeQuery("SELECT COUNT(*) FROM public.users_test;")
    // rs.next()
    // val dbRowCount = rs.getLong(1)
    // println(s"\nActual row count in DB after tests: $dbRowCount")
    // conn.close()

    // --- DB Cleanup ---
    // TestDbSetup.cleanup() // Uncomment this line if you want to drop the table after the test

    // Note: The EC managed *by* your BufferedWriter is not shut down here,
    // assuming its shutdown method (if it has one) or the process exit handles it.
    // The global EC used by the test code can also be shut down if needed,
    // but typically not necessary in a simple application main.
  }
}