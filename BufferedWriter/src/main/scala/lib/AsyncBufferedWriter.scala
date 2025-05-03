package lib

import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, SQLException}
import java.time.LocalDateTime
import java.util.concurrent._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.concurrent.duration._

// Prometheus imports
import io.prometheus.client.{Counter, Gauge, Histogram, Summary}
import io.prometheus.client.hotspot.DefaultExports

// HikariCP imports
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

// Add imports for scheduling
import java.util.concurrent.{Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

class AsyncBufferedWriter(batchSize: Int,
                          flushTimeoutMillis: Long,
                          pgHost: String,
                          pgPort: Int,
                          pgDb: String,
                          pgUser: String,
                          pgPassword: String) {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // Initialize Prometheus metrics

  // Metric to count total rows inserted (existing)
  private val rowsInsertedCounter = Counter.build()
    .name("buffered_writer_rows_inserted_total")
    .help("Total number of rows inserted by BufferedWriter")
    .register()

  // Gauge for pending rows in the queue (existing)
  private val pendingRowsGauge = Gauge.build()
    .name("buffered_writer_pending_rows")
    .help("Number of rows pending insertion")
    .register()

  // Gauge for number of batches/groups in the queue (existing)
  private val queueLengthGauge = Gauge.build()
    .name("buffered_writer_queue_length")
    .help("Number of batches in the write pending queue")
    .register()

  // Summary for time spent by items in the queue before processing (existing)
  private val queueTimeSeconds = Summary.build()
    .name("buffered_writer_queue_time_seconds")
    .help("Time spent by batches in the write queue before processing")
    .quantile(0.5, 0.05)
    .quantile(0.9, 0.01)
    .register()

  // Histogram for distribution of batch sizes, now labeled by trigger reason
  private val batchSizeHistogram = Histogram.build()
    .name("buffered_writer_batch_size")
    .help("Distribution of batch sizes")
    .buckets(10, 50, 100, 500, 1000)
    .labelNames("trigger_reason") // Added label
    .register()

  // Summary for overall insert latency (from insert call to future completion) (existing)
  private val insertLatency = Summary.build()
    .name("buffered_writer_insert_latency_seconds")
    .help("Latency of insert operations")
    .quantile(0.5, 0.05)
    .quantile(0.9, 0.01)
    .register()

  // Gauge for connection pool size (existing)
  private val connectionPoolGauge = Gauge.build()
    .name("buffered_writer_connection_pool_size")
    .help("Current size of the connection pool")
    .register()

  // Add metrics for successful and failed futures, now labeled by trigger reason

  // Counter for successful batch write futures, labeled by trigger reason
  private val successfulFuturesCounter = Counter.build()
    .name("buffered_writer_successful_futures_total")
    .help("Total number of successful futures for batch writes")
    .labelNames("trigger_reason") // Added label
    .register()

  // Counter for failed batch write futures, labeled by trigger reason and error type
  private val failedFuturesCounter = Counter.build()
    .name("buffered_writer_failed_futures_total")
    .help("Total number of failed futures for batch writes")
    .labelNames("trigger_reason", "error_type") // Added label
    .register()

  // Summary for execution time of write futures, labeled by status and trigger reason
  private val futureLatency = Summary.build()
    .name("buffered_writer_future_execution_time_seconds")
    .help("Execution time of futures for batch writes")
    .quantile(0.5, 0.05)
    .quantile(0.9, 0.01)
    .labelNames("status", "trigger_reason")  // Added label
    .register()

  // New metric: Counter for the number of flush events, labeled by trigger reason
  private val flushEventsCounter = Counter.build()
    .name("buffered_writer_flush_events_total")
    .help("Total number of times a flush operation was triggered")
    .labelNames("trigger_reason") // Label to distinguish why it was triggered
    .register()


  // Register JVM metrics (existing)
  DefaultExports.initialize()

  // Executor service for async database writes (existing)
  private val es: ExecutorService = Executors.newCachedThreadPool()
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(es)

  // Scheduled executor service for periodic checks (Existing)
  private val scheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()

  private val jdbcURL = s"jdbc:postgresql://$pgHost:$pgPort/$pgDb"

  private val checkIntervalMillis = 1000L // Check every second

  // HikariCP connection pool (existing)
  private val connectionPool: HikariDataSource = {
    Class.forName("org.postgresql.Driver")

    val config = new HikariConfig()
    config.setRegisterMbeans(true)
    config.setJdbcUrl(jdbcURL)
    config.setUsername(pgUser)
    config.setPassword(pgPassword)
    config.setMaximumPoolSize(10) // Set fixed pool size
    config.setMinimumIdle(3)      // Minimum number of idle connections
    config.setIdleTimeout(30000)  // How long a connection can remain idle before being removed
    config.setConnectionTimeout(10000) // Maximum time to wait for a connection from the pool
    config.setPoolName("BufferedWriterConnectionPool")
    new HikariDataSource(config)
  }

  case class InsertResult(writtenAt: LocalDateTime)

  /** Write pending inserts to this write-pending queue with timestamp */
  private val writePendingQueue = new ListBuffer[(String, Seq[Seq[Any]], Promise[InsertResult], Long)]()

  // The task that periodically checks if a flush is needed due to timeout (Existing)
  private val flushCheckTask: Runnable = new Runnable {
    override def run(): Unit = {
      try {
        AsyncBufferedWriter.this.synchronized {
          if (writePendingQueue.nonEmpty) {
            val oldestElementTimestamp = writePendingQueue.head._4
            val currentTime = System.currentTimeMillis()
            val age = currentTime - oldestElementTimestamp

            if (age >= flushTimeoutMillis) {
              logger.debug(s"Timeout ($flushTimeoutMillis ms) reached for oldest element. Triggering time-based flush.")
              triggerFlushAndClearQueueLocked("timeout") // Call with reason
            }
          }
        }
      } catch {
        case t: Throwable =>
          logger.error("Error in scheduled flush check task", t)
      }
    }
  }

  // Scheduled future for the timeout check task (Existing)
  private val flushTask: ScheduledFuture[_] = scheduler.scheduleAtFixedRate(
    flushCheckTask,
    checkIntervalMillis,
    checkIntervalMillis,
    TimeUnit.MILLISECONDS
  )

  def shutdown(): Unit = {
    flushTask.cancel(false)
    scheduler.shutdown()
    try {
      if (!scheduler.awaitTermination(flushTimeoutMillis + 10, TimeUnit.SECONDS)) {
        scheduler.shutdownNow()
      }
    } catch {
      case e: InterruptedException =>
        scheduler.shutdownNow()
        Thread.currentThread().interrupt()
    }

    this.synchronized {
      logger.info(s"Processing ${writePendingQueue.size} remaining rows in the queue before shutdown")

      if (writePendingQueue.nonEmpty) {
        try {
          // Trigger flush for remaining items on shutdown
          triggerFlushAndClearQueueLocked("shutdown", completePromisesImmediately = true) // Call with reason
          logger.info(s"Triggered flush for remaining rows during shutdown")

        } catch {
          case ex: Exception =>
            logger.error(s"Error processing remaining writes during shutdown: ${ex.getMessage}", ex)
            writePendingQueue.foreach { case (_, _, p, _) => p.failure(ex) }
            writePendingQueue.clear()
            pendingRowsGauge.set(0)
            queueLengthGauge.set(0)
        }
      }
    }

    es.shutdown()
    try {
      if (!es.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
        es.shutdownNow()
      }
    } catch {
      case e: InterruptedException =>
        es.shutdownNow()
        Thread.currentThread().interrupt()
    } finally {
      if (connectionPool != null && !connectionPool.isClosed) {
        connectionPool.close()
      }
    }
  }

  private def addToQueue(sql: String, rows: Seq[Seq[Any]], promise: Promise[InsertResult]): Int = this.synchronized {
    if (sql == null || rows == null || promise == null || writePendingQueue == null) {
      throw new IllegalArgumentException("SQL, rows, and promise must not be null")
    }

    val timestamp = System.currentTimeMillis()
    writePendingQueue.append((sql, rows, promise, timestamp))

    val totalRows = writePendingQueue.foldLeft(0) { (acc, elem) =>
      acc + elem._2.size
    }

    pendingRowsGauge.set(totalRows)
    queueLengthGauge.set(writePendingQueue.size)

    totalRows
  }

  def insert(sql: String, rows: Seq[Seq[Any]]): Future[InsertResult] = {
    val promise = Promise[InsertResult]()
    val insertTimer = insertLatency.startTimer()

    this.synchronized {
      val totalRows = addToQueue(sql, rows, promise)

      if (totalRows >= batchSize) {
        logger.debug(s"Batch size ($batchSize) reached. Triggering size-based flush.")
        triggerFlushAndClearQueueLocked("size") // Call with reason
      }
    }

    promise.future.onComplete {
      case _ => insertTimer.observeDuration()
    }

    promise.future
  }

  /**
   * Triggers a flush of the current queue and clears it.
   * This method must be called from WITHIN a `this.synchronized` block.
   * The actual write operation is submitted as a Future to avoid blocking the synchronised block.
   *
   * @param reason The reason for the flush ("size", "timeout", "shutdown") for logging/metrics.
   * @param completePromisesImmediately Whether to complete promises within the synchronized block (used for shutdown failure handling).
   */
  private def triggerFlushAndClearQueueLocked(reason: String, completePromisesImmediately: Boolean = false): Unit = {
    if (writePendingQueue.isEmpty) {
      logger.debug(s"Triggered flush ($reason) but queue is empty.")
      return
    }

    // Increment the flush event counter with the reason
    flushEventsCounter.labels(reason).inc()

    val queueCopy = ListBuffer[(String, Seq[Seq[Any]], Promise[InsertResult], Long)]() ++= writePendingQueue
    val totalRowsInBatch = queueCopy.foldLeft(0)(_ + _._2.size)

    writePendingQueue.clear()

    pendingRowsGauge.set(0)
    queueLengthGauge.set(0)

    logger.info(s"Triggering flush of batch with ${queueCopy.size} items (${totalRowsInBatch} rows) due to $reason.")

    // Record batch size in histogram, labeled by reason
    batchSizeHistogram.labels(reason).observe(totalRowsInBatch)

    val startTime = System.nanoTime()

    Future {
      val now = System.currentTimeMillis()

      // Record how long items waited in the queue
      queueCopy.foreach { case (_, _, _, timestamp) =>
        val queueTimeInSeconds = (now - timestamp) / 1000.0
        queueTimeSeconds.observe(queueTimeInSeconds) // Queue time is independent of trigger reason
      }

      val rowsWritten = writeRows(queueCopy) // writeRows handles promise failure internally on SQLException

      // If writeRows completed without throwing, complete promises successfully
      queueCopy.foreach { case (_, _, p, _) =>
        // Only succeed if the promise hasn't already been failed by writeRows (e.g., SQLException)
        if (!p.isCompleted) {
          p.success(InsertResult(LocalDateTime.now()))
        }
      }

      rowsWritten
    }.onComplete {
      case Success(rowsWritten) =>
        // Track successful futures, labeled by trigger reason
        successfulFuturesCounter.labels(reason).inc()
        val executionTimeSeconds = (System.nanoTime() - startTime) / 1.0e9
        futureLatency.labels("success", reason).observe(executionTimeSeconds) // Labeled
        logger.debug(s"Batch write operation ($reason) completed successfully in $executionTimeSeconds seconds, wrote $rowsWritten rows.")

      case Failure(ex) =>
        // Promises should have been failed by writeRows or the Future's own failure
        // Track failed futures, labeled by trigger reason and error type
        val errorType = ex.getClass.getSimpleName
        failedFuturesCounter.labels(reason, errorType).inc() // Labeled
        val executionTimeSeconds = (System.nanoTime() - startTime) / 1.0e9
        futureLatency.labels("failure", reason).observe(executionTimeSeconds) // Labeled

        logger.error(s"Batch write operation ($reason) failed after $executionTimeSeconds seconds: ${ex.getMessage}", ex)
    }
  }

  // Existing method to write to DB (no metric changes needed here, metrics are updated by the caller Future)
  private def writeRows(queue: ListBuffer[(String, Seq[Seq[Any]], Promise[InsertResult], Long)]): Int = {
    var conn: Connection = null
    var totalRowsWritten = 0
    var caughtException: Option[Throwable] = None // Track if an exception occurred

    try {
      connectionPoolGauge.set(connectionPool.getHikariPoolMXBean.getTotalConnections)
      conn = connectionPool.getConnection()
      conn.setAutoCommit(false)

      val groupedBySQL = queue.groupBy(_._1)

      groupedBySQL.foreach { case (sql, entries) =>
        var stmt: java.sql.PreparedStatement = null
        try {
          stmt = conn.prepareStatement(sql)
          val allRows = entries.flatMap(_._2)

          allRows.foreach { row =>
            for (i <- row.indices) {
              stmt.setObject(i + 1, row(i))
            }
            stmt.addBatch()
          }

          val batchResults = stmt.executeBatch()
          val successfulRowsInBatch = batchResults.count(_ >= 0)
          totalRowsWritten += successfulRowsInBatch

          // Increment total rows counter
          rowsInsertedCounter.inc(successfulRowsInBatch)

        } catch {
          case ex: SQLException =>
            logger.error(s"SQLException during batch execution for SQL: $sql", ex)
            caughtException = Some(ex) // Store the exception
            throw ex // Re-throw to trigger outer rollback
        } finally {
          if (stmt != null) {
            try { stmt.close() } catch { case ex: SQLException => logger.error("Error closing statement", ex) }
          }
        }
      }

      // Only commit if no exception was caught during statement processing
      if (caughtException.isEmpty) {
        conn.commit()
        logger.debug(s"Transaction committed with $totalRowsWritten rows")
      } else {
        // This case should ideally not be hit if throw ex is used, but as a safeguard:
        logger.error("Commit skipped due to prior SQLException.")
        conn.rollback() // Rollback if statement processing failed
      }


      totalRowsWritten

    } catch {
      case ex: SQLException =>
        logger.error(s"SQL Exception during batch write: ${ex.getMessage}", ex)
        if (conn != null) {
          try { conn.rollback(); logger.debug("Transaction rolled back.") }
          catch { case rbEx: SQLException => logger.error("Error during rollback", rbEx) }
        }
        // Fail promises associated with this batch because the transaction failed
        queue.foreach { case (_, _, p, _) => if (!p.isCompleted) p.failure(ex) }
        throw ex // Re-throw to be caught by Future's onComplete

      case ex: Exception =>
        logger.error(s"General Exception during batch write: ${ex.getMessage}", ex)
        if (conn != null) {
          try { conn.rollback(); logger.debug("Transaction rolled back.") }
          catch { case rbEx: SQLException => logger.error("Error during rollback", rbEx) }
        }
        // Fail promises associated with this batch because the write failed
        queue.foreach { case (_, _, p, _) => if (!p.isCompleted) p.failure(ex) }
        throw ex // Re-throw to be caught by Future's onComplete
    } finally {
      if (conn != null) {
        try { conn.close() } // Returns to pool
        catch { case ex: SQLException => logger.error("Error closing connection (returning to pool)", ex) }
      }
      connectionPoolGauge.set(connectionPool.getHikariPoolMXBean.getTotalConnections)
    }
  }
}