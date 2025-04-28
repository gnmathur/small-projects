package lib

import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, SQLException}
import java.time.LocalDateTime
import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

// Prometheus imports
import io.prometheus.client.{Counter, Gauge, Histogram, Summary}
import io.prometheus.client.hotspot.DefaultExports

// HikariCP imports
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

class AsyncBufferedWriter(batchSize: Int,
                          pgHost: String,
                          pgPort: Int,
                          pgDb: String,
                          pgUser: String,
                          pgPassword: String) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // Initialize Prometheus metrics
  private val rowsInsertedCounter = Counter.build()
    .name("buffered_writer_rows_inserted_total")
    .help("Total number of rows inserted by BufferedWriter")
    .register()

  private val pendingRowsGauge = Gauge.build()
    .name("buffered_writer_pending_rows")
    .help("Number of rows pending insertion")
    .register()

  private val queueLengthGauge = Gauge.build()
    .name("buffered_writer_queue_length")
    .help("Number of batches in the write pending queue")
    .register()

  private val queueTimeSeconds = Summary.build()
    .name("buffered_writer_queue_time_seconds")
    .help("Time spent by batches in the write queue before processing")
    .quantile(0.5, 0.05)
    .quantile(0.9, 0.01)
    .register()

  private val batchSizeHistogram = Histogram.build()
    .name("buffered_writer_batch_size")
    .help("Distribution of batch sizes")
    .buckets(10, 50, 100, 500, 1000)
    .register()

  private val insertLatency = Summary.build()
    .name("buffered_writer_insert_latency_seconds")
    .help("Latency of insert operations")
    .quantile(0.5, 0.05)   // Add 50th percentile with 5% error
    .quantile(0.9, 0.01)   // Add 90th percentile with 1% error
    .register()

  private val connectionPoolGauge = Gauge.build()
    .name("buffered_writer_connection_pool_size")
    .help("Current size of the connection pool")
    .register()

  // Add metrics for successful and failed futures
  private val successfulFuturesCounter = Counter.build()
    .name("buffered_writer_successful_futures_total")
    .help("Total number of successful futures in BufferedWriter")
    .register()

  private val failedFuturesCounter = Counter.build()
    .name("buffered_writer_failed_futures_total")
    .help("Total number of failed futures in BufferedWriter")
    .labelNames("error_type")
    .register()

  private val futureLatency = Summary.build()
    .name("buffered_writer_future_execution_time_seconds")
    .help("Execution time of futures in BufferedWriter")
    .quantile(0.5, 0.05)
    .quantile(0.9, 0.01)
    .labelNames("status")  // "success" or "failure"
    .register()

  // Register JVM metrics
  DefaultExports.initialize()

  private val es: ExecutorService = Executors.newCachedThreadPool()
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(es)

  private val jdbcURL = s"jdbc:postgresql://$pgHost:$pgPort/$pgDb"

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

  def shutdown(): Unit = {
    // Process any remaining writes in the queue before shutting down
    this.synchronized {
      logger.info(s"Processing ${writePendingQueue.size} remaining rows in the queue before shutdown")

      if (writePendingQueue.nonEmpty) {
        try {
          val rowsWritten = writeRows(writePendingQueue)
          logger.info(s"Processed $rowsWritten remaining rows during shutdown")

          // Complete all promises with success
          writePendingQueue.foreach {
            case (_, _, p, _) => p.success(InsertResult(LocalDateTime.now()))
          }
          // Reset gauges after clearing the queue
          pendingRowsGauge.set(0)
          queueLengthGauge.set(0)

        } catch {
          case ex: Exception =>
            logger.error(s"Error processing remaining writes during shutdown: ${ex.getMessage}", ex)
            // Complete all promises with failure
            writePendingQueue.foreach { case (_, _, p, _) => p.failure(ex) }
        } finally {
          // Clear the queue after processing
          writePendingQueue.clear()
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
      // Close the connection pool when shutting down
      if (connectionPool != null && !connectionPool.isClosed) {
        connectionPool.close()
      }
    }
  }

  private def addToQueue(sql: String, rows: Seq[Seq[Any]], promise: Promise[InsertResult]): Int = {
    if (sql == null || rows == null || promise == null || writePendingQueue == null) {
      throw new IllegalArgumentException("SQL, rows, and promise must not be null")
    }

    // Add current timestamp in milliseconds
    val timestamp = System.currentTimeMillis()
    writePendingQueue.append((sql, rows, promise, timestamp))

    val totalRows = writePendingQueue.foldLeft(0) { (acc, elem) =>
      acc + elem._2.size
    }

    // Update both gauges with current queue metrics
    pendingRowsGauge.set(totalRows)
    queueLengthGauge.set(writePendingQueue.size)

    totalRows
  }

  /** Insert rows into Postgres */
  def insert(sql: String, rows: Seq[Seq[Any]]): Future[InsertResult] = {
    val promise = Promise[InsertResult]()
    val insertTimer = insertLatency.startTimer()
    val startTime = System.nanoTime() // For tracking future execution time

    this.synchronized {
      val (shouldWrite, totalRows) = {
        val totalRows = addToQueue(sql, rows, promise)
        (totalRows >= batchSize, totalRows)
      }

      if (shouldWrite) {
        val queueCopy = {
          val copy = ListBuffer[(String, Seq[Seq[Any]], Promise[InsertResult], Long)]() ++= writePendingQueue
          writePendingQueue.clear()

          // Reset both gauges after clearing the queue
          pendingRowsGauge.set(0)
          queueLengthGauge.set(0)

          copy
        }

        // Record batch size in histogram
        batchSizeHistogram.observe(totalRows)

        Future {
          val now = System.currentTimeMillis()

          // When processing items, record how long they waited in the queue
          queueCopy.foreach { case (_, _, _, timestamp) =>
            val queueTimeInSeconds = (now - timestamp) / 1000.0
            queueTimeSeconds.observe(queueTimeInSeconds)
          }

          val rowsWritten = writeRows(queueCopy)
          // Complete all promises with the number of rows they contributed
          queueCopy.foreach { case (_, _, p, _) =>
            p.success(InsertResult(LocalDateTime.now()))
          }
        }.onComplete {
          case Success(_) =>
            // Track successful futures
            successfulFuturesCounter.inc()
            val executionTimeSeconds = (System.nanoTime() - startTime) / 1.0e9
            futureLatency.labels("success").observe(executionTimeSeconds)
            logger.debug(s"Batch write operation completed successfully in $executionTimeSeconds seconds")

          case Failure(ex) =>
            // Complete all promises with failure in case of error
            queueCopy.foreach { case (_, _, p, _) => p.failure(ex) }

            // Track failed futures with error type
            val errorType = ex.getClass.getSimpleName
            failedFuturesCounter.labels(errorType).inc()
            val executionTimeSeconds = (System.nanoTime() - startTime) / 1.0e9
            futureLatency.labels("failure").observe(executionTimeSeconds)

            logger.error(s"Batch write operation failed after $executionTimeSeconds seconds: ${ex.getMessage}", ex)
        }
      }
    }

    // When the future completes, stop the timer
    promise.future.onComplete {
      case _ => insertTimer.observeDuration()
    }

    promise.future
  }

  private def writeRows(queue: ListBuffer[(String, Seq[Seq[Any]], Promise[InsertResult], Long)]): Int = {
    var conn: Connection = null
    var totalRowsWritten = 0

    try {
      // Update connection pool gauge
      connectionPoolGauge.set(connectionPool.getHikariPoolMXBean.getTotalConnections)

      // Get connection from the pool instead of creating a new one
      conn = connectionPool.getConnection()
      conn.setAutoCommit(false)

      // Group by SQL statement
      val groupedBySQL = queue.groupBy(_._1)

      groupedBySQL.foreach { case (sql, entries) =>
        val stmt = conn.prepareStatement(sql)
        try {
          // Combine all rows for this SQL statement
          val allRows = entries.flatMap(_._2)

          allRows.foreach { row =>
            for (i <- row.indices) {
              stmt.setObject(i + 1, row(i))
            }
            stmt.addBatch()
          }

          // Execute the batch after adding all rows for this SQL
          val batchResults = stmt.executeBatch()
          val rowsInBatch = batchResults.sum
          totalRowsWritten += rowsInBatch

          // Increment counter with rows written
          rowsInsertedCounter.inc(rowsInBatch)
        } finally {
          stmt.close()
        }
      }

      conn.commit()

      logger.debug(s"Transaction committed with $totalRowsWritten rows")

      totalRowsWritten
    } catch {
      case ex: SQLException =>
        logger.error(s"SQL Exception: ${ex.getMessage}", ex)
        if (conn != null) conn.rollback()
        throw ex
      case ex: Exception =>
        logger.error(s"General Exception: ${ex.getMessage}", ex)
        if (conn != null) conn.rollback()
        throw ex
    } finally {
      // Return connection to the pool instead of closing it
      if (conn != null) {
        conn.close() // In Hikari, this returns the connection to the pool rather than actually closing it
      }

      // Update connection pool gauge after returning connection
      connectionPoolGauge.set(connectionPool.getHikariPoolMXBean.getTotalConnections)
    }
  }
}