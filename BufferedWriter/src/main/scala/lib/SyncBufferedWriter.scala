package lib

import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, SQLException}
import java.time.LocalDateTime
import java.util.{Timer, TimerTask}
import scala.collection.mutable.ListBuffer
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

// Prometheus imports
import io.prometheus.client.{Counter, Gauge, Histogram, Summary}
import io.prometheus.client.hotspot.DefaultExports

class SyncBufferedWriter(batchSize: Int,
                         pgHost: String,
                         pgPort: Int,
                         pgDb: String,
                         pgUser: String,
                         pgPassword: String) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  // Initialize Prometheus metrics
  private val rowsInsertedCounter = Counter.build()
    .name("sync_buffered_writer_rows_inserted_total")
    .help("Total number of rows inserted by SynchronousBufferedWriter")
    .register()

  private val pendingRowsGauge = Gauge.build()
    .name("sync_buffered_writer_pending_rows")
    .help("Number of rows pending insertion")
    .register()

  private val queueLengthGauge = Gauge.build()
    .name("sync_buffered_writer_queue_length")
    .help("Number of batches in the write pending queue")
    .register()

  private val batchSizeHistogram = Histogram.build()
    .name("sync_buffered_writer_batch_size")
    .help("Distribution of batch sizes")
    .buckets(10, 50, 100, 500, 1000)
    .register()

  private val insertLatency = Summary.build()
    .name("sync_buffered_writer_insert_latency_seconds")
    .help("Latency of insert operations")
    .quantile(0.5, 0.05)
    .quantile(0.9, 0.01)
    .register()

  private val connectionPoolGauge = Gauge.build()
    .name("sync_buffered_writer_connection_pool_size")
    .help("Current size of the connection pool")
    .register()

  // Register JVM metrics
  DefaultExports.initialize()

  private val jdbcURL = s"jdbc:postgresql://$pgHost:$pgPort/$pgDb"

  private val connectionPool: HikariDataSource = {
    Class.forName("org.postgresql.Driver")

    val config = new HikariConfig()
    config.setJdbcUrl(jdbcURL)
    config.setUsername(pgUser)
    config.setPassword(pgPassword)
    config.setMaximumPoolSize(6)
    config.setMinimumIdle(5)
    config.setIdleTimeout(30000)
    config.setConnectionTimeout(10000)
    config.setPoolName("SynchronousBufferedWriterConnectionPool")
    new HikariDataSource(config)
  }

  case class InsertResult(writtenAt: LocalDateTime, rowsWritten: Int)

  /** Write pending inserts to this write-pending queue with timestamp */
  private val writePendingQueue = new ListBuffer[(String, Seq[Seq[Any]], Long)]()

  // Optional: A scheduled flush timer for batches that don't reach the threshold
  private val flushTimer = new Timer("SynchronousBufferedWriterFlushTimer", true)
  private val flushPeriodMs = 30000 // 30 seconds

  // Initialize the flush timer task
  flushTimer.scheduleAtFixedRate(new TimerTask {
    override def run(): Unit = {
      flushQueue()
    }
  }, flushPeriodMs, flushPeriodMs)

  def shutdown(): Unit = {
    flushTimer.cancel()

    // Close the connection pool when shutting down
    if (connectionPool != null && !connectionPool.isClosed) {
      connectionPool.close()
    }
  }

  private def addToQueue(sql: String, rows: Seq[Seq[Any]]): Int = {
    if (sql == null || rows == null || writePendingQueue == null) {
      throw new IllegalArgumentException("SQL and rows must not be null")
    }

    // Add current timestamp in milliseconds
    val timestamp = System.currentTimeMillis()
    writePendingQueue.append((sql, rows, timestamp))

    val totalRows = writePendingQueue.foldLeft(0) { (acc, elem) =>
      acc + elem._2.size
    }

    // Update both gauges with current queue metrics
    pendingRowsGauge.set(totalRows)
    queueLengthGauge.set(writePendingQueue.size)

    totalRows
  }

  // Added method to explicitly flush the queue
  def flushQueue(): InsertResult = {
    this.synchronized {
      if (writePendingQueue.isEmpty) {
        return InsertResult(LocalDateTime.now(), 0)
      }

      val queueCopy = ListBuffer[(String, Seq[Seq[Any]], Long)]() ++= writePendingQueue
      writePendingQueue.clear()

      // Reset both gauges after clearing the queue
      pendingRowsGauge.set(0)
      queueLengthGauge.set(0)

      // Record batch size in histogram
      val totalRows = queueCopy.foldLeft(0) { (acc, elem) => acc + elem._2.size }
      batchSizeHistogram.observe(totalRows)

      val rowsWritten = writeRows(queueCopy)
      InsertResult(LocalDateTime.now(), rowsWritten)
    }
  }

  /** Insert rows into Postgres - now synchronous */
  def insert(sql: String, rows: Seq[Seq[Any]]): InsertResult = {
    if (rows.isEmpty) {
      return InsertResult(LocalDateTime.now(), 0)
    }

    val insertTimer = insertLatency.startTimer()
    try {
      this.synchronized {
        val (shouldWrite, totalRows) = {
          val totalRows = addToQueue(sql, rows)
          (totalRows >= batchSize, totalRows)
        }

        if (shouldWrite) {
          val result = flushQueue()
          return result
        } else {
          // If the batch size threshold isn't met yet, just return a result
          // indicating the rows were queued but not yet written
          return InsertResult(LocalDateTime.now(), 0)
        }
      }
    } finally {
      insertTimer.observeDuration()
    }
  }

  /** Force a synchronous write of all pending rows */
  def flush(): InsertResult = {
    flushQueue()
  }

  private def writeRows(queue: ListBuffer[(String, Seq[Seq[Any]], Long)]): Int = {
    var conn: Connection = null
    var totalRowsWritten = 0

    try {
      // Update connection pool gauge
      connectionPoolGauge.set(connectionPool.getHikariPoolMXBean.getTotalConnections)

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
      if (conn != null) {
        conn.close() // Returns connection to the pool
      }

      // Update connection pool gauge after returning connection
      connectionPoolGauge.set(connectionPool.getHikariPoolMXBean.getTotalConnections)
    }
  }
}
