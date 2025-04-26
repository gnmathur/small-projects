package lib

import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Connection, SQLException}
import java.time.LocalDateTime
import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue}
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

// Add HikariCP imports
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

class BufferedWriter(BATCH_SIZE: Int,
                     pgHost: String,
                     pgPort: Int,
                     pgDb: String,
                     pgUser: String,
                     pgPassword: String) {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val es: ExecutorService = Executors.newCachedThreadPool()
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(es)

  private val jdbcURL = s"jdbc:postgresql://$pgHost:$pgPort/$pgDb"

  private val connectionPool: HikariDataSource = {
    Class.forName("org.postgresql.Driver")

    val config = new HikariConfig()
    config.setJdbcUrl(jdbcURL)
    config.setUsername(pgUser)
    config.setPassword(pgPassword)
    config.setMaximumPoolSize(10) // Set fixed pool size
    config.setMinimumIdle(5)      // Minimum number of idle connections
    config.setIdleTimeout(30000)  // How long a connection can remain idle before being removed
    config.setConnectionTimeout(10000) // Maximum time to wait for a connection from the pool
    config.setPoolName("BufferedWriterConnectionPool")
    new HikariDataSource(config)
  }

  case class InsertResult(writtenAt: LocalDateTime)

  /** Write pending inserts to this write-pending queue */
  private val writePendingQueue = new ListBuffer[(String, Seq[Seq[Any]], Promise[InsertResult])]()

  def shutdown(): Unit = {
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

    writePendingQueue.append((sql, rows, promise))
    writePendingQueue.foldLeft(0) { (acc, elem) =>
      acc + elem._2.size
    }
  }

  /** Insert rows into Postgres */
  def insert(sql: String, rows: Seq[Seq[Any]]): Future[InsertResult] = {
    val promise = Promise[InsertResult]()

    this.synchronized {
      val (shouldWrite, totalRows) = {
        val totalRows = addToQueue(sql, rows, promise)
        (totalRows >= BATCH_SIZE, totalRows)
      }

      if (shouldWrite) {
        val queueCopy = {
          val copy = ListBuffer[(String, Seq[Seq[Any]], Promise[InsertResult])]() ++= writePendingQueue
          writePendingQueue.clear()
          copy
        }

        Future {
          val rowsWritten = writeRows(queueCopy)
          // Complete all promises with the number of rows they contributed
          queueCopy.foreach { case (_, batchRows, p) =>
            p.success(InsertResult(LocalDateTime.now()))
          }
        }.onComplete {
          case Failure(ex) =>
            // Complete all promises with failure in case of error
            queueCopy.foreach { case (_, _, p) => p.failure(ex) }
          case _ => // Success is already handled above
        }
      }
    }
    promise.future
  }

  private def writeRows(queue: ListBuffer[(String, Seq[Seq[Any]], Promise[InsertResult])]): Int = {
    var conn: Connection = null
    var totalRowsWritten = 0

    try {
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
          totalRowsWritten += batchResults.sum
        } finally {
          stmt.close()
        }
      }

      conn.commit()

      logger.debug(s"Transaction committed with $totalRowsWritten rows")

      totalRowsWritten
    } catch {
      case ex: SQLException =>
        println(s"SQL Exception: ${ex.getMessage}")
        if (conn != null) conn.rollback()
        throw ex
      case ex: Exception =>
        println(s"General Exception: ${ex.getMessage}")
        if (conn != null) conn.rollback()
        throw ex
    } finally {
      // Return connection to the pool instead of closing it
      if (conn != null) {
        conn.close() // In Hikari, this returns the connection to the pool rather than actually closing it
      }
    }
  }
}
