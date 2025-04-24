import java.sql.{Connection, DriverManager, SQLException}
import java.time.LocalDateTime
import java.util.concurrent.{ExecutorService, Executors, LinkedBlockingQueue}
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object BufferedWriter extends App {
  private val BATCH_SIZE = 5
  private val es : ExecutorService = Executors.newCachedThreadPool()
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(es)
  /** Write pending inserts to this write-pending queue */
  private val writePendingQueue = new ListBuffer[(String, Seq[Seq[Any]])]()

  private def addToQueue(sql: String, row: Seq[Seq[Any]]): Int = {
    writePendingQueue.append((sql, row))
    writePendingQueue.foldLeft(0) { (acc, elem) =>
      acc + elem._2.size
    }
  }

  /** Insert a row into Postgres */
  def insert(sql: String, row: Seq[Seq[Any]]): Future[Int] = {
    val (shouldWrite, totalRows) = this.synchronized {
      val totalRows = addToQueue(sql, row)
      (totalRows >= BATCH_SIZE, totalRows)
    }

    if (shouldWrite) {
      val queueCopy = this.synchronized {
        val copy = ListBuffer[(String, Seq[Seq[Any]])]() ++= writePendingQueue
        writePendingQueue.clear()
        copy
      }

      Future {
        writeRows(queueCopy)
      }
    } else {
      Future.successful(0)
    }
  }

  private def writeRows(queue: ListBuffer[(String, Seq[Seq[Any]])]): Int = {
    var conn: Connection = null
    var totalRowsWritten = 0

    try {
      conn = getConnection
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
      println(s"Transaction committed with $totalRowsWritten rows")
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
      if (conn != null) {
        conn.close()
      }
    }
  }


  /*
  private def writeRows(writePendingQueue: ListBuffer[(String, Seq[Seq[Any]])]): Int = {
    var conn: Connection = null
    var rowsWritten = 0
    try {
      conn = getConnection
      conn.setAutoCommit(false)

      while (writePendingQueue.nonEmpty) {
        val (sql, params) = writePendingQueue.remove(0)
        val rs = conn.prepareStatement(sql)
        try {

          params.foreach { row =>
            row.zipWithIndex.foreach { case (value, i) =>
              rs.setObject(i + 1, value)
            }
            rowsWritten += 1
            rs.addBatch()
          }
          rs.executeBatch()
          rs.close()
        } catch {
          case ex: SQLException =>
            println(s"SQL Exception: ${ex.getMessage}")
            if (conn != null) conn.rollback()
            throw ex
        }
      }
      conn.commit()
      println(s"Transaction committed with $rowsWritten rows")
      rowsWritten
    } catch {
      case ex: Exception =>
        if (conn != null) conn.rollback()
        println(s"Transaction failed: ${ex.getMessage}")
        0
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }
  */

  private def getConnection: Connection = {
    Class.forName("org.postgresql.Driver")
    try {
      DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "postgres")
    } catch {
      case ex: SQLException =>
        println(s"Failed to obtain connection: ${ex.getMessage}")
        throw ex
    }
  }

  private def runUseCase(): Unit = {
    // Specify the insert string
    val insertSql = "INSERT INTO users (id, name, balance, created_date) VALUES (?, ?, ?, ?)"
    // Create two sets of rows to insert, emulating two separate insert operations at different times
    val rowsT0 = Seq(
      Seq(1, "Jai", 4000.0, LocalDateTime.now()),
      Seq(2, "Veeru", 5000.0, LocalDateTime.now()),
      Seq(3, "Samba", 6000.0, LocalDateTime.now()),
    )
    val rowsT1 = Seq(
      Seq(4, "Ram", 1000.0, LocalDateTime.now()),
      Seq(5, "Shyam", 2000.0, LocalDateTime.now()),
      Seq(6, "Krishna", 3000.0, LocalDateTime.now()),
    )

    // Insert the rows and handle the futures
    val f1 = insert(insertSql, rowsT0)
    f1.onComplete {
      case Success(count) => println(s"First batch: Successfully inserted $count rows")
      case Failure(ex) => println(s"First batch: Failed to insert rows - ${ex.getMessage}")
    }

    val f2 = insert(insertSql, rowsT1)
    f2.onComplete {
      case Success(count) => println(s"Second batch: Successfully inserted $count rows")
      case Failure(ex) => println(s"Second batch: Failed to insert rows - ${ex.getMessage}")
    }

    Await.result(f1, scala.concurrent.duration.Duration.Inf)
    Await.result(f2, scala.concurrent.duration.Duration.Inf)

    // Print results of the insert operations
    println(s"Rows inserted in first operation: ${f1.value}")
    println(s"Rows inserted in second operation: ${f2.value}")
  }

  runUseCase()

  es.shutdown()
}