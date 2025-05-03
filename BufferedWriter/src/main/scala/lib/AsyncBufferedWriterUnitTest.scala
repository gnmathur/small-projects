package lib

import java.time.LocalDateTime
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object AsyncBufferedWriterUnitTest extends App {
  private val es: ExecutorService = Executors.newCachedThreadPool()
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(es)

  private def runUseCase(): Unit = {
    // Specify the insert string
    val insertSql = "INSERT INTO users (id, name, balance, created_date) VALUES (?, ?, ?, ?)"

    // Create two sets of rows to insert, emulating two separate insert operations at different times
    val rowsT0 = Seq(
      Seq(1, "Jai", 4000.0, LocalDateTime.now()),
      Seq(2, "Veeru", 5000.0, LocalDateTime.now()),
      Seq(3, "Samba", 6000.0, LocalDateTime.now())
    )

    val rowsT1 = Seq(
      Seq(4, "Ram", 1000.0, LocalDateTime.now()),
      Seq(5, "Shyam", 2000.0, LocalDateTime.now()),
      Seq(6, "Krishna", 3000.0, LocalDateTime.now())
    )

    // Create an instance of BufferedWriterPromise
    val bufferedWriter = new AsyncBufferedWriter(5, 1000, "192.168.52.194", 5432, "postgres", "postgres", "postgres")

    // Insert the rows and handle the futures
    val f1 = bufferedWriter.insert(insertSql, rowsT0)
    f1.onComplete {
      case Success(at) => println(s"First batch: Successfully inserted at $at")
      case Failure(ex) => println(s"First batch: Failed to insert rows - ${ex.getMessage}")
    }

    val f2 = bufferedWriter.insert(insertSql, rowsT1)
    f2.onComplete {
      case Success(at) => println(s"Second batch: Successfully inserted at $at")
      case Failure(ex) => println(s"Second batch: Failed to insert rows - ${ex.getMessage}")
    }

    // Wait for both futures to complete
    import scala.concurrent.duration._
    Await.ready(Future.sequence(Seq(f1, f2)), 1.minute)

    bufferedWriter.shutdown()
  }

  runUseCase()
  es.shutdown()
}