# Notes

## Implementation Notes
* BufferedWriter needs to expose `shutdown` routine. The executor cannot be closed before the Futures are complete. If
  the client closes the executor early, the Futures will fail to run
* Exceptions while running the Future are captured with the `onComplete` on the Future. That will ensure that the Future
  completes gracefully with the error
* With the current implementation, a fast caller will make the BufferedWrite a fail a lot with connection exhaustion
  errors from the Hikari connection pool. There are only 10 connections and LOTS of Futures get created in a very short
  time span. They will all try to get a connection but eventually the Hikari connection pool timeout will fail
  `connectionPool.getConnection()`, and the Future will fail gracefully because of a Hikari exception
* Each Future in the BufferedWriter now needs a connection to the PG. That's why we need a connection pool. Sizing of
  the pool and connection wait timeout now become important. Note that without a connection pool, we will have a
  situation where each connection with either need to use the same connection - defeting the purpose of the
  BufferedWriter and making it serial, or each Future will need to have its own connection, potentially leading to 100's
  to concurreny connections to PG - which is not a good way to do things in the real world.

## Questions?

* Can the BufferedWriter and Client application share the execution context, and/or executors. Should they?

# Build and Run

```bash
 $ sbt clean assembly && java -cp target/scala-2.13/BufferedWriter-assembly-0.1.0-SNAPSHOT.jar lib.BufferedWriterUnitTest 2>&1  | tee out
 $ sbt clean assembly && java -cp target/scala-2.13/BufferedWriter-assembly-0.1.0-SNAPSHOT.jar BufferedWriterScaleTester 2>&1  | tee out
```

