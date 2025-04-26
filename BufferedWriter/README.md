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
* A BufferedWriter with a ListBuffer has no backpressure and can easily get swamped and made useless but a runaway
  client

* There are two clients in the package now 
    ** legacy - older clients that use a simple Future based multi-thread worker threads, where each is producing
    independent of the other, at a rate that's only limited by a random 1 - 10ms delay between each insert call
    ** ratedefined - this uses a nice producer and consumer model. This allows us to control the overall rate of
    insertions! It also has a lot of knobs to control the traffic pattern and the queue and batch sizes etc.

## Questions?

* Can the BufferedWriter and Client application share the execution context, and/or executors. Should they?

# Build and Run

```bash
 $ sbt clean assembly && java -cp target/scala-2.13/BufferedWriter-assembly-0.1.0-SNAPSHOT.jar lib.BufferedWriterUnitTest 2>&1  | tee out
 $ sbt clean assembly && java -cp target/scala-2.13/BufferedWriter-assembly-0.1.0-SNAPSHOT.jar BufferedWriterScaleTester 2>&1  | tee out
```

or

```bash
~/wkspcs/small-projects/BufferedWriter (main)
>> sbt
[info] welcome to sbt 1.10.10 (Amazon.com Inc. Java 21.0.6)
[info] loading settings for project bufferedwriter-build from plugins.sbt...
[info] loading project definition from /Users/gmathur/wkspcs/small-projects/BufferedWriter/project
[info] loading settings for project root from build.sbt...
[info] set current project to BufferedWriter (in build file:/Users/gmathur/wkspcs/small-projects/BufferedWriter/)
[info] sbt server started at local:///Users/gmathur/.sbt/1.0/server/5d53c81b09b845884548/sock
[info] started sbt server
sbt:BufferedWriter> run

Multiple main classes detected. Select one to run:
 [1] lib.BufferedWriterUnitTest
 [2] tester.legacy.BufferedWriterScaleTester
 [3] tester.legacy.BufferedWriterScaleTester2
 [4] tester.ratedefined.RateDefinedBufferedWriterTester

Enter number:

```
