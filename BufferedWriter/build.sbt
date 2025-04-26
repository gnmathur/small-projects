ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "BufferedWriter"
  )

// Postgres
libraryDependencies += "org.postgresql" % "postgresql" % "42.6.0"

// Hikari CP
libraryDependencies += "com.zaxxer" % "HikariCP" % "6.3.0"

// Logging - Consolidated and made more explicit
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "2.0.13",       // SLF4J API (Facade)
  "ch.qos.logback" % "logback-classic" % "1.5.6" // Logback Implementation (for SLF4J 2.x)
)

// Enable assembly plugin
enablePlugins(AssemblyPlugin)

// Merge strategy (important to avoid META-INF conflicts)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "module-info.class" => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Optional: set main class manually
mainClass in assembly := Some("BufferedWriterTest")

// Ensure runtime dependencies are included
run / fork := true