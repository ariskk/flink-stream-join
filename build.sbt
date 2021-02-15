ThisBuild / scalaVersion     := "2.12.11"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.ariskk"

lazy val flinkVersion = "1.12.1"
lazy val libs = Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-test-utils" % flinkVersion % Test,
  "org.scalatest" %% "scalatest" % "3.2.3" % Test
)

lazy val root = (project in file("."))
  .settings(
    name := "flink-stream-join",
    libraryDependencies ++= libs
  )

