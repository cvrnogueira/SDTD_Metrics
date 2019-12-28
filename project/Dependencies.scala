import sbt._

/**
  * Project dependencies.
  */
object Dependencies {

  object Version {
    val logback = "1.2.3"
    val akkaHttp = "10.0.11"
    val akkaHttpCore = "10.0.11"
    val akkaActor = "2.4.20"
    val akkaHttpSpray = "10.0.11"
    val akkaStream = "2.4.20"
    val akkaSlf4j = "2.4.20"
    val slf4j = "1.7.28"
    val log4j = "1.2.17"
    val scalaLogging = "3.9.2"
    val cassandra = "3.0.8"
  }

  lazy val akka = Seq(
    "com.typesafe.akka" %% "akka-http" % Version.akkaHttp,
    "com.typesafe.akka" %% "akka-stream" % Version.akkaStream,
    "com.typesafe.akka" %% "akka-http-spray-json" % Version.akkaHttpSpray,
    "com.typesafe.akka" %% "akka-http-core" % Version.akkaHttpCore,
    "com.typesafe.akka" %% "akka-actor" % Version.akkaActor,
    "com.typesafe.akka" %% "akka-stream" % Version.akkaStream,
    "com.typesafe.akka" %% "akka-slf4j" % Version.akkaSlf4j
  )

  val logging = Seq(
    "org.slf4j" % "slf4j-api" % Version.slf4j,
    "org.slf4j" % "slf4j-log4j12" % Version.slf4j,
    "log4j" % "log4j" % Version.log4j,
    "com.typesafe.scala-logging" %% "scala-logging" % Version.scalaLogging
  )

  val cassandra = Seq(
    "com.datastax.cassandra" % "cassandra-driver-core" % Version.cassandra,
    "com.datastax.cassandra" % "cassandra-driver-mapping" % Version.cassandra
  )

  lazy val all = logging ++ akka ++ cassandra
}