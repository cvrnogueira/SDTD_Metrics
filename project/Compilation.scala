import sbt.Keys.{libraryDependencies, _}
import sbt._
import sbtassembly.AssemblyKeys._
import sbtassembly.{MergeStrategy, PathList}


/**
  * Project compilation settings.
  */
object Compilation {

  lazy val apiName = "sdtd-metrics"
  lazy val suffix = ".jar"

  lazy val buildSettings = Seq(
    name := apiName,
    organization := "io.ensimag.sdtd.metrics",
    version := "1.0.0"
  )

  lazy val settings = Seq(
    // force java target and source versions
    javacOptions ++= Seq("-source", "1.11", "-target", "1.11"),

    // set scala version
    ThisBuild / scalaVersion := "2.12.6",

    // add project dependencies
    libraryDependencies ++= Dependencies.all,

    // set project main entry point class
    mainClass := Some("io.ensimag.sdtd.metrics.Application"),

    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", _@_*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat 
      case _ => MergeStrategy.first
    },
  )
}
