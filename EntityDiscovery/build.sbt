import scala.concurrent.duration.DurationInt

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.11"

lazy val root = (project in file("."))
  .settings(
    name := "EntityDiscovery"
  )

resolvers ++= Seq(
  "Sonatype Snapshots" at "https://s01.oss.sonatype.org/content/repositories/snapshots/"
)

libraryDependencies += "org.scala-lang" % "scala-reflect" % scalaVersion.value

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.4.0"
libraryDependencies += "org.apache.spark" %% "spark-catalyst" % "3.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.4.0"

libraryDependencies += "edu.upc.essi.dtim" % "nextiadi" % "0.1.1-SNAPSHOT"
//libraryDependencies += "io.github.haross.nextia" % "nextiajd_2.12" % "0.1.0-SNAPSHOT"
libraryDependencies += "io.github.haross.nuup" % "nuup_2.12" % "0.1.0-SNAPSHOT"

libraryDependencies += "javax.json" % "javax.json-api" % "1.1.4"
libraryDependencies += "org.glassfish" % "javax.json" % "1.1.4"

libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.7.8"

libraryDependencies += "com.databricks" %% "spark-xml" % "0.16.0"


//forceUpdatePeriod := Some(0 seconds)
ThisBuild / forceUpdatePeriod := Some(0.seconds)
updateOptions := updateOptions.value.withLatestSnapshots(true)
