import sbt._
import sbt.Keys._

object SparkSqlFormats extends Build {
  override lazy val settings = super.settings ++ Seq(
    organization := "com.github.upio",
    scalaVersion := "2.11.8",
    scalacOptions += "-target:jvm-1.8",
    version := "0.1",
    resolvers ++= Seq(
      "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases/"
    ),
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % "2.3.2",
      "org.apache.spark" %% "spark-core" % "1.3.1",
      "org.apache.spark" %% "spark-sql" % "1.3.1",

      "org.mockito" % "mockito-all" % "1.10.19" % "test",
      "org.scalatest" %% "scalatest" % "2.2.6" % "test"
    )
  )
}
