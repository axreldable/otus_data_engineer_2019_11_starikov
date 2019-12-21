import sbt._

object Dependencies {

  lazy val sparkVersion = "2.4.4"

  lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

  lazy val json4Jackson = "org.json4s" %% "json4s-jackson" % "3.6.7"

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
}
