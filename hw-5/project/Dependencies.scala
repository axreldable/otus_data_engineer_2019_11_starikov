import sbt._

object Dependencies {

  lazy val sparkVersion = "2.4.3"

  lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

  lazy val scopt = "com.github.scopt" %% "scopt" % "3.7.0"

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.12.0"
}
