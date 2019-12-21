import sbt._

object Dependencies {

  lazy val sparkVersion = "2.4.3"

  lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

  lazy val json4Jackson = "org.json4s" %% "json4s-jackson" % "3.6.7"

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.12.0"

  // Here because of a problem:
  // Caused by: com.fasterxml.jackson.databind.JsonMappingException: Incompatible Jackson version: 2.9.8
  // https://stackoverflow.com/questions/45869367/
  // spark-unit-test-fails-with-exceptionininitializererror-with-latest-version-of
  lazy val jacksonModuleForTest = "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.8"

}
