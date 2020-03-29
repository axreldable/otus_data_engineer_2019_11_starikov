import sbt._

object Dependencies {
  lazy val sparkVersion = "2.4.0"
  lazy val flinkVersion = "1.10.0"

  lazy val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

  lazy val scopt = "com.github.scopt" %% "scopt" % "3.7.0"

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
  lazy val sparkTestingBase = "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.12.0"

  lazy val flinkCore = "org.apache.flink" % "flink-core" % flinkVersion
  lazy val flinkStreaming = "org.apache.flink" %% "flink-streaming-scala" % flinkVersion
  lazy val flinkConnectorKafka = "org.apache.flink" %% "flink-connector-kafka" % flinkVersion

  lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % "0.10.0.0"
  lazy val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
  lazy val typesafeLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
  lazy val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion
  lazy val sparkStreamingKafka = "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
  lazy val sparkStreamingKafkaAssembly = "org.apache.spark" %% "spark-streaming-kafka-0-10-assembly" % sparkVersion
}
