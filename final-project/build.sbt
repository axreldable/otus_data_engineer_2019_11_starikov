name := "final-project"
organization in ThisBuild := "ru.star"
scalaVersion in ThisBuild := "2.11.12"
parallelExecution in ThisBuild := false
logLevel := Level.Warn

lazy val global = project
  .in(file("."))
  .settings(version := "1.0.0")
  .aggregate(
    input_adapter,
    generator,
    reader,
    input_adapter_spark
  )

lazy val input_adapter = project
  .in(file("input-adapter"))
  .settings(
    name := "input-adapter",
    version := "1.0.0",
    assemblySettings,
    libraryDependencies ++= Seq(
      Dependencies.flinkCore,
      Dependencies.flinkStreaming,
      Dependencies.flinkConnectorKafka,
    )
  )

lazy val generator = project
  .in(file("generator"))
  .settings(
    name := "generator",
    version := "1.0.0",
    assemblySettings,
    libraryDependencies ++= Seq(
      Dependencies.sparkCore % Provided,
      Dependencies.sparkSql % Provided,
      Dependencies.kafkaClients,
      Dependencies.logback,
      Dependencies.typesafeLogging,
    )
  )

lazy val reader = project
  .in(file("reader"))
  .settings(
    name := "reader",
    version := "1.0.0",
    assemblySettings,
    libraryDependencies ++= Seq(
      Dependencies.sparkCore % Provided,
      Dependencies.sparkSql % Provided,
      Dependencies.kafkaClients,
      Dependencies.logback,
      Dependencies.typesafeLogging,
    )
  )

lazy val input_adapter_spark = project
  .in(file("input-adapter-spark"))
  .settings(
    name := "input-adapter-spark",
    version := "1.0.0",
    assemblySettings,
    libraryDependencies ++= Seq(
      Dependencies.sparkCore % Provided,
      Dependencies.sparkSql % Provided,
      Dependencies.sparkStreaming % Provided,
      Dependencies.sparkStreamingKafka % Provided,
      Dependencies.kafkaClients,
      Dependencies.logback,
      Dependencies.typesafeLogging,
    )
  )

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + "_" + version.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
)
