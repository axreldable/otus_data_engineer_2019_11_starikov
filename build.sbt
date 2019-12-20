name := "scala-hw"
organization in ThisBuild := "ru.star"
scalaVersion in ThisBuild := "2.11.12"
parallelExecution in ThisBuild := false
logLevel := Level.Warn

lazy val global = project
  .in(file("."))
  .settings(version := "1.0.0")
  .aggregate(
    hw_3
  )

lazy val hw_3 = project
  .in(file("hw-3"))
  .settings(
    name := "hw-3",
    version := "1.0.0",
    assemblySettings,
    libraryDependencies ++= Seq(
      Dependencies.sparkCore % Provided,
      Dependencies.sparkSql % Provided,
      Dependencies.scalaTest % Test
    )
  )


lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + "_" + version.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case _                           => MergeStrategy.first
  }
)
