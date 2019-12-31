name := "scala-hw"
organization in ThisBuild := "ru.star"
scalaVersion in ThisBuild := "2.11.12"
parallelExecution in ThisBuild := false
logLevel := Level.Warn

lazy val global = project
  .in(file("."))
  .settings(version := "1.0.0")
  .aggregate(
    hw_4,
    hw_5
  )

lazy val hw_4 = project
  .in(file("hw-4"))
  .settings(
    name := "hw-4",
    version := "1.0.0",
    assemblySettings,
    libraryDependencies ++= Seq(
      Dependencies.sparkCore % Provided,
      Dependencies.sparkSql % Provided,
      Dependencies.json4Jackson,
      Dependencies.scalaTest % Test,
      Dependencies.sparkTestingBase % Test,
      Dependencies.jacksonModuleForTest % Test
    )
  )

lazy val hw_5 = project
  .in(file("hw-5"))
  .settings(
    name := "hw-5",
    version := "1.0.0",
    assemblySettings,
    libraryDependencies ++= Seq(
      Dependencies.sparkCore % Provided,
      Dependencies.sparkSql % Provided,
      Dependencies.scopt,
      Dependencies.scalaTest % Test,
      Dependencies.sparkTestingBase % Test
    )
  )


lazy val assemblySettings = Seq(
  assemblyJarName in assembly := name.value + "_" + version.value + ".jar",
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case _                           => MergeStrategy.first
  }
)
