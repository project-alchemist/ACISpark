ThisBuild / description := "Alchemist-Client Interface for Apache Spark"
ThisBuild / organization := "alchemist"
ThisBuild / version := "0.5"
ThisBuild / scalaVersion := "2.11.12"
ThisBuild / autoStartServer := false

lazy val SparkVersion      = "2.3.3"
lazy val SparkTestVersion  = s"${SparkVersion}_0.12.0"
lazy val EnumeratumVersion = "1.5.13"
lazy val ScoptVersion      = "3.7.1"
lazy val ScalaTestVersion  = "3.0.7"

lazy val testSettings = Seq(
  parallelExecution in Test := false,
  fork in Test := true
)

lazy val compilerSettings = Seq(
  javacOptions ++= Seq(
    "-source",
    "1.8",
    "-target",
    "1.8",
    "-Xms512M",
    "-Xmx2048M",
    "-XX:MaxPermSize=2048M",
    "-XX:+CMSClassUnloadingEnabled"
  ),
  scalacOptions ++= Seq("-encoding", "utf-8", "-deprecation", "-unchecked", "-feature")
)

lazy val fmtSettings = Seq(
  scalafmtOnCompile := true
)

lazy val `alchemist-core` = (project in file("modules/core"))
  .settings(testSettings, compilerSettings, fmtSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-mllib"        % SparkVersion % Provided,
      "com.beachape"     %% "enumeratum"         % EnumeratumVersion,
      "org.scalatest"    %% "scalatest"          % ScalaTestVersion % Test,
      "com.holdenkarau"  %% "spark-testing-base" % SparkTestVersion % Test
    )
  )

lazy val `alchemist-example` = (project in file("modules/example"))
  .settings(testSettings, compilerSettings, fmtSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-mllib" % SparkVersion % Provided,
      "com.github.scopt" %% "scopt"       % ScoptVersion
    ),
    run in Compile := Defaults
      .runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
      .evaluated
  )
  .dependsOn(`alchemist-core`)

lazy val alchemist = (project in file("."))
  .disablePlugins(AssemblyPlugin, ScalafmtPlugin)
  .aggregate(`alchemist-core`, `alchemist-example`)
