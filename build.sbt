
ThisBuild / description := "Alchemist-Client Interface for Apache Spark"
ThisBuild / organization := "alchemist"
ThisBuild / version := "0.5"
ThisBuild / scalaVersion := "2.11.12"

lazy val SparkVersion = "2.3.2"

lazy val `alchemist-example` = (project in file("example"))
  .dependsOn(`alchemist`)
  .settings(

    target := { baseDirectory.value / "target" },

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-mllib" % SparkVersion % Provided
    ),

    run in Compile := Defaults
      .runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
      .evaluated
  )

lazy val `alchemist` = (project in file("."))
  .settings(

    target := { baseDirectory.value / "target" },

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-mllib" % SparkVersion % Provided,
      "com.beachape" %% "enumeratum" % "1.5.13"
    ),

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),

    parallelExecution in Test := false,
    fork in Test := true
  )
