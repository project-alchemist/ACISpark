
ThisBuild / description := "Alchemist-Client Interface for Apache Spark"
ThisBuild / organization := "alchemist"
ThisBuild / version := "0.5"
ThisBuild / scalaVersion := "2.11.12"

lazy val SparkVersion = "2.3.2"

lazy val `aci-spark` = (project in file("."))
  .settings(

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-mllib" % SparkVersion % Provided,
      "com.beachape" %% "enumeratum" % "1.5.13"
    ),

    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),

    parallelExecution in Test := false,
    fork in Test := true
  )

lazy val `aci-spark-example` = (project in file("example"))
  .settings(

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-mllib" % SparkVersion % Provided
    ),

    run in Compile := Defaults
      .runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))
      .evaluated
  ).dependsOn(`aci-spark`)
