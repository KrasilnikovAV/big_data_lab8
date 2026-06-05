ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "ru.itmo.bigdata"

lazy val sparkVersion = "3.4.0"

lazy val root = (project in file("."))
  .settings(
    name := "big-data-lab8-k8s-datamart",
    Compile / mainClass := Some("DataMartApp"),
    run / fork := true,
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
    javaOptions ++= Seq("-Xms512M", "-Xmx1G"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
      "com.mysql" % "mysql-connector-j" % "8.4.0",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Test / parallelExecution := false,
    assembly / assemblyJarName := "big-data-lab8-k8s-datamart.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
  )
