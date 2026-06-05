ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "0.1.0"
ThisBuild / organization := "ru.itmo.bigdata"

lazy val sparkVersion = "3.4.0"
lazy val sparkJava17Options = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "big-data-lab8-k8s-datamart",
    Compile / mainClass := Some("DataMartApp"),
    run / fork := true,
    Test / fork := true,
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked"),
    javaOptions ++= Seq("-Xms512M", "-Xmx1G") ++ sparkJava17Options,
    Test / javaOptions ++= sparkJava17Options,
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
