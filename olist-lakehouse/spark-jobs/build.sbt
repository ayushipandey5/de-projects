import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

ThisBuild / organization := "com.olist"
ThisBuild / version  := "0.1.0"
ThisBuild / scalaVersion := "2.13.12"

val sparkVersion = "3.5.0"

val current = LocalDateTime.now()
val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")
val dttm = current.format(formatter)

lazy val commonAssemblySettings = Seq(
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
)

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
)

lazy val common = (project in file("common"))
  .settings(
    name := "olist-common",
    libraryDependencies ++= sparkDependencies ++ Seq(
      "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.36.1",
      "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0",
      "com.github.pureconfig" %% "pureconfig" % "0.17.4"
    ),
//    libraryDependencies ++= sparkDependencies

  )


lazy val orders = (project in file("pipelines/orders"))
  .dependsOn(common)
  .settings(
    commonAssemblySettings,
    libraryDependencies ++= sparkDependencies,
    name := "olist-orders-pipeline",
    assembly / mainClass := Some("com.olist.pipelines.orders.Main"),
    assembly / assemblyJarName := s"orders-pipeline-$dttm.jar"
  )

lazy val customers = (project in file("pipelines/customers"))
  .dependsOn(common)
  .settings(
    commonAssemblySettings,
    libraryDependencies ++= sparkDependencies,
    name := "olist-customers-pipeline",
    assembly / mainClass := Some("com.olist.pipelines.customers.Main"),
    assembly / assemblyJarName := s"customers-pipeline-$dttm.jar"
  )