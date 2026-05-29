import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

ThisBuild / organization := "com.olist"
ThisBuild / version  := "0.1.0"
ThisBuild / scalaVersion := "2.13.12"

val sparkVersion = "3.5.0"
val icebergVersion = "1.9.2"

val current = LocalDateTime.now()
val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
val dttm = current.format(formatter)

lazy val commonAssemblySettings = Seq(
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", "versions", xs @ _*) => MergeStrategy.discard
    case "mozilla/public-suffix-list.txt" => MergeStrategy.first
    case PathList("mozilla", "public-suffix-list.txt") => MergeStrategy.first
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first

    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case PathList("META-INF", "services", _*) => MergeStrategy.filterDistinctLines

    case x =>
      val oldStrategy = (assembly / assemblyMergeStrategy).value
      oldStrategy(x)
  }
)

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
)

lazy val common = (project in file("common"))
  .settings(
    commonAssemblySettings,
    name := "olist-common",
    libraryDependencies ++= sparkDependencies ++ Seq(
      "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.36.1" % "provided",
      "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % icebergVersion,
      "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0",
      "com.github.pureconfig" %% "pureconfig" % "0.17.4",
      "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.5"
    ),
//    libraryDependencies ++= sparkDependencies

  )


lazy val type1_scd = (project in file("pipelines/type1_scd"))
  .dependsOn(common)
  .settings(
    commonAssemblySettings,
    libraryDependencies ++= sparkDependencies,
    name := "olist-type1-scd-pipeline",
    assembly / mainClass := Some("com.olist.pipelines.type1_scd.Main"),
    assembly / assemblyJarName := s"type1-scd-dpp-$dttm.jar"
  )


lazy val orders = (project in file("pipelines/orders"))
  .dependsOn(common)
  .settings(
    commonAssemblySettings,
    libraryDependencies ++= sparkDependencies,
    name := "olist-orders-pipeline",
    assembly / mainClass := Some("com.olist.pipelines.orders.Main"),
    assembly / assemblyJarName := s"orders-dpp-$dttm.jar"
  )


lazy val reference_dimensions = (project in file("pipelines/reference_dimensions"))
  .dependsOn(common)
  .settings(
    commonAssemblySettings,
    libraryDependencies ++= sparkDependencies,
    name := "olist-reference-dimensions-pipeline",
    assembly / mainClass := Some("com.olist.pipelines.reference_dimensions.Main"),
    assembly / assemblyJarName := s"reference-dimensions-dpp-$dttm.jar"
  )

lazy val order_payments = (project in file("pipelines/order_payments"))
  .dependsOn(common)
  .settings(
    commonAssemblySettings,
    libraryDependencies ++= sparkDependencies,
    name := "olist-order-payments-pipeline",
    assembly / mainClass := Some("com.olist.pipelines.order_payments.Main"),
    assembly / assemblyJarName := s"order-payments-dpp-$dttm.jar"
  )

lazy val order_items = (project in file("pipelines/order_items"))
  .dependsOn(common)
  .settings(
    commonAssemblySettings,
    libraryDependencies ++= sparkDependencies,
    name := "olist-order-items-pipeline",
    assembly / mainClass := Some("com.olist.pipelines.order_items.Main"),
    assembly / assemblyJarName := s"order-items-dpp-$dttm.jar"
  )

lazy val order_reviews = (project in file("pipelines/order_reviews"))
  .dependsOn(common)
  .settings(
    commonAssemblySettings,
    libraryDependencies ++= sparkDependencies,
    name := "olist-order-reviews-pipeline",
    assembly / mainClass := Some("com.olist.pipelines.order_reviews.Main"),
    assembly / assemblyJarName := s"order-reviews-dpp-$dttm.jar"
  )