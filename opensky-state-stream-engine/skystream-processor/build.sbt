ThisBuild / scalaVersion := "2.13.12"
val kafkaVersion = "3.7.0"

resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"

lazy val root = (project in file("."))
  .settings(
    name := "skystream-processor",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
      "com.lihaoyi" %% "upickle" % "3.1.0",
      "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.20.0",
      "com.github.pureconfig" %% "pureconfig" % "0.17.4"
    ),
      assembly / mainClass := Some("skystreamprocessor.Main")
  )