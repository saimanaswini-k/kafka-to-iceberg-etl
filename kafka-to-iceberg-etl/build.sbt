ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.15"
ThisBuild / organization := "com.example"

val flinkVersion = "1.17.1"
val json4sVersion = "4.0.6"
val hadoopVersion = "3.3.6"
val icebergVersion = "1.5.0"

lazy val root = (project in file("."))
  .settings(
    name := "iceberg-kafka",
    resolvers ++= Seq(
      "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
      "Apache Release Repository" at "https://repository.apache.org/content/repositories/releases/",
      "Maven Central" at "https://repo1.maven.org/maven2/",
      Resolver.mavenLocal
    ),
    updateOptions := updateOptions.value.withGigahorse(false),
    updateOptions := updateOptions.value.withCachedResolution(true),
    libraryDependencies ++= Seq(
      "org.apache.flink" % "flink-streaming-java" % flinkVersion,
      "org.apache.flink" %% "flink-scala" % flinkVersion % Provided,
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % Provided,
      "org.apache.flink" % "flink-clients" % flinkVersion % Provided,
      "org.apache.kafka" % "kafka-clients" % "3.6.1" % Provided,
      "org.apache.flink" % "flink-table-common" % flinkVersion % Provided,
      "org.apache.flink" %% "flink-table-api-scala-bridge" % flinkVersion % Provided,
      "org.apache.iceberg" % s"iceberg-flink-runtime-1.17" % icebergVersion,
      "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
      "org.json4s" %% "json4s-core" % json4sVersion,
      "org.json4s" %% "json4s-jackson" % json4sVersion,
      "org.json4s" %% "json4s-ext" % json4sVersion,
      "com.typesafe" % "config" % "1.4.2",
      "org.slf4j" % "slf4j-api" % "1.7.32",
      "org.slf4j" % "slf4j-log4j12" % "1.7.36",
      "log4j" % "log4j" % "1.2.17",
      "org.apache.spark" %% "spark-core" % "3.5.3" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.3" % "provided",
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % "1.5.0",
      "joda-time" % "joda-time" % "2.10.14",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
      "ch.qos.logback" % "logback-classic" % "1.2.10" % Runtime,
      "org.apache.flink" % "flink-connector-kafka" % flinkVersion,
      "org.apache.flink" % "flink-sql-connector-kafka" % flinkVersion
    ),
    
    // Add exclusion rule for the problematic dependency
    excludeDependencies += "io.netty" % "netty-transport-native-epoll",
    
    // Add the appropriate Netty transport for macOS
    libraryDependencies += {
      val osArch = System.getProperty("os.arch").toLowerCase
      val classifier = if (osArch.contains("aarch64")) "osx-aarch_64" else "osx-x86_64"
      "io.netty" % "netty-transport-native-kqueue" % "4.1.96.Final" classifier classifier
    },

    // Add dependency exclusions to prevent version conflicts
    dependencyOverrides ++= Seq(
      "org.json4s" %% "json4s-core" % json4sVersion,
      "org.json4s" %% "json4s-jackson" % json4sVersion,
      "org.json4s" %% "json4s-ext" % json4sVersion,
      "org.json4s" %% "json4s-ast" % json4sVersion,
      "org.json4s" %% "json4s-scalap" % json4sVersion,
      "joda-time" % "joda-time" % "2.10.14"
    )
  )

// Java 17 compatibility settings
javacOptions ++= Seq("-source", "17", "-target", "17")
scalacOptions ++= Seq("-release", "17")

// Assembly settings for creating a fat JAR
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf" => MergeStrategy.concat
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

// Don't run tests during assembly
assembly / test := {}

// Main class setting
assembly / mainClass := Some("com.example.KafkaToIcebergJob") 