package com.example.utils

import com.typesafe.config.{Config, ConfigFactory, ConfigException}
import org.slf4j.LoggerFactory
import java.nio.file.Paths

object ConfigLoader {
  private val logger = LoggerFactory.getLogger(getClass)
  private val config: Config = loadConfig()
  
  // Load configuration with fallback to default
  private def loadConfig(): Config = {
    try {
      val conf = ConfigFactory.load()
      
      // Resolve any ${user.dir} placeholders
      val resolvedConfig = ConfigFactory.parseString(
        conf.root().render()
          .replace("${user.dir}", Paths.get(".").toAbsolutePath.normalize.toString)
      ).withFallback(conf).resolve()
      
      logger.info("Configuration loaded successfully")
      resolvedConfig
    } catch {
      case e: Exception =>
        logger.error(s"Failed to load configuration: ${e.getMessage}", e)
        throw new RuntimeException("Failed to load application configuration", e)
    }
  }

  private def getRequiredString(path: String): String = {
    try {
      config.getString(path)
    } catch {
      case e: ConfigException.Missing =>
        logger.error(s"Missing required configuration: $path")
        throw new RuntimeException(s"Missing required configuration: $path", e)
      case e: ConfigException.WrongType =>
        logger.error(s"Invalid configuration type for: $path")
        throw new RuntimeException(s"Invalid configuration type for: $path", e)
    }
  }

  private def getRequiredLong(path: String): Long = {
    try {
      config.getLong(path)
    } catch {
      case e: ConfigException.Missing =>
        logger.error(s"Missing required configuration: $path")
        throw new RuntimeException(s"Missing required configuration: $path", e)
      case e: ConfigException.WrongType =>
        logger.error(s"Invalid configuration type for: $path")
        throw new RuntimeException(s"Invalid configuration type for: $path", e)
    }
  }

  private def getRequiredInt(path: String): Int = {
    try {
      config.getInt(path)
    } catch {
      case e: ConfigException.Missing =>
        logger.error(s"Missing required configuration: $path")
        throw new RuntimeException(s"Missing required configuration: $path", e)
      case e: ConfigException.WrongType =>
        logger.error(s"Invalid configuration type for: $path")
        throw new RuntimeException(s"Invalid configuration type for: $path", e)
    }
  }

  object Kafka {
    val bootstrapServers: String = getRequiredString("kafka.bootstrap.servers")
    val groupId: String = getRequiredString("kafka.group.id")
    val topic: String = getRequiredString("kafka.topic")

    // Validate Kafka configuration
    if (!bootstrapServers.contains(":")) {
      logger.warn("Kafka bootstrap servers might be incorrectly configured")
    }
  }

  object Iceberg {
    val warehouse: String = getRequiredString("iceberg.warehouse")
    val catalogType: String = getRequiredString("iceberg.catalog.type")
    val tableName: String = getRequiredString("iceberg.table.name")
    val tableNamespace: String = getRequiredString("iceberg.table.namespace")

    // Validate warehouse path
    if (!warehouse.startsWith("hdfs://") && !warehouse.startsWith("file://")) {
      logger.warn(s"Iceberg warehouse path might be incorrectly configured: $warehouse")
    }
  }

  object Flink {
    val checkpointInterval: Long = getRequiredLong("flink.checkpoint.interval")
    val parallelism: Int = getRequiredInt("flink.parallelism")

    // Validate Flink configuration
    if (checkpointInterval <= 0) {
      logger.warn("Flink checkpoint interval should be positive")
    }
    if (parallelism <= 0) {
      logger.warn("Flink parallelism should be positive")
    }
  }

  // Log configuration on startup
  logger.info("Configuration loaded...")
  logger.info(s"Kafka bootstrap servers: ${Kafka.bootstrapServers}")
  logger.info(s"Kafka topic: ${Kafka.topic}")
  logger.info(s"Iceberg warehouse: ${Iceberg.warehouse}")
  logger.info(s"Flink checkpoint interval: ${Flink.checkpointInterval}ms")
  logger.info(s"Flink parallelism: ${Flink.parallelism}")
} 