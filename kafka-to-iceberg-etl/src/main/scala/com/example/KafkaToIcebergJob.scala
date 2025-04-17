package com.example

import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.CheckpointConfig
import com.example.utils.ConfigLoader
import org.slf4j.LoggerFactory
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.hadoop.conf.Configuration
import org.apache.flink.table.data.RowData
import java.nio.file.Paths

// Import the moved classes
import com.example.serializers.JsonRecordDeserializer
import com.example.mappers.JsonToRowDataMapper
import com.example.filters.NonNullFilter
import com.example.services.IcebergTableService

/**
 * Main job class that connects Kafka to Iceberg.
 * Reads data from Kafka, transforms it, and writes to Iceberg table.
 */
object KafkaToIcebergJob {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    try {
      logger.info("========== STARTING KAFKA TO ICEBERG JOB ==========")
      logger.info(s"Configuration: bootstrap-servers=${ConfigLoader.Kafka.bootstrapServers}, " +
                  s"topic=${ConfigLoader.Kafka.topic}, " +
                  s"warehouse=${ConfigLoader.Iceberg.warehouse}")
      
      // Set up the execution environment
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val parallelism = ConfigLoader.Flink.parallelism
      env.setParallelism(parallelism)
      logger.info(s"Setting Flink parallelism to: $parallelism")

      // Get project root directory for checkpoint path
      val projectRoot = Paths.get(".").toAbsolutePath.normalize.toString
      val checkpointPath = s"file://$projectRoot/data/checkpoints"
      
      // Configure checkpointing
      val checkpointInterval = ConfigLoader.Flink.checkpointInterval
      val checkpointConfig = env.getCheckpointConfig
      checkpointConfig.setCheckpointInterval(checkpointInterval)
      checkpointConfig.setCheckpointStorage(checkpointPath)
      logger.info(s"Checkpoint configuration: path=$checkpointPath, interval=${checkpointInterval}ms")
      
      // Create Kafka source with custom deserializer
      logger.info(s"Creating Kafka source for topic: ${ConfigLoader.Kafka.topic}")
      val kafkaSource = createKafkaSource()

      // Process stream
      logger.info("Building Flink data processing pipeline")
      val kafkaStream = env.fromSource(
        kafkaSource,
        WatermarkStrategy.noWatermarks(),
        "Kafka Source"
      ).map(new JsonToRowDataMapper)
       .filter(new NonNullFilter)
      logger.debug("Pipeline created with mappers and filters")

      // Configure Iceberg
      logger.info("Configuring Iceberg components")
      val hadoopConf = new Configuration()
      
      // Create the schema and ensure table exists
      val schema = IcebergTableService.createTaxiDataSchema()
      logger.debug(s"Iceberg schema created with ${schema.columns().size()} columns")
      
      // Ensure table exists
      val tablePath = IcebergTableService.getTablePath()
      val tableCreated = IcebergTableService.ensureTableExists(hadoopConf, schema)
      if (tableCreated) {
        logger.info(s"Created new Iceberg table at: $tablePath")
      } else {
        logger.info(s"Using existing Iceberg table at: $tablePath")
      }
      
      // Write to Iceberg
      logger.info("Configuring Iceberg sink")
      IcebergTableService.configureSink(kafkaStream.javaStream, hadoopConf)

      // Execute the job
      logger.info("Executing Flink job - Kafka to Iceberg pipeline started")
      env.execute("Kafka to Iceberg Job")
    } catch {
      case e: Exception =>
        logger.error(s"FATAL ERROR: Job failed with exception: ${e.getMessage}", e)
        logger.error("Stack trace: " + e.getStackTrace.mkString("\n  "))
        throw e
    }
  }
  
  /**
   * Creates a configured Kafka source for the pipeline
   * @return Configured KafkaSource
   */
  private def createKafkaSource(): KafkaSource[String] = {
    val bootstrapServers = ConfigLoader.Kafka.bootstrapServers
    val topic = ConfigLoader.Kafka.topic
    val groupId = ConfigLoader.Kafka.groupId
    
    logger.debug(s"Creating Kafka source with: servers=$bootstrapServers, topic=$topic, groupId=$groupId")
    
    KafkaSource.builder[String]()
      .setBootstrapServers(bootstrapServers)
      .setTopics(topic)
      .setGroupId(groupId)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setDeserializer(new JsonRecordDeserializer())
      .setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .setProperty("auto.offset.reset", "earliest")
      .setProperty("enable.auto.commit", "false")
      .build()
  }
} 