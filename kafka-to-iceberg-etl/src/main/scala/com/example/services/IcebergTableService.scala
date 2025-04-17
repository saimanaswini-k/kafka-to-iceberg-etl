package com.example.services

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.Schema
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.types.Types
import org.slf4j.LoggerFactory
import com.example.utils.ConfigLoader
import org.apache.iceberg.flink.TableLoader
import org.apache.flink.table.data.RowData
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.iceberg.flink.sink.FlinkSink
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConverters._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.example.utils.ConfigLoader
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.{CatalogProperties, Table}
import org.apache.iceberg.flink.{CatalogLoader, FlinkCatalogFactory}
import java.util.{HashMap => JavaHashMap, Map => JavaMap}

/**
 * Service for handling Iceberg table operations
 */
object IcebergTableService {
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Creates the taxi data schema for Iceberg
   * @return Schema object with taxi trip fields
   */
  def createTaxiDataSchema(): Schema = {
    logger.info("Creating Iceberg schema for taxi data")
    val schema = new Schema(
      Types.NestedField.required(1, "tripID", Types.StringType.get()),
      Types.NestedField.required(2, "VendorID", Types.StringType.get()),
      Types.NestedField.required(3, "tpep_pickup_datetime", Types.TimestampType.withoutZone()),
      Types.NestedField.required(4, "tpep_dropoff_datetime", Types.TimestampType.withoutZone()),
      Types.NestedField.required(5, "passenger_count", Types.StringType.get()),
      Types.NestedField.required(6, "trip_distance", Types.StringType.get()),
      Types.NestedField.required(7, "RatecodeID", Types.StringType.get()),
      Types.NestedField.required(8, "store_and_fwd_flag", Types.StringType.get()),
      Types.NestedField.required(9, "PULocationID", Types.StringType.get()),
      Types.NestedField.required(10, "DOLocationID", Types.StringType.get()),
      Types.NestedField.required(11, "payment_type", Types.StringType.get()),
      Types.NestedField.required(12, "primary_passenger", Types.StringType.get()),
      Types.NestedField.required(13, "fare_details", Types.StringType.get())
    )
    
    logger.debug(s"Schema created with ${schema.columns().size()} columns")
    logger.debug(s"Schema columns: ${schema.columns().asScala.map(_.name()).mkString(", ")}")
    schema
  }
  
  /**
   * Gets the Iceberg table path
   * @return Full table path
   */
  def getTablePath(): String = {
    val path = s"${ConfigLoader.Iceberg.warehouse}/${ConfigLoader.Iceberg.tableNamespace}/${ConfigLoader.Iceberg.tableName}"
    logger.debug(s"Computed Iceberg table path: $path")
    path
  }
  
  /**
   * Creates or ensures a table exists with the given schema
   * @param hadoopConf Hadoop configuration
   * @param schema Schema to use for table creation
   * @return True if table was created, false if it already existed
   */
  def ensureTableExists(hadoopConf: Configuration, schema: Schema): Boolean = {
    val catalog = loadHadoopCatalog(hadoopConf)
    val tableIdentifier = getTableIdentifier()
    logger.info(s"Ensuring table exists: ${tableIdentifier.toString}")

    if (!catalog.tableExists(tableIdentifier)) {
      logger.info(s"Table ${tableIdentifier.toString} does not exist. Creating...")
      try {
        val props = getTableProperties()
        logger.debug(s"Table properties: ${props.asScala.map(kv => s"${kv._1}=${kv._2}").mkString(", ")}")
        
        // Create partitioned table by pickup date
        val partitionSpec = PartitionSpec.builderFor(schema)
          .day("tpep_pickup_datetime")
          .build()
        
        logger.debug(s"Partition spec: ${partitionSpec.fields().asScala.map(f => s"${f.name()}").mkString(", ")}")
        
        catalog.createTable(tableIdentifier, schema, partitionSpec, props)
        logger.info(s"Table ${tableIdentifier.toString} created successfully.")
        true
      } catch {
        case e: Exception =>
          logger.error(s"Failed to create Iceberg table at ${tableIdentifier.toString}: ${e.getMessage}", e)
          throw new RuntimeException(s"Failed to create Iceberg table at ${tableIdentifier.toString}", e)
      }
    } else {
      logger.info(s"Table ${tableIdentifier.toString} already exists.")
      
      // Log some metadata about the existing table
      Try {
        val existingSchema = catalog.loadTable(tableIdentifier).schema()
        logger.debug(s"Existing table has ${existingSchema.columns().size()} columns")
        logger.debug(s"Table fields: ${existingSchema.columns().asScala.map(_.name()).mkString(", ")}")
        
        val partitionSpec = catalog.loadTable(tableIdentifier).spec()
        logger.debug(s"Table partition spec: ${partitionSpec.fields().asScala.map(_.name()).mkString(", ")}")
      } match {
        case Success(_) => // Metadata loaded successfully
        case Failure(e) => logger.warn(s"Failed to load table metadata for logging: ${e.getMessage}")
      }
      
      false
    }
  }
  
  /**
   * Get the table properties for optimization
   * @return Java HashMap with table properties
   */
  def getTableProperties(): java.util.HashMap[String, String] = {
    logger.debug("Setting up Iceberg table properties")
    val props = new java.util.HashMap[String, String]()
    props.put("write.target-file-size-bytes", "1048576")   // 1MB files instead of 10MB
    props.put("write.parquet.row-group-size-bytes", "5242880")  // 5MB
    props.put("write.parquet.compression-codec", "zstd")
    props.put("commit.manifest.min-count-to-merge", "1")
    props.put("commit.manifest.target-size-bytes", "8388608")  // 8MB
    props.put("write.format.default", "parquet")
    props.put("write.parquet.page-size-bytes", "1048576")  // 1MB
    props.put("write.parquet.dict-size-bytes", "2097152")  // 2MB
    props.put("write.metadata.delete-after-commit.enabled", "true")
    props.put("write.metadata.previous-versions-max", "1")  // Keep fewer versions
    props
  }
  
  /**
   * Configure the sink to write RowData to Iceberg
   * @param dataStream Stream of RowData to write
   * @param hadoopConf Hadoop configuration
   */
  def configureSink(dataStream: DataStream[RowData], hadoopConf: Configuration): Unit = {
    val tableLoader = createTableLoader(hadoopConf)
    val tableIdentifier = getTableIdentifier()
    logger.info(s"Configuring FlinkSink for table: ${tableIdentifier.toString}")
    
    try {
      FlinkSink.forRowData(dataStream)
        .tableLoader(tableLoader)
        .append()
      
      logger.info("Iceberg sink configured successfully")
    } catch {
      case e: Exception =>
        logger.error(s"Failed to configure Iceberg sink: ${e.getMessage}", e)
        throw new RuntimeException("Failed to configure Iceberg sink", e)
    }
  }

  private def loadHadoopCatalog(hadoopConf: Configuration): Catalog = {
    val warehousePath = ConfigLoader.Iceberg.warehouse
    logger.info(s"Loading HadoopCatalog with warehouse path: $warehousePath")

    val catalog = new HadoopCatalog()
    catalog.setConf(hadoopConf)

    val properties = new JavaHashMap[String, String]()
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath)

    catalog.initialize("hadoop_catalog", properties) 
    
    logger.info("HadoopCatalog loaded successfully.")
    catalog
  }

  private def getTableIdentifier(): TableIdentifier = {
    TableIdentifier.of(ConfigLoader.Iceberg.tableNamespace, ConfigLoader.Iceberg.tableName)
  }

  private def createTableLoader(hadoopConf: Configuration): TableLoader = {
    val tableIdentifier = getTableIdentifier()
    logger.info(s"Creating TableLoader for: ${tableIdentifier.toString}")

    // Define properties for the Hadoop catalog
    val properties = new JavaHashMap[String, String]()
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, ConfigLoader.Iceberg.warehouse)

    // Use CatalogLoader.hadoop to create the loader directly
    val catalogName = "hadoop_catalog" // Provide a name for the catalog instance
    val catalogLoader = CatalogLoader.hadoop(catalogName, hadoopConf, properties)

    // Load the table using the catalog loader
    val loader = TableLoader.fromCatalog(catalogLoader, tableIdentifier)
    logger.info(s"TableLoader created successfully for ${tableIdentifier.toString}")
    loader
  }
}

/**
 * Service class to handle Iceberg table operations with automatic fallback mechanisms
 */
class IcebergTableService(spark: SparkSession) {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // The actual physical location of your Iceberg table
  private val tableLocation = "file:///Users/apple/Desktop/iceberg-kafka/data/warehouse/default/json_data"
  
  // The catalog reference using spark_catalog
  private val catalogTableName = "spark_catalog.default.json_data"
  
  /**
   * Get a DataFrame from the Iceberg table, automatically handling path issues
   */
  def getTable(): DataFrame = {
    try {
      logger.info(s"Attempting to read table via catalog: $catalogTableName")
      val df = spark.table(catalogTableName)
      logger.info(s"Successfully read table via catalog with ${df.count()} records")
      df
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to read via catalog: ${e.getMessage}")
        logger.info(s"Falling back to direct path access: $tableLocation")
        
        try {
          // Check if table exists
          val tables = new org.apache.iceberg.hadoop.HadoopTables(new org.apache.hadoop.conf.Configuration())
          if (tables.exists(tableLocation)) {
            logger.info(s"Table exists at location: $tableLocation")
            val df = spark.read.format("iceberg").load(tableLocation)
            logger.info(s"Successfully read table via direct path with ${df.count()} records")
            df
          } else {
            logger.error(s"No table found at location: $tableLocation")
            spark.emptyDataFrame  // Return empty DataFrame if table doesn't exist
          }
        } catch {
          case ex: Exception => 
            logger.error(s"Error accessing table directly: ${ex.getMessage}")
            spark.emptyDataFrame
        }
    }
  }
  
  /**
   * Execute a SQL query against the table with automatic fallback
   */
  def executeQuery(sqlQuery: String): DataFrame = {
    try {
      logger.info(s"Executing SQL query via catalog")
      spark.sql(sqlQuery)
    } catch {
      case e: Exception =>
        logger.warn(s"SQL query failed: ${e.getMessage}")
        logger.info("Falling back to DataFrame API")
        
        // Extract the operation from the SQL query and apply it using DataFrame API
        val df = getTable()
        
        if (sqlQuery.toLowerCase.contains("where")) {
          // Extract filter condition and apply it
          val whereClause = sqlQuery.toLowerCase.split("where")(1).split("order by|group by|limit").head.trim
          df.filter(whereClause)
        } else {
          df
        }
    }
  }
  
  /**
   * Register the table with the catalog if possible, but don't fail if it doesn't work
   */
  def registerTable(): Unit = {
    try {
      logger.info(s"Attempting to register table at location: $tableLocation")
      
      // First ensure the namespace exists
      spark.sql("CREATE NAMESPACE IF NOT EXISTS spark_catalog.default")
      
      // Try to register the table
      spark.sql(s"""
        CREATE TABLE IF NOT EXISTS $catalogTableName
        USING iceberg
        LOCATION '$tableLocation'
      """)
      
      logger.info("Table registered successfully with catalog")
    } catch {
      case e: Exception =>
        logger.warn(s"Failed to register table with catalog: ${e.getMessage}")
        logger.info("Will use direct path access for all operations")
    }
  }
  
  /**
   * Time travel to a specific snapshot
   */
  def timeTravel(asOfTimestamp: String = null, snapshotId: Long = -1): DataFrame = {
    try {
      if (snapshotId > 0) {
        logger.info(s"Time travel to snapshot ID: $snapshotId")
        spark.read
          .option("snapshot-id", snapshotId.toString)
          .format("iceberg")
          .table(catalogTableName)
      } else if (asOfTimestamp != null) {
        logger.info(s"Time travel to timestamp: $asOfTimestamp")
        spark.read
          .option("as-of-timestamp", asOfTimestamp)
          .format("iceberg")
          .table(catalogTableName)
      } else {
        // Get the previous snapshot ID
        val snapshots = spark.read.format("iceberg").table(s"$catalogTableName.snapshots")
        
        if (snapshots.count() > 1) {
          val prevSnapshotId = snapshots.orderBy(org.apache.spark.sql.functions.col("committed_at").desc)
            .select("snapshot_id").collect()(1).getLong(0)
          
          logger.info(s"Time travel to previous snapshot ID: $prevSnapshotId")
          spark.read
            .option("snapshot-id", prevSnapshotId.toString)
            .format("iceberg")
            .table(catalogTableName)
        } else {
          logger.warn("Not enough snapshots for time travel")
          getTable()
        }
      }
    } catch {
      case e: Exception =>
        logger.error(s"Time travel failed: ${e.getMessage}")
        logger.info("Falling back to current table version")
        getTable()  // Return current version instead of throwing exception
    }
  }
} 