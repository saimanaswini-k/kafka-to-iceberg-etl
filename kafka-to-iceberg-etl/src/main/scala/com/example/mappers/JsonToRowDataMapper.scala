package com.example.mappers

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.GenericRowData
import org.apache.flink.table.data.StringData
import org.slf4j.LoggerFactory
import com.example.models.TaxiTrip
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.io.Serializable
import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicLong

/**
 * Maps JSON strings to Iceberg RowData objects
 * Converts TaxiTrip model objects to the format required by Iceberg
 */
@SerialVersionUID(1L)
class JsonToRowDataMapper extends MapFunction[String, RowData] with Serializable {
  @transient private lazy val logger = LoggerFactory.getLogger(classOf[JsonToRowDataMapper])
  @transient private lazy val jsonFormats = DefaultFormats
  @transient private lazy val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  @transient private lazy val recordCounter = new AtomicLong(0)
  @transient private lazy val successCounter = new AtomicLong(0)
  @transient private lazy val failureCounter = new AtomicLong(0)
  @transient private lazy val lastStatsTime = new AtomicLong(System.currentTimeMillis())
  @transient private val statsIntervalMs = 60000 // Log stats every minute

  override def map(jsonString: String): RowData = {
    val recordId = recordCounter.incrementAndGet()
    
    // Log mapping statistics periodically
    val currentTime = System.currentTimeMillis()
    if (currentTime - lastStatsTime.get() > statsIntervalMs) {
      val success = successCounter.get()
      val failure = failureCounter.get()
      val total = success + failure
      val successRate = if (total > 0) (success.toDouble / total * 100) else 0.0
      
      logger.info(f"Mapper stats: processed=$total%,d records (success=$success%,d, failure=$failure%,d, rate=$successRate%.1f%%)")
      lastStatsTime.set(currentTime)
    }
    
    try {
      // Trim and validate input
      val trimmedJson = jsonString.trim()
      if (trimmedJson.isEmpty || !trimmedJson.startsWith("{") || !trimmedJson.endsWith("}")) {
        logger.warn(s"[Record #$recordId] Invalid JSON input: ${truncateForLogging(trimmedJson)}")
        failureCounter.incrementAndGet()
        return null
      }

      // Log JSON input details
      if (logger.isDebugEnabled()) {
        logger.debug(s"[Record #$recordId] Processing JSON input (${trimmedJson.length} bytes)")
      }
      
      // Parse JSON
      val trip = TaxiTrip.fromJson(trimmedJson)
      if (trip == null) {
        logger.warn(s"[Record #$recordId] Failed to parse JSON to TaxiTrip model")
        failureCounter.incrementAndGet()
        return null
      }

      // Check required fields
      if (trip.tripID == null || trip.tripID.isEmpty) {
        logger.warn(s"[Record #$recordId] Missing required field: tripID")
        failureCounter.incrementAndGet()
        return null
      }

      // Create row with correct number of fields
      val row = new GenericRowData(13)
      logger.debug(s"[Record #$recordId] Creating RowData for tripID=${trip.tripID}")
      
      // Set string fields
      row.setField(0, StringData.fromString(trip.tripID))
      row.setField(1, StringData.fromString(trip.VendorID))
      
      // Safely handle date fields - convert to TimestampData
      try {
        // Get timestamp in milliseconds and convert to TimestampData
        val pickupTimestamp = trip.tpep_pickup_datetime.getTime
        logger.debug(s"[Record #$recordId] Pickup timestamp in millis: $pickupTimestamp")
        row.setField(2, org.apache.flink.table.data.TimestampData.fromEpochMillis(pickupTimestamp))
      } catch {
        case e: Exception =>
          logger.warn(s"[Record #$recordId] Error setting pickup datetime: ${e.getMessage}", e)
          // Use current time as fallback
          row.setField(2, org.apache.flink.table.data.TimestampData.fromEpochMillis(System.currentTimeMillis()))
      }
      
      try {
        // Get timestamp in milliseconds and convert to TimestampData
        val dropoffTimestamp = trip.tpep_dropoff_datetime.getTime
        logger.debug(s"[Record #$recordId] Dropoff timestamp in millis: $dropoffTimestamp")
        row.setField(3, org.apache.flink.table.data.TimestampData.fromEpochMillis(dropoffTimestamp))
      } catch {
        case e: Exception =>
          logger.warn(s"[Record #$recordId] Error setting dropoff datetime: ${e.getMessage}", e)
          // Use current time as fallback
          row.setField(3, org.apache.flink.table.data.TimestampData.fromEpochMillis(System.currentTimeMillis()))
      }
      
      // Set the rest of the fields
      row.setField(4, StringData.fromString(trip.passenger_count))
      row.setField(5, StringData.fromString(trip.trip_distance))
      row.setField(6, StringData.fromString(trip.RatecodeID))
      row.setField(7, StringData.fromString(trip.store_and_fwd_flag))
      row.setField(8, StringData.fromString(trip.PULocationID))
      row.setField(9, StringData.fromString(trip.DOLocationID))
      row.setField(10, StringData.fromString(trip.payment_type))
      
      // Convert maps to JSON strings
      val primaryPassenger = if (trip.primary_passenger != null) {
        try {
          compact(render(Extraction.decompose(trip.primary_passenger)(jsonFormats)))
        } catch {
          case e: Exception =>
            logger.warn(s"[Record #$recordId] Error serializing primary_passenger: ${e.getMessage}")
            "{}"
        }
      } else ""
      
      val fareDetails = if (trip.fare_details != null) {
        try {
          compact(render(Extraction.decompose(trip.fare_details)(jsonFormats)))
        } catch {
          case e: Exception =>
            logger.warn(s"[Record #$recordId] Error serializing fare_details: ${e.getMessage}")
            "{}"
        }
      } else ""
      
      row.setField(11, StringData.fromString(primaryPassenger))
      row.setField(12, StringData.fromString(fareDetails))
      
      // Log successful mapping
      logger.debug(s"[Record #$recordId] Successfully mapped JSON to RowData (tripID=${trip.tripID})")
      successCounter.incrementAndGet()
      
      row
    } catch {
      case e: Exception =>
        logger.error(s"[Record #$recordId] Error mapping JSON to RowData: ${e.getMessage}", e)
        failureCounter.incrementAndGet()
        null
    }
  }
  
  /**
   * Truncates a string for logging to avoid excessively long log entries
   */
  private def truncateForLogging(str: String, maxLength: Int = 200): String = {
    if (str == null) "null"
    else if (str.length <= maxLength) str
    else str.substring(0, maxLength) + s"... (${str.length - maxLength} more bytes)"
  }
} 