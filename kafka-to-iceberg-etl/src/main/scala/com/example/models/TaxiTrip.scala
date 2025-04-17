package com.example.models

import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.util.Date
import java.io.Serializable
import java.util.TimeZone
import org.slf4j.LoggerFactory
import java.text.SimpleDateFormat

case class TaxiTrip(
  tripID: String,
  VendorID: String,
  tpep_pickup_datetime: Date,
  tpep_dropoff_datetime: Date,
  passenger_count: String,
  trip_distance: String,
  RatecodeID: String,
  store_and_fwd_flag: String,
  PULocationID: String,
  DOLocationID: String,
  payment_type: String,
  primary_passenger: Map[String, Any],
  fare_details: Map[String, Any]
) extends Serializable

object TaxiTrip extends Serializable {
  private val logger = LoggerFactory.getLogger(getClass)
  
  // Create a more flexible date formatter
  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  dateFormatter.setLenient(true)
  
  // Safe date parsing helper
  private def safeParseDateString(s: String): Date = {
    try {
      if (s == null || s.trim.isEmpty) {
        logger.warn("Received null or empty date string, using current date")
        return new Date()
      }
      
      logger.debug(s"Attempting to parse date: $s")
      // Try with default format first
      try {
        dateFormatter.parse(s.trim)
      } catch {
        case _: Exception =>
          // If default format fails, try alternative formats
          val alternativeFormats = Array(
            new SimpleDateFormat("yyyy/MM/dd HH:mm:ss"),
            new SimpleDateFormat("MM/dd/yyyy HH:mm:ss"),
            new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")
          )
          
          for (formatter <- alternativeFormats) {
            formatter.setLenient(true)
            try {
              val result = formatter.parse(s.trim)
              logger.debug(s"Successfully parsed date with alternative format: $s to ${result}")
              return result
            } catch {
              case _: Exception => // Continue to next formatter
            }
          }
          
          // As a last resort, create a fallback date
          logger.warn(s"Could not parse date '$s' with any format, using current date as fallback")
          new Date()
      }
    } catch {
      case e: Exception => 
        logger.error(s"Failed to parse date '$s': ${e.getMessage}")
        // Return current date as fallback to avoid null errors
        new Date()
    }
  }
  
  // Custom deserializer for date fields
  class CustomDateDeserializer extends CustomSerializer[Date](format => (
    {
      case JString(s) => safeParseDateString(s)
      case JNull => new Date() // Default to current date if null
    },
    {
      case d: Date => JString(dateFormatter.format(d))
    }
  ))
  
  // Custom date format implementation for JSON4S
  @transient private lazy val formats: Formats = new DefaultFormats {
    override val dateFormat = new DateFormat {
      def format(d: Date): String = dateFormatter.format(d)
      
      def parse(s: String): Option[Date] = {
        try {
          Some(safeParseDateString(s))
        } catch {
          case _: Exception => 
            Some(new Date()) // Return current date as fallback
        }
      }
      
      def timezone = TimeZone.getDefault
    }
  } + new CustomDateDeserializer() // Add the custom deserializer

  def fromJson(jsonString: String): TaxiTrip = {
    try {
      if (jsonString == null || jsonString.trim.isEmpty) {
        logger.error("Received null or empty JSON string")
        return null
      }
      
      implicit val fmt: Formats = formats
      val json = parse(jsonString)
      logger.debug(s"Parsed JSON: $json")
      
      // Extract the TaxiTrip
      val trip = json.extract[TaxiTrip]
      
      // Validate required fields
      if (trip.tripID == null || trip.tripID.isEmpty) {
        logger.error("Extracted TaxiTrip is missing required tripID field")
        return null
      }
      
      trip
    } catch {
      case e: Exception =>
        logger.error(s"Failed to parse TaxiTrip from JSON: ${e.getMessage}", e)
        null
    }
  }
} 