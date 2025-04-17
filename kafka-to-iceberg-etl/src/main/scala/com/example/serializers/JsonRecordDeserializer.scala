package com.example.serializers

import org.slf4j.LoggerFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import scala.collection.mutable.StringBuilder
import java.io.IOException

/**
 * Custom JSON deserializer that accumulates JSON records from Kafka
 * Handles JSON records that might be split across multiple messages
 */
class JsonRecordDeserializer extends KafkaRecordDeserializationSchema[String] {
  private val logger = LoggerFactory.getLogger(getClass)
  private val recordBuilder = new StringBuilder()
  private var braceCount = 0
  private var inRecord = false
  private var totalRecordsProcessed = 0
  private var totalRecordsEmitted = 0
  private var lastLogTime = System.currentTimeMillis()
  private val logIntervalMs = 60000 // Log stats every minute
  
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]], out: Collector[String]): Unit = {
    try {
      // Log processing stats periodically
      val currentTime = System.currentTimeMillis()
      if (currentTime - lastLogTime > logIntervalMs) {
        logger.info(f"Deserializer stats: processed=${totalRecordsProcessed}%,d records, emitted=${totalRecordsEmitted}%,d complete JSON objects")
        lastLogTime = currentTime
      }
      
      // Skip null records
      if (record.value() == null) {
        logger.debug(s"Skipping null record from partition=${record.partition()}, offset=${record.offset()}")
        return
      }
      
      totalRecordsProcessed += 1
      val messageStr = new String(record.value(), "UTF-8").trim()
      
      // Skip empty messages
      if (messageStr.isEmpty) {
        logger.debug(s"Skipping empty record from partition=${record.partition()}, offset=${record.offset()}")
        return
      }
      
      // Track record info for debugging
      val recordInfo = s"partition=${record.partition()}, offset=${record.offset()}, key=${Option(record.key()).map(new String(_, "UTF-8")).getOrElse("null")}"
      logger.debug(s"Processing Kafka record: $recordInfo")
      
      if (logger.isTraceEnabled()) {
        logger.trace(s"Record content (${messageStr.length} bytes): ${truncateForLogging(messageStr)}")
      }
      
      // Process each character in the message
      messageStr.foreach { char =>
        if (char == '{') {
          if (!inRecord) {
            inRecord = true
            recordBuilder.clear()
            logger.debug(s"Starting new JSON record at $recordInfo")
          }
          braceCount += 1
          recordBuilder.append(char)
        } else if (char == '}') {
          braceCount -= 1
          recordBuilder.append(char)
          
          if (braceCount == 0 && inRecord) {
            inRecord = false
            val completeRecord = recordBuilder.toString()
            recordBuilder.clear()
            
            if (completeRecord.trim().startsWith("{") && completeRecord.trim().endsWith("}")) {
              totalRecordsEmitted += 1
              logger.debug(s"Emitting complete JSON record #$totalRecordsEmitted (${completeRecord.length} bytes)")
              
              if (logger.isTraceEnabled()) {
                logger.trace(s"JSON content: ${truncateForLogging(completeRecord)}")
              }
              
              out.collect(completeRecord)
            } else {
              logger.warn(s"Discarding invalid JSON record: ${truncateForLogging(completeRecord)}")
            }
          }
        } else if (inRecord) {
          recordBuilder.append(char)
        }
      }
      
      // Check for incomplete records that might span multiple Kafka messages
      if (inRecord && braceCount > 0) {
        logger.debug(s"Incomplete JSON record after processing message: braceCount=$braceCount, bufferSize=${recordBuilder.length}")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error processing Kafka record: ${e.getMessage}", e)
        // Don't rethrow - continue processing other records
    }
  }

  override def getProducedType: TypeInformation[String] = {
    TypeExtractor.getForClass(classOf[String])
  }
  
  /**
   * Truncates a string for logging to avoid excessively long log entries
   */
  private def truncateForLogging(str: String, maxLength: Int = 200): String = {
    if (str.length <= maxLength) str
    else str.substring(0, maxLength) + s"... (${str.length - maxLength} more bytes)"
  }
} 