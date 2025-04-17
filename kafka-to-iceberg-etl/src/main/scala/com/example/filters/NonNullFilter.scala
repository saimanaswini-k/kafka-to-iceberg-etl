package com.example.filters

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.table.data.RowData
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.util.concurrent.atomic.AtomicLong

/**
 * Filters out null records from the data stream
 */
class NonNullFilter extends FilterFunction[RowData] with Serializable {
  @transient private lazy val logger = LoggerFactory.getLogger(classOf[NonNullFilter])
  @transient private lazy val recordCounter = new AtomicLong(0)
  @transient private lazy val nullCounter = new AtomicLong(0)
  @transient private lazy val lastStatsTime = new AtomicLong(System.currentTimeMillis())
  @transient private val statsIntervalMs = 60000 // Log stats every minute
  
  override def filter(value: RowData): Boolean = {
    val recordId = recordCounter.incrementAndGet()
    
    // Log statistics periodically
    val currentTime = System.currentTimeMillis()
    if (currentTime - lastStatsTime.get() > statsIntervalMs) {
      val totalRecords = recordCounter.get()
      val nullRecords = nullCounter.get()
      val nullRate = if (totalRecords > 0) (nullRecords.toDouble / totalRecords * 100) else 0.0
      
      logger.info(f"Filter stats: total=${totalRecords}%,d records, null=${nullRecords}%,d records (${nullRate}%.2f%%)")
      lastStatsTime.set(currentTime)
    }
    
    if (value == null) {
      nullCounter.incrementAndGet()
      logger.debug(s"[Record #$recordId] Filtering out null record")
      false
    } else {
      // Extract trip ID from row data for better logging if available
      val tripId = try {
        val stringData = value.getString(0)
        if (stringData != null) stringData.toString else "unknown"
      } catch {
        case _: Exception => "unknown"
      }
      
      logger.debug(s"[Record #$recordId] Keeping record with tripID=$tripId")
      true
    }
  }
} 