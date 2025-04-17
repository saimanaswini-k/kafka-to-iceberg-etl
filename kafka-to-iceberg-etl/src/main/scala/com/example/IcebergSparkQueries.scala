package com.example

import org.apache.spark.sql.SparkSession

/**
 * A utility application for querying Apache Iceberg tables using Apache Spark.
 * Demonstrates catalog-based SQL queries against Iceberg tables.
 */
object IcebergSparkQueries {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Iceberg Queries")
      .getOrCreate()
      
    // First, list all namespaces and tables to find what's available
    println("Available namespaces:")
    spark.sql("SHOW NAMESPACES IN local").show()
    
    // List tables in the confirmed 'default' namespace
    println("Tables in local.default:")
    spark.sql("SHOW TABLES IN local.default").show()
    
    // Use the confirmed namespace from output
    val catalog = "local"
    val namespace = "default"  // This is confirmed from the output
    val table = "json_data"
    val tablePath = s"$catalog.$namespace.$table"
    
    println(s"Using table: $tablePath")
    
    // Basic query to select all data (limit to avoid large output)
    println("Basic SELECT query:")
    spark.sql(s"SELECT * FROM $tablePath LIMIT 10").show()
    
    // Count records
    println("Count query:")
    spark.sql(s"SELECT COUNT(*) FROM $tablePath").show()
    
    // Aggregation query
    println("Aggregation query:")
    spark.sql(s"""
      SELECT passenger_count, COUNT(*) as trip_count 
      FROM $tablePath 
      GROUP BY passenger_count 
      ORDER BY trip_count DESC
    """).show()
    
    // Filter query
    println("Filter query:")
    spark.sql(s"""
      SELECT * FROM $tablePath 
      WHERE trip_distance > '5.0' 
      LIMIT 10
    """).show()
    
    // Time travel using snapshot ID instead of timestamp
    println("Available snapshots:")
    spark.sql(s"SELECT snapshot_id, committed_at FROM $tablePath.snapshots ORDER BY committed_at").show()
    
    try {
      println("Time travel using snapshot ID:")
      val minSnapshotIdRow = spark.sql(s"SELECT MIN(snapshot_id) AS min_id FROM $tablePath.snapshots").collect()(0)
      val minSnapshotId = minSnapshotIdRow.getLong(0)
      println(s"Using earliest snapshot ID: $minSnapshotId")
      
      spark.sql(s"""
        SELECT * FROM $tablePath VERSION AS OF $minSnapshotId
        LIMIT 10
      """).show()
    } catch {
      case e: Exception => 
        println(s"Time travel failed: ${e.getMessage}")
        println("Displaying current data instead:")
        spark.sql(s"SELECT * FROM $tablePath LIMIT 10").show()
    }
    
    // View table history
    println("Table history:")
    spark.sql(s"SELECT * FROM $tablePath.history").show()
    
    // View table snapshots
    println("Table snapshots:")
    spark.sql(s"SELECT * FROM $tablePath.snapshots").show()
    
    spark.stop()
  }
} 