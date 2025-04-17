# Kafka-Spark-Iceberg Integration

This project demonstrates the integration between Apache Kafka, Apache Spark, and Apache Iceberg for building a modern data lakehouse architecture.

## Overview

This project provides a framework for:
- Streaming data from Kafka into Iceberg tables
- Running Spark queries against Iceberg tables
- Managing Iceberg table services and operations

## Components

- **Kafka Integration**: Stream processing from Kafka topics to Iceberg tables
- **Spark Queries**: Analytics and data processing using Spark SQL on Iceberg tables
- **Iceberg Table Services**: Utilities for managing Iceberg table operations

## Getting Started

### Prerequisites

- Apache Spark 3.5.3
- Apache Kafka 3.9.0
- Apache Iceberg 1.2.1
- Java 17
- Scala 2.12.15

### Running the Application

The project includes several scripts to run different components:

1. **Process Kafka data to Iceberg**:
   ```
   ./run-kafka-iceberg-job.sh
   ```

2. **Run Iceberg-specific queries**:
   ```
   ./run-iceberg-queries.sh
   ```

3. **Run general Spark queries**:
   ```
   ./run-spark-queries.sh
   ```

## Project Structure

- `src/main/scala/com/example/IcebergSparkQueries.scala`: Implementation of Spark queries for Iceberg tables
- `src/main/scala/com/example/services/IcebergTableService.scala`: Service layer for Iceberg table operations
- `run-kafka-iceberg-job.sh`: Script to run the Kafka to Iceberg integration
- `run-iceberg-queries.sh`: Script to execute Iceberg-specific queries
- `run-spark-queries.sh`: Script to run general Spark queries

## Configuration

[Add details about configuration files and options]

## Examples

[Add examples of common usage patterns]

## Contributing

[Add contribution guidelines]

## License

[Add license information] 