#!/bin/bash
# Check if FLINK_HOME environment variable is set, otherwise use the provided value
if [ -z "$FLINK_HOME" ]; then
  FLINK_HOME="/Users/apple/Downloads/flink-1.19.2"
  echo "FLINK_HOME not set, using default: $FLINK_HOME"
  if [ ! -d "$FLINK_HOME" ]; then
    echo "Error: Flink installation not found at $FLINK_HOME"
    echo "Please set the FLINK_HOME environment variable to your Flink installation directory"
    exit 1
  fi
fi

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_PATH="$SCRIPT_DIR/target/scala-2.12/iceberg-kafka-assembly-0.1.0-SNAPSHOT.jar"
KRYO_CONF="$SCRIPT_DIR/kryo-serialization.conf"

# Set JVM options to allow reflection access
JAVA_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
echo "Running Kafka to Iceberg job"

# Build the project using sbt
echo "Building project..."
sbt clean update compile assembly

# Check if JAR was built successfully
if [ ! -f "$JAR_PATH" ]; then
  echo "Error: Failed to build the JAR file at $JAR_PATH"
  echo "Please check the sbt build logs for errors."
  exit 1
fi
echo "JAR built successfully: $JAR_PATH"

# Ensure JAR has read and execute permissions
chmod +rx "$JAR_PATH"

# Run the Flink job
echo "Submitting Flink job..."
$FLINK_HOME/bin/flink run \
  -Denv.java.opts="$JAVA_OPTS" \
  -c com.example.KafkaToIcebergJob \
  "$JAR_PATH"

# Check if job submission was successful
if [ $? -eq 0 ]; then
  echo "Job submitted successfully. Check the Flink Web UI at http://localhost:8081 for status."
else
  echo "Error: Failed to submit the Flink job. Check the logs for details."
  exit 1
fi