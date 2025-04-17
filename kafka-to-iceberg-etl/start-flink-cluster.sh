#!/bin/bash

FLINK_HOME="/Users/apple/Downloads/flink-1.19.2"

# Stop any running Flink cluster
echo "Stopping any running Flink cluster..."
$FLINK_HOME/bin/stop-cluster.sh

# Set environment variables for Java options
export FLINK_ENV_JAVA_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED"
echo "Setting Java options: $FLINK_ENV_JAVA_OPTS"

# Start the Flink cluster (JobManager and TaskManager)
echo "Starting Flink cluster..."
$FLINK_HOME/bin/start-cluster.sh

# Wait a moment for the cluster to start
sleep 5

# Verify that TaskManagers are registered
echo "Checking registered TaskManagers..."
$FLINK_HOME/bin/flink list

echo ""
echo "Flink cluster started with TaskManager configuration."
echo "You can access the Flink Web UI at: http://localhost:8081"
echo ""
echo "To run your Kafka to Iceberg job:"
echo "cd /Users/apple/Desktop/iceberg-kafka"
echo "sbt assembly"
echo "$FLINK_HOME/bin/flink run -Denv.java.opts=\"$FLINK_ENV_JAVA_OPTS\" target/scala-2.12/iceberg-kafka-assembly-0.1.0-SNAPSHOT.jar" 