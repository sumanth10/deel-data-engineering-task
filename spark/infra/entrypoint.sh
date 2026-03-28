#!/bin/bash
set -e

echo "Waiting for Kafka..."
until nc -z kafka 29092 > /dev/null 2>&1; do
  echo "Kafka not ready, retrying in 5s..."
  sleep 5
done

# Extra buffer — port open does not mean Kafka is fully initialised
sleep 10
echo "Kafka ready."

exec /opt/spark/bin/spark-submit \
  --master "local[4]" \
  --driver-memory 2g \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.ui.port=4040 \
  --conf spark.ui.enabled=true \
  /opt/spark-jobs/pipeline.py