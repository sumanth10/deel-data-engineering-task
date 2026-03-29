import logging
import sys

sys.path.insert(0, "/opt/spark-jobs/jobs")

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

from raw.config import RawIngestionConfig
from raw.job import RawIngestionJob
from logistics.config import LogisticsConfig
from logistics.job import LogisticsTransformJob
from common.schemas import (
    ORDERS_SCHEMA,
    CUSTOMERS_SCHEMA,
    PRODUCTS_SCHEMA,
    ORDER_ITEMS_SCHEMA,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("pipeline")

# Kafka metadata columns appended by batch_processor.py on every raw write.
# Must exactly match the select in process_cdc_batch — mergeSchema=false
# means any mismatch between this schema and the actual write will fail loudly.
_KAFKA_META = StructType(
    [
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("kafka_ingest_ts", TimestampType(), True),
    ]
)

_RAW_TABLE_SCHEMAS = {
    "orders": StructType(ORDERS_SCHEMA.fields + _KAFKA_META.fields),
    "customers": StructType(CUSTOMERS_SCHEMA.fields + _KAFKA_META.fields),
    "products": StructType(PRODUCTS_SCHEMA.fields + _KAFKA_META.fields),
    "order_items": StructType(ORDER_ITEMS_SCHEMA.fields + _KAFKA_META.fields),
}


def _initialize_raw_tables(spark: SparkSession, raw_base_path: str) -> None:
    for table, schema in _RAW_TABLE_SCHEMAS.items():
        path = f"{raw_base_path}/{table}"
        if not DeltaTable.isDeltaTable(spark, path):
            logger.info("Initialising raw Delta table: %s", path)
            spark.createDataFrame([], schema).write.format("delta").mode("append").save(
                path
            )
        else:
            logger.info("Raw Delta table already exists: %s", path)


def build_session() -> SparkSession:
    return SparkSession.builder.appName("acme-analytics-pipeline").getOrCreate()


def main() -> None:
    spark = build_session()
    logger.info("SparkSession started — version=%s", spark.version)

    raw_config = RawIngestionConfig()
    logistics_config = LogisticsConfig()

    _initialize_raw_tables(spark, raw_config.raw_base_path)

    raw_queries = RawIngestionJob(spark, raw_config).start()
    logistics_queries = LogisticsTransformJob(spark, logistics_config).start()

    all_queries = raw_queries + logistics_queries
    logger.info("Pipeline running — active_queries=%d", len(all_queries))
    logger.info("Spark UI available at http://localhost:4040")

    spark.streams.awaitAnyTermination()

    for query in all_queries:
        if not query.isActive:
            logger.error(
                "query=%s terminated exception=%s",
                query.name,
                query.exception(),
            )


if __name__ == "__main__":
    main()
