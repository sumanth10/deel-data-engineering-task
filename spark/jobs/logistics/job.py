from pyspark.sql import DataFrame, SparkSession

from common.logging_utils import get_logger
from logistics.config import LogisticsConfig
from logistics.transforms import (
    merge_customers,
    merge_products,
    merge_orders,
    merge_order_items,
)

logger = get_logger("logistics.job")

# Maps each raw Delta table path to its MERGE function and logistics target path.
# Adding a new table means adding one entry here — nothing else changes.
_STREAM_CONFIG = [
    ("orders",      merge_orders),
    ("customers",   merge_customers),
    ("products",    merge_products),
    ("order_items", merge_order_items),
]


class LogisticsTransformJob:
    JOB_NAME = "logistics-transform"

    def __init__(self, spark: SparkSession, config: LogisticsConfig) -> None:
        self.spark  = spark
        self.config = config

    def _raw_stream_for(self, table: str) -> DataFrame:
        return (
            self.spark.readStream
            .format("delta")
            # maxFilesPerTrigger bounds how many new Raw files are processed
            # per batch. Prevents a burst of Raw writes from flooding one
            # Logistics batch and causing OOM during the MERGE.
            .option("maxFilesPerTrigger", str(self.config.max_files_per_trigger))
            .load(f"{self.config.raw_base_path}/{table}")
        )

    def _start_query(self, table: str, merge_fn):
        raw_path      = f"{self.config.raw_base_path}/{table}"
        logistics_path = f"{self.config.logistics_base_path}/{table}"
        checkpoint_path = f"{self.config.checkpoint_base_path}/logistics/{table}"

        stream = self._raw_stream_for(table)

        return (
            stream.writeStream
            .foreachBatch(
                lambda df, bid, lp=logistics_path, fn=merge_fn:
                    fn(df, bid, lp)
            )
            .trigger(processingTime=f"{self.config.trigger_seconds} seconds")
            .option("checkpointLocation", checkpoint_path)
            .queryName(f"logistics_{table}")
            .start()
        )

    def start(self) -> list:
        logger.info("job=%s tables=%s", self.JOB_NAME, [t for t, _ in _STREAM_CONFIG])
        queries = [self._start_query(table, fn) for table, fn in _STREAM_CONFIG]
        logger.info("job=%s active_queries=%d", self.JOB_NAME, len(queries))
        return queries