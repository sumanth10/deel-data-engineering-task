import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

from common.logging_utils import get_logger
from analytical.config import AnalyticalConfig

logger = get_logger("analytical.job")


class AnalyticalAggregationJob:

    JOB_NAME = "analytical-aggregation"

    def __init__(self, spark: SparkSession, config: AnalyticalConfig) -> None:
        self.spark  = spark
        self.config = config

    def _jdbc_props(self) -> dict:
        return {
            "user":     self.config.analytics_db_user,
            "password": self.config.analytics_db_password,
            "driver":   "org.postgresql.Driver",
        }

    def _compute_and_write(self, batch_df, batch_id: int) -> None:

        t_start = time.monotonic()

        orders = (
            self.spark.read
            .format("delta")
            .load(f"{self.config.logistics_base_path}/orders")
            .filter(
                (col("_deleted") == False) &
                (col("status") != "COMPLETED")
            )
            .select("order_id", "delivery_date", "status", "customer_id", "order_date")
            .cache()
        )

        order_items = (
            self.spark.read
            .format("delta")
            .load(f"{self.config.logistics_base_path}/order_items")
            .filter(col("_deleted") == False)
            .select("order_item_id", "order_id", "product_id", "quantity")
        )

        # Denormalised open orders — one row per order item.
        # orders is broadcast because it is the smaller side of the join
        # after filtering to non-COMPLETED only.
        open_orders = order_items.join(
            broadcast(orders), "order_id"
        ).select(
            "order_id",
            "delivery_date",
            "status",
            "customer_id",
            "order_date",
            "order_item_id",
            "product_id",
            "quantity",
        )

        try:
            (
                open_orders.write
                .mode("overwrite")
                .jdbc(
                    url=self.config.analytics_jdbc_url,
                    table="analytics.open_orders",
                    properties=self._jdbc_props(),
                )
            )

            row_count = open_orders.count()
            logger.info(
                "batch=%d open_orders written rows=%d elapsed_ms=%d",
                batch_id,
                row_count,
                int((time.monotonic() - t_start) * 1000),
            )

        finally:
            orders.unpersist()

    def start(self) -> list:
        logger.info("job=%s", self.JOB_NAME)

        # Rate source fires one row per second — used purely as a timer.
        # Gold does not consume the streaming data. It reads a full Silver
        # snapshot inside foreachBatch on every trigger interval.
        timer_stream = (
            self.spark.readStream
            .format("rate")
            .option("rowsPerSecond", 1)
            .load()
        )

        query = (
            timer_stream.writeStream
            .foreachBatch(self._compute_and_write)
            .trigger(processingTime=f"{self.config.trigger_seconds} seconds")
            .option(
                "checkpointLocation",
                f"{self.config.checkpoint_base_path}/analytical/orders",
            )
            .queryName("analytical_kpis")
            .start()
        )

        logger.info("job=%s query=analytical_kpis started", self.JOB_NAME)
        return [query]