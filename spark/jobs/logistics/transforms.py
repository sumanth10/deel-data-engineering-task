import time
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_unixtime, to_date, lit, current_timestamp
from pyspark.sql.types import TimestampType, DecimalType

from common.logging_utils import get_logger

logger = get_logger("logistics.transforms")


def _to_date_col(c):
    return to_date(from_unixtime(c.cast("long") * 86400))


def _to_timestamp_col(c):
    # Source uses TIMESTAMP(3) — Debezium serialises as milliseconds since epoch.
    return (c / 1000).cast(TimestampType())


def merge_customers(batch_df: DataFrame, batch_id: int, path: str) -> None:
    if batch_df.isEmpty():
        return

    t_start = time.monotonic()
    spark = batch_df.sparkSession

    inserts_updates = batch_df.filter(col("op").isin("c", "u", "r")).select(
        col("after.customer_id"),
        col("after.customer_name"),
        col("after.is_active"),
        col("after.customer_address"),
        _to_timestamp_col(col("after.updated_at")).alias("updated_at"),
        _to_timestamp_col(col("after.created_at")).alias("created_at"),
        lit(False).alias("_deleted"),
    )

    # Soft delete: mark customer inactive rather than removing the row.
    # Orders referencing this customer remain intact for historical queries.
    deletes = batch_df.filter(col("op") == "d").select(
        col("before.customer_id")
    )

    if DeltaTable.isDeltaTable(spark, path):
        target = DeltaTable.forPath(spark, path)

        if not inserts_updates.isEmpty():
            target.alias("t").merge(
                inserts_updates.alias("s"),
                "t.customer_id = s.customer_id"
            ).whenMatchedUpdate(set={
                "customer_name":    "s.customer_name",
                "is_active":        "s.is_active",
                "customer_address": "s.customer_address",
                "updated_at":       "s.updated_at",
                "_deleted":         "false",
            }).whenNotMatchedInsertAll().execute()

        if not deletes.isEmpty():
            target.alias("t").merge(
                deletes.alias("s"),
                "t.customer_id = s.customer_id"
            ).whenMatchedUpdate(set={
                "is_active":  "false",
                "_deleted":   "true",
                "updated_at": "current_timestamp()",
            }).execute()
    else:
        if not inserts_updates.isEmpty():
            inserts_updates.write.format("delta").mode("append").save(path)

    logger.info(
        "batch=%d dim_customers inserts_updates=%d deletes=%d elapsed_ms=%d",
        batch_id,
        inserts_updates.count(),
        deletes.count(),
        int((time.monotonic() - t_start) * 1000),
    )


def merge_products(batch_df: DataFrame, batch_id: int, path: str) -> None:
    if batch_df.isEmpty():
        return

    t_start = time.monotonic()
    spark = batch_df.sparkSession

    inserts_updates = batch_df.filter(col("op").isin("c", "u", "r")).select(
        col("after.product_id"),
        col("after.product_name"),
        col("after.barcode"),
        col("after.unity_price").cast(DecimalType(10, 2)).alias("unity_price"),
        col("after.is_active"),
        _to_timestamp_col(col("after.updated_at")).alias("updated_at"),
        _to_timestamp_col(col("after.created_at")).alias("created_at"),
        lit(False).alias("_deleted"),
    )

    deletes = batch_df.filter(col("op") == "d").select(
        col("before.product_id")
    )

    if DeltaTable.isDeltaTable(spark, path):
        target = DeltaTable.forPath(spark, path)

        if not inserts_updates.isEmpty():
            target.alias("t").merge(
                inserts_updates.alias("s"),
                "t.product_id = s.product_id"
            ).whenMatchedUpdate(set={
                "product_name": "s.product_name",
                "barcode":      "s.barcode",
                "unity_price":  "s.unity_price",
                "is_active":    "s.is_active",
                "updated_at":   "s.updated_at",
                "_deleted":     "false",
            }).whenNotMatchedInsertAll().execute()

        if not deletes.isEmpty():
            target.alias("t").merge(
                deletes.alias("s"),
                "t.product_id = s.product_id"
            ).whenMatchedUpdate(set={
                "is_active":  "false",
                "_deleted":   "true",
                "updated_at": "current_timestamp()",
            }).execute()
    else:
        if not inserts_updates.isEmpty():
            inserts_updates.write.format("delta").mode("append").save(path)

    logger.info(
        "batch=%d dim_products inserts_updates=%d deletes=%d elapsed_ms=%d",
        batch_id,
        inserts_updates.count(),
        deletes.count(),
        int((time.monotonic() - t_start) * 1000),
    )


def merge_orders(batch_df: DataFrame, batch_id: int, path: str) -> None:

    if batch_df.isEmpty():
        return

    t_start = time.monotonic()
    spark = batch_df.sparkSession

    inserts_updates = batch_df.filter(col("op").isin("c", "u", "r")).select(
        col("after.order_id"),
        _to_date_col(col("after.order_date")).alias("order_date"),
        _to_date_col(col("after.delivery_date")).alias("delivery_date"),
        col("after.customer_id"),
        col("after.status"),
        _to_timestamp_col(col("after.updated_at")).alias("updated_at"),
        _to_timestamp_col(col("after.created_at")).alias("created_at"),
        lit(False).alias("_deleted"),
    )

    deletes = batch_df.filter(col("op") == "d").select(
        col("before.order_id"),
        _to_date_col(col("before.order_date")).alias("order_date"),
    )

    if DeltaTable.isDeltaTable(spark, path):
        target = DeltaTable.forPath(spark, path)

        if not inserts_updates.isEmpty():
            target.alias("t").merge(
                inserts_updates.alias("s"),
                "t.order_id = s.order_id AND t.order_date = s.order_date"
            ).whenMatchedUpdate(set={
                "status":        "s.status",
                "delivery_date": "s.delivery_date",
                "customer_id":   "s.customer_id",
                "updated_at":    "s.updated_at",
                "_deleted":      "false",
            }).whenNotMatchedInsertAll().execute()

        if not deletes.isEmpty():
            target.alias("t").merge(
                deletes.alias("s"),
                "t.order_id = s.order_id AND t.order_date = s.order_date"
            ).whenMatchedUpdate(set={
                "_deleted":   "true",
                "updated_at": "current_timestamp()",
            }).execute()
    else:
        # Partitioned by order_date. MERGE condition includes order_date so Delta
        # uses partition pruning,only the relevant date partition is scanned.
        if not inserts_updates.isEmpty():
            inserts_updates.write \
                .format("delta") \
                .partitionBy("order_date") \
                .mode("append") \
                .save(path)

    logger.info(
        "batch=%d fact_orders inserts_updates=%d deletes=%d elapsed_ms=%d",
        batch_id,
        inserts_updates.count(),
        deletes.count(),
        int((time.monotonic() - t_start) * 1000),
    )


def merge_order_items(batch_df: DataFrame, batch_id: int, path: str) -> None:
    if batch_df.isEmpty():
        return

    t_start = time.monotonic()
    spark = batch_df.sparkSession

    inserts_updates = batch_df.filter(col("op").isin("c", "u", "r")).select(
        col("after.order_item_id"),
        col("after.order_id"),
        col("after.product_id"),
        # quanity is the actual column name in source DDL (typo preserved in raw).
        # Aliased to quantity here — logistics layer uses the correct spelling.
        col("after.quanity").alias("quantity"),
        _to_timestamp_col(col("after.updated_at")).alias("updated_at"),
        _to_timestamp_col(col("after.created_at")).alias("created_at"),
        lit(False).alias("_deleted"),
    )

    deletes = batch_df.filter(col("op") == "d").select(
        col("before.order_item_id")
    )

    if DeltaTable.isDeltaTable(spark, path):
        target = DeltaTable.forPath(spark, path)

        if not inserts_updates.isEmpty():
            target.alias("t").merge(
                inserts_updates.alias("s"),
                "t.order_item_id = s.order_item_id"
            ).whenMatchedUpdate(set={
                "quantity":   "s.quantity",
                "updated_at": "s.updated_at",
                "_deleted":   "false",
            }).whenNotMatchedInsertAll().execute()

        if not deletes.isEmpty():
            target.alias("t").merge(
                deletes.alias("s"),
                "t.order_item_id = s.order_item_id"
            ).whenMatchedUpdate(set={
                "_deleted":   "true",
                "updated_at": "current_timestamp()",
            }).execute()
    else:
        if not inserts_updates.isEmpty():
            inserts_updates.write.format("delta").mode("append").save(path)

    logger.info(
        "batch=%d fact_order_items inserts_updates=%d deletes=%d elapsed_ms=%d",
        batch_id,
        inserts_updates.count(),
        deletes.count(),
        int((time.monotonic() - t_start) * 1000),
    )