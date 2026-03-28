import time
from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, from_json
from pyspark.sql.types import StringType, StructType

from common.schemas import TOPIC_SCHEMA_MAP
from common.logging_utils import get_logger

logger = get_logger("raw.batch_processor")


def _parse_and_classify(
    df: DataFrame,
    schema: StructType,
) -> Tuple[DataFrame, DataFrame]:

    # Debezium heartbeats (null Kafka value) are dropped silently — they are
    # expected infrastructure noise, not data failures.
    non_null = df.filter(col("value").isNotNull())

    parsed = non_null.withColumn(
        "payload",
        from_json(col("value").cast(StringType()), schema),
    )
    # malformed covers only one case: the Kafka value is not parseable JSON.
    malformed = parsed.filter(col("payload").isNull())
    valid     = parsed.filter(col("payload").isNotNull())

    return valid, malformed


def _write_dead_letter(
    df: DataFrame,
    batch_id: int,
    path: str,
    reason: str,
) -> None:
    if df.isEmpty():
        return
    (
        df
        .withColumn("dead_letter_reason", lit(reason))
        .withColumn("dead_letter_batch_id", lit(batch_id))
        .withColumn("dead_letter_ts", current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .save(path)
    )


def process_cdc_batch(
    batch_df: DataFrame,
    batch_id: int,
    *,
    topic: str,
    raw_path: str,
    dead_letter_path: str,
) -> None:
    t_start = time.monotonic()

    if batch_df.isEmpty():
        return

    schema = TOPIC_SCHEMA_MAP[topic]
    valid, malformed = _parse_and_classify(batch_df, schema)

    malformed_count = malformed.count()
    if malformed_count > 0:
        _write_dead_letter(
            malformed.select("value", "topic", "partition", "offset"),
            batch_id,
            dead_letter_path,
            reason="json_parse_failure",
        )
        logger.warning(
            "topic=%s batch=%d dead_letter=%d reason=json_parse_failure",
            topic, batch_id, malformed_count,
        )

    valid_record_count = valid.count()
    if valid_record_count > 0:
        (
            valid
            .select(
                col("payload.*"),
                col("topic"),
                col("partition"),
                col("offset"),
                col("timestamp").alias("kafka_ingest_ts"),
            )
            .write
            .format("delta")
            .mode("append")
            # Schema drift must fail loudly. Silent mergeSchema would let a
            # connector change corrupt the raw table undetected.
            .option("mergeSchema", "false")
            .save(raw_path)
        )

    logger.info(
        "topic=%s batch=%d valid=%d dead_letter=%d elapsed_ms=%d",
        topic,
        batch_id,
        valid_record_count,
        malformed_count,
        int((time.monotonic() - t_start) * 1000),
    )