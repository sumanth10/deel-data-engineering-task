from pyspark.sql import DataFrame, SparkSession

from common.schemas import TOPIC_SCHEMA_MAP
from common.logging_utils import get_logger
from raw.config import RawIngestionConfig
from raw.batch_processor import process_cdc_batch

logger = get_logger("raw.job")

# order_items and orders change at a significantly higher rate than customers
# or products in a logistics domain. Independent caps prevent high-volume topics
# from consuming the entire maxOffsetsPerTrigger budget and starving the others.
_HIGH_VOLUME_TOPICS = frozenset({
    "finance_db.operations.order_items",
    "finance_db.operations.orders",
})


class RawIngestionJob:

    JOB_NAME = "raw-ingestion"

    def __init__(self, spark: SparkSession, config: RawIngestionConfig) -> None:
        self.spark  = spark
        self.config = config

    def _kafka_stream_for(self, topic: str) -> DataFrame:
        # One readStream per topic gives each an independent Kafka consumer,
        # independent maxOffsetsPerTrigger budget, and an independent failure
        # boundary. If order_items lags, customers is unaffected. A single shared
        # consumer would conflate their resource profiles and make per-topic
        # tuning impossible.
        max_offsets = (
            self.config.max_offsets_per_trigger
            if topic in _HIGH_VOLUME_TOPICS
            else self.config.max_offsets_per_trigger_low_volume
        )

        return (
            self.spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.config.kafka_bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", str(max_offsets))
            .option("failOnDataLoss", "false")
            .load()
        )

    def _table_name(self, topic: str) -> str:
        return topic.split(".")[-1]

    def _start_query(self, topic: str):
        table            = self._table_name(topic)
        raw_path         = f"{self.config.raw_base_path}/{table}"
        checkpoint_path  = f"{self.config.checkpoint_base_path}/raw/{table}"
        dead_letter_path = f"{self.config.dead_letter_base_path}/{table}"

        stream = self._kafka_stream_for(topic)

        return (
            stream.writeStream
            .foreachBatch(
                lambda df, bid, t=topic, rp=raw_path, dl=dead_letter_path:
                    process_cdc_batch(df, bid, topic=t, raw_path=rp, dead_letter_path=dl)
            )
            .trigger(processingTime=f"{self.config.trigger_seconds} seconds")
            .option("checkpointLocation", checkpoint_path)
            .queryName(f"raw_{table}")
            .start()
        )

    def start(self) -> list:
        logger.info("job=%s topics=%s", self.JOB_NAME, list(TOPIC_SCHEMA_MAP.keys()))
        queries = [self._start_query(topic) for topic in TOPIC_SCHEMA_MAP]
        logger.info("job=%s active_queries=%d", self.JOB_NAME, len(queries))
        return queries