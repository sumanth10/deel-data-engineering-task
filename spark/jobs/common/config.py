from pydantic import Field
from pydantic_settings import BaseSettings


class BaseConfig(BaseSettings):
    kafka_bootstrap_servers: str = Field(..., description="Internal Kafka address reachable from Spark containers.")
    raw_base_path: str = Field(default="/delta/raw")
    logistics_base_path: str = Field(default="/delta/logistics")
    checkpoint_base_path: str = Field(default="/tmp/checkpoints")
    dead_letter_base_path: str = Field(default="/delta/dead_letter")
    # Default matches 2 executors × 2 cores. Overriding 200 prevents
    # 200 tiny files per micro-batch which would degrade every downstream read.
    spark_shuffle_partitions: int = Field(default=8, ge=1)

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}