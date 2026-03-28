from pydantic import Field
from common.config import BaseConfig


class RawIngestionConfig(BaseConfig):
    trigger_seconds: int = Field(default=10, ge=1)
    # Without this cap, the Debezium initial snapshot (all historical rows
    # emitted as op='r') floods a single batch and causes OOM on the executor.
    max_offsets_per_trigger: int = Field(default=10_000, ge=1)
    # customers and products change rarely compared to orders and order_items.
    # A lower cap keeps their batches lean without wasting executor resources.
    max_offsets_per_trigger_low_volume: int = Field(default=2_000, ge=1)