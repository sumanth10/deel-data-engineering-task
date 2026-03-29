from pydantic import Field
from common.config import BaseConfig


class LogisticsConfig(BaseConfig):
    trigger_seconds: int = Field(default=10, ge=1)
    max_files_per_trigger: int = Field(default=100, ge=1)