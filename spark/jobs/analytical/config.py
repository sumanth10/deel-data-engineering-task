from pydantic import Field
from common.config import BaseConfig


class AnalyticalConfig(BaseConfig):
    trigger_seconds: int = Field(default=20, ge=1)
    analytics_jdbc_url: str = Field(..., description="JDBC URL for analytics PostgreSQL.")
    analytics_db_user: str = Field(...)
    analytics_db_password: str = Field(...)
    # Serial single-connection write is correct for pre-aggregated KPI tables
    # that are already tiny. Parallelism would add overhead not value here.
    jdbc_num_partitions: int = Field(default=1, ge=1)