#pipeline.py — unified entry point for the ACME analytics pipeline
import logging
import os
import sys

sys.path.insert(0, "/opt/spark-jobs/jobs")

from pyspark.sql import SparkSession

from raw.config import RawIngestionConfig
from raw.job import RawIngestionJob


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("pipeline")


def build_session() -> SparkSession:
    # Config already injected via spark-submit --conf flags in entrypoint.sh.
    # getOrCreate() picks up that existing session rather than creating a new one.
    return SparkSession.builder.appName("acme-analytics-pipeline").getOrCreate()


def main() -> None:
    spark = build_session()
    logger.info("SparkSession started — version=%s", spark.version)

    raw_config        = RawIngestionConfig()


    # Each job registers its streaming queries against the shared SparkSession.
    raw_queries        = RawIngestionJob(spark, raw_config).start()


    all_queries = raw_queries
    logger.info("Pipeline running — active_queries=%d", len(all_queries))
    logger.info("Spark UI available at http://localhost:4040")

    # Block until any query stops. If one fails, log it and exit so the
    # container restarts (relies on restart: unless-stopped in docker-compose).
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