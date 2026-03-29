FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip netcat-openbsd wget && \
    rm -rf /var/lib/apt/lists/*

RUN wget -q -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar \
    https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.9.3/antlr4-runtime-4.9.3.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

WORKDIR /opt/spark-jobs

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY jobs/ ./jobs/
COPY pipeline.py .
COPY infra/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN mkdir -p /tmp/checkpoints /delta/raw /delta/logistics /delta/dead_letter && \
    chown -R spark:spark /opt/spark-jobs /tmp/checkpoints /delta && \
    chown spark:spark /entrypoint.sh

USER spark

ENTRYPOINT ["/entrypoint.sh"]