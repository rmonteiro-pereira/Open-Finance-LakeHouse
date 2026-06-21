# Cluster image for Open-Finance-Lakehouse Kedro pipelines.
# Self-contained Spark via the pyspark wheel (no standalone cluster needed) with
# S3A + Delta jars baked in so there is NO runtime Ivy/Maven resolution.
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONUTF8=1 \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless curl procps tini ca-certificates locales \
    && sed -i 's/# pt_BR.UTF-8/pt_BR.UTF-8/' /etc/locale.gen && locale-gen \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/app

# Python deps (curated — avoids the heavy apache-airflow in pyproject; the image
# only runs `kedro run`, orchestration lives in the cluster Airflow).
COPY requirements-cluster.txt ./
RUN pip install --no-cache-dir -r requirements-cluster.txt

# Bake S3A + Delta jars into pyspark/jars (versions matched to Spark 3.5.x).
# Also bake the OpenLineage Spark listener + kafka-clients so the Kedro Spark
# jobs can emit column-level lineage to Redpanda with NO runtime Ivy/Maven
# resolution (spark.jars.packages stays empty). kafka-clients is required by the
# OpenLineage Kafka transport (Spark 3.5 does not bundle it); producer
# compression defaults to none, so no lz4/snappy codec jars are needed.
ARG PYSPARK_JARS=/usr/local/lib/python3.11/site-packages/pyspark/jars
RUN cd "$PYSPARK_JARS" \
    && curl -fsSLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.50.0/openlineage-spark_2.12-1.50.0.jar \
    && curl -fsSLO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar

# Project source + editable install (so `kedro` resolves the package).
COPY . .
# The repo ships src/Open_Finance_Lakehouse (TitleCase) but pyproject declares
# package_name=open_finance_lakehouse. That only works on case-insensitive
# filesystems (Windows); normalize to lowercase so it imports on Linux.
RUN if [ -d src/Open_Finance_Lakehouse ]; then mv src/Open_Finance_Lakehouse src/open_finance_lakehouse; fi
RUN pip install --no-cache-dir -e . --no-deps

ENV LANG=pt_BR.UTF-8 LC_ALL=pt_BR.UTF-8
ENTRYPOINT ["tini", "--"]
CMD ["kedro", "run", "--env", "cluster", "--pipeline", "selic"]
