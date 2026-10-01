FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y openjdk-17-jre-headless procps \
    && mkdir -p /opt/airflow/data \
    && chown -R airflow:0 /opt/airflow/data \
    && chmod -R 775 /opt/airflow/data \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.5 \
    clickhouse-connect==0.8.17 \
    requests==2.32.3

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

