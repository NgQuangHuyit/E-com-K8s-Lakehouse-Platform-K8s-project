FROM ngquanghuyit/spark-delta-lake:3.3 as spark-client


FROM apache/airflow:2.9.2


USER root

# Cài đặt các gói thự viện hệ thống và Java
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        git \
        curl \
        default-jdk \
        procps \
        && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=spark-client /opt/bitnami/spark /home/airflow/spark

# Set Spark ENV
ENV SPARK_HOME=/home/airflow/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# (Optional) Hadoop/S3 configs nếu dùng s3a
ENV HADOOP_CONF_DIR=$SPARK_HOME/conf

# Tạo thư mục cho JDBC drivers
RUN mkdir -p /opt/airflow/jars && chmod -R 755 /opt/airflow/jars

# Tải Hive JDBC driver
RUN curl -L -o /opt/airflow/jars/hive-jdbc-3.1.3-standalone.jar \
    https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3-standalone.jar

USER airflow

# Cài đặt Airflow providers và các gói Python
RUN pip install --no-cache-dir \
        dbt-core==1.9.3 \
        "dbt-spark[PyHive]==1.9.3" \
        pyspark==3.3.4 \
        delta-spark==2.2.0 

RUN pip install --no-cache-dir \
    apache-airflow-providers-jdbc==4.3.1 \
    jaydebeapi==1.2.3 \
    JPype1==1.5.0 \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.2/constraints-3.12.txt \
    --no-deps
 

USER root
COPY ./ecom_lakehouse_pipeline /home/airflow/ecom_lakehouse_pipeline
RUN chown -R airflow:root /home/airflow/ecom_lakehouse_pipeline && \
    chmod -R 775 /home/airflow/ecom_lakehouse_pipeline

USER airflow
ENV DBT_HOME_PROJECT=/home/airflow/ecom_lakehouse_pipeline


