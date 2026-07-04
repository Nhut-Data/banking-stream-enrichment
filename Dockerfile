# =============================================================================
# Dockerfile — Custom Airflow image cho Banking Stream-Table Join Platform
#
# Base: apache/airflow:3.2.2 (khớp với docker-compose.yml)
#
# Providers cài thêm so với base image:
#   - apache-airflow-providers-google     : BigQuery operator (Bước 6)
#   - apache-airflow-providers-postgres   : Postgres hook (load Berka data)
#   - apache-airflow-providers-apache-kafka: Kafka sensor nếu cần trigger DAG
#
# Python packages cài thêm:
#   - kafka-python  : Speed Layer consumer (dùng trong src/)
#   - dbt-bigquery  : chạy dbt từ Airflow BashOperator
#   - pandas        : xử lý data trong DAG
#   - psycopg2-binary: Postgres driver
# =============================================================================

FROM apache/airflow:3.2.2

# Switch sang root để cài system dependencies
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    # build-essential: cần để compile một số Python packages
    build-essential \
    # libpq-dev: Postgres C headers, cần cho psycopg2
    libpq-dev \
    # curl: dùng trong healthcheck và script
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch về airflow user trước khi pip install
# Bắt buộc: airflow image không cho phép pip install với root
USER airflow

# Cài Airflow providers + Python packages cần thiết cho project
# Dùng --constraint để đảm bảo version tương thích với Airflow 3.2.2
# (tránh dependency conflict — common issue khi tự install providers)
RUN pip install --no-cache-dir \
    # Airflow providers
    apache-airflow-providers-google==10.27.0 \
    apache-airflow-providers-postgres==5.12.0 \
    apache-airflow-providers-apache-kafka==1.6.1 \
    # Python packages cho pipeline
    kafka-python==2.0.2 \
    dbt-bigquery==1.8.0 \
    dbt-postgres==1.8.0 \
    pandas==2.2.2 \
    psycopg2-binary==2.9.9 \
    # Data generation dependencies (dùng trong data_generation/)
    numpy==1.26.4 \
    scipy==1.13.0