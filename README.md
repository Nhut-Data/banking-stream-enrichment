# Banking Stream-Table Join Enrichment Platform

Real-time transaction enrichment pipeline mô phỏng fraud detection infrastructure trong ngân hàng. Transaction stream volume cao được enrich với customer profile trong real-time qua Stream-Table Join pattern — kiến trúc chuẩn trong fraud detection thực tế trước khi đến tầng ML.

---

## Architecture

```
Berka CSV → PostgreSQL → Debezium CDC → Kafka → Speed Layer → BigQuery
                                           ↓
                                      Batch Layer (dbt + Airflow)
```

**Data flow chi tiết:**

| Bước | Component | Vai trò |
|------|-----------|---------|
| 0 | `data_generation/` | Học phân phối thống kê từ Berka thật → sinh synthetic data + tiêm lỗi có chủ đích |
| 1 | PostgreSQL 16 | Core banking simulation — giữ 5 bảng Berka gốc |
| 2 | Debezium Connect | CDC qua WAL (log-based, không polling) → capture mọi thay đổi |
| 3 | Kafka (KRaft) | Message broker — topic riêng theo bảng nguồn |
| 4 | Speed Layer | Stream-Table Join: trans stream + profile State Table → enriched records |
| 5 | Batch Layer | Airflow + dbt: re-process toàn bộ, dedupe, data quality tests |
| 6 | BigQuery | Serving layer — hợp nhất Speed + Batch view |

**Tại sao Stream-Table Join, không phải Lambda Architecture?** → [ADR 001](docs/decisions/001-stream-table-join-not-lambda.md)

---

## Quickstart

### Prerequisites

- Docker Desktop + WSL2
- Python 3.10+
- `kafka-python==2.0.2`, `psycopg2-binary`, `dbt-postgres`

### 1. Clone và setup

```bash
git clone https://github.com/NhutData/banking-stream-enrichment.git
cd banking-stream-enrichment
cp .env.example .env
```

### 2. Khởi động hạ tầng

```bash
make up
# Chờ ~2 phút để tất cả services healthy
docker compose ps
```

### 3. Load Berka data vào Postgres

```bash
# Đặt Berka CSV files vào data/raw/
# Download từ: https://sorry.vse.cz/~berka/challenge/pkdd1999/berka.htm

# Load 5 bảng vào Postgres qua docker cp (bypass WSL2 socket issue)
docker cp data/raw/district.csv banking-stream-enrichment-postgres-1:/tmp/
docker cp data/raw/account.csv  banking-stream-enrichment-postgres-1:/tmp/
docker cp data/raw/client.csv   banking-stream-enrichment-postgres-1:/tmp/
docker cp data/raw/disp.csv     banking-stream-enrichment-postgres-1:/tmp/
docker cp data/raw/trans.csv    banking-stream-enrichment-postgres-1:/tmp/

docker exec banking-stream-enrichment-postgres-1 psql -U airflow -d berka -c \
  "COPY district FROM '/tmp/district.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '\"', NULL '?');"
docker exec banking-stream-enrichment-postgres-1 psql -U airflow -d berka -c \
  "COPY account FROM '/tmp/account.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '\"');"
docker exec banking-stream-enrichment-postgres-1 psql -U airflow -d berka -c \
  "COPY client FROM '/tmp/client.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '\"');"
docker exec banking-stream-enrichment-postgres-1 psql -U airflow -d berka -c \
  "COPY disposition (disp_id, client_id, account_id, type) FROM '/tmp/disp.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '\"');"
docker exec banking-stream-enrichment-postgres-1 psql -U airflow -d berka -c \
  "COPY trans (trans_id, account_id, date, type, operation, amount, balance, k_symbol, bank, account) FROM '/tmp/trans.csv' WITH (FORMAT csv, HEADER true, DELIMITER ';', QUOTE '\"', NULL '');"
```

### 4. Đăng ký Debezium CDC connector

```bash
make register-connector
# Verify: curl http://localhost:8083/connectors/berka-postgres-connector/status
```

### 5. Chạy Speed Layer

```bash
# Thêm kafka hostname vào /etc/hosts (1 lần duy nhất)
echo "$(docker inspect berka-kafka --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}') kafka" | sudo tee -a /etc/hosts

# Chạy Speed Layer trong Docker network
docker run --rm -it \
  --network banking-platform-net \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -v $(pwd)/speed_layer:/app/speed_layer \
  -w /app \
  python:3.11-slim \
  bash -c "pip install kafka-python==2.0.2 -q && python3 -m speed_layer.run"
```

### 6. Chạy Batch Layer (dbt)

```bash
cd dbt
dbt run    # 7 models
dbt test   # 31 tests
```

---

## Results

| Metric | Value |
|--------|-------|
| CDC messages (Kafka) | 1,056,324 |
| Speed Layer enriched | 776,000+ records |
| Speed Layer success rate | 100% |
| Speed Layer DLQ | 0 |
| dbt models | 7 (staging × 5, intermediate × 1, mart × 1) |
| dbt tests | 31 / 31 PASS |
| fct_enriched_transactions | 1,056,321 rows |

---

## Data Quality — Corruption có chủ đích

Synthetic data được tiêm 3 loại lỗi có kiểm soát để test pipeline:

| Loại lỗi | Tỷ lệ | Mục đích | Pipeline xử lý |
|----------|-------|----------|----------------|
| Duplicate records | 2% | Mô phỏng Kafka producer retry | dbt `unique(trans_id)` test |
| Referential violation | 2% | account_id không tồn tại | Speed Layer DLQ + dbt `relationships` test |
| Missing values | 3% | null ở field không bắt buộc | dbt `not_null` test |

Chi tiết: [`docs/corruption_manifest.json`](docs/corruption_manifest.json)

---

## Stack

| Layer | Technology |
|-------|-----------|
| Source | Berka PKDD'99 dataset |
| OLTP | PostgreSQL 16 |
| CDC | Debezium Connect 3.0 |
| Message Broker | Confluent Kafka 7.6.1 (KRaft) |
| Stream Processing | Python + kafka-python |
| Orchestration | Apache Airflow 3.x (CeleryExecutor) |
| Transformation | dbt-postgres 1.10 |
| Serving | BigQuery (Google Cloud) |
| Monitoring | Kafka UI |

---

## Architecture Decision Records

Các quyết định kiến trúc quan trọng được document trong `docs/decisions/`:

- [ADR 001](docs/decisions/001-stream-table-join-not-lambda.md) — Stream-Table Join thay vì Lambda Architecture
- [ADR 002](docs/decisions/002-berka-only-no-ibm-aml.md) — Berka dataset thay vì IBM AML / PaySim
- [ADR 003](docs/decisions/003-synthetic-data-generation.md) — Synthetic Data Generation để test hạ tầng
- [ADR 004](docs/decisions/004-deploy-once-no-cloud-sql.md) — Deploy 1 lần, không dùng Cloud SQL thường trực

---

## Project Structure

```
banking-stream-enrichment/
├── data_generation/     # Bước 0: sinh synthetic data + tiêm lỗi
├── infra/
│   ├── postgres/        # Schema SQL + load script
│   ├── debezium/        # Connector config
│   └── kafka/           # Topic setup script
├── speed_layer/         # Bước 4: Stream-Table Join (Python)
├── dbt/                 # Bước 5: Batch transform + data quality
│   └── models/
│       ├── staging/     # 1-1 với source tables, type casting
│       ├── intermediate/ # Join 4 profile tables → customer view
│       └── marts/       # fct_enriched_transactions
├── airflow/             # Bước 5: DAG orchestration
├── docs/
│   ├── decisions/       # Architecture Decision Records
│   └── corruption_manifest.json
└── tests/
    ├── unit/
    └── integration/
```