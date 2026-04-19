
# 📈 Real-Time Stock Market Data Pipeline

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-C72E49?style=for-the-badge&logo=minio&logoColor=white)

> A production-grade, end-to-end data engineering pipeline that ingests IBM stock market data in real time via Apache Kafka, processes it with Apache Spark (both streaming and batch), stores the enriched output as partitioned Parquet files in a MinIO data lake, and monitors the entire system with Prometheus and Grafana — fully containerised with Docker.

---

## 📌 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [Tech Stack](#️-tech-stack)
- [Project Structure](#-project-structure)
- [Data Pipeline Flow](#-data-pipeline-flow)
- [Features](#-features)
- [Transformations](#-transformations)
- [How to Run](#-how-to-run)
- [Monitoring Setup](#-monitoring-setup)
- [Key Engineering Decisions](#-key-engineering-decisions)
- [Challenges & Debugging](#-challenges--debugging)
- [Future Improvements](#-future-improvements)
- [Author](#-author)

---

## 🧠 Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│           Alpha Vantage API / Yahoo Finance / Historical CSV        │
└────────────────────────┬────────────────────────────────────────────┘
                         │
              ┌──────────▼──────────┐
              │    Apache Kafka     │  ← Real-time stock tick ingestion
              │  (Message Broker)   │
              └──────────┬──────────┘
                         │
         ┌───────────────┼───────────────┐
         │               │               │
         ▼               ▼               ▼
  ┌─────────────┐  ┌───────────┐  ┌───────────────┐
  │Spark Stream │  │Spark Batch│  │   Raw CSV      │
  │  Processing │  │Processing │  │  (MinIO/S3A)   │
  └──────┬──────┘  └─────┬─────┘  └───────┬───────┘
         │               │                │
         └───────────────▼────────────────┘
                         │
              ┌──────────▼──────────┐
              │  Transformations    │  ← daily_returns, typical_price,
              │  (PySpark Engine)   │     price_range, date enrichment
              └──────────┬──────────┘
                         │
              ┌──────────▼──────────┐
              │   MinIO Data Lake   │  ← Partitioned Parquet output
              │  (S3-Compatible)    │     /year/month/day/
              └──────────┬──────────┘
                         │
              ┌──────────▼──────────┐
              │  Apache Airflow     │  ← DAG orchestration & scheduling
              └──────────┬──────────┘
                         │
              ┌──────────▼──────────┐
              │ Prometheus + Grafana│  ← Metrics, dashboards & alerts
              └─────────────────────┘
```

All services are orchestrated via **Docker Compose**, making the full stack reproducible in a single command.

---

## ⚙️ Tech Stack

| Technology | Role | Why It Was Chosen |
|---|---|---|
| **Apache Kafka** | Real-time message broker | Decouples data producers from consumers; handles high-throughput tick data with durability and replay capability |
| **Apache Spark** | Batch + streaming compute engine | Unified API for both historical (batch) and live (structured streaming) data processing at scale |
| **Apache Airflow** | Workflow orchestration | DAG-based scheduling with visibility, retries, and dependency management — industry standard for data pipelines |
| **MinIO** | Object storage / data lake | S3-compatible open-source storage; enables local development with the same interface as AWS S3 in production |
| **Prometheus** | Metrics collection | Pull-based monitoring that scrapes Kafka, Spark, and Airflow metrics with low overhead |
| **Grafana** | Observability dashboards | Provides real-time visualisation of pipeline health, consumer lag, and job throughput |
| **Docker** | Containerisation | Ensures environment parity across development and production; the entire stack runs with one command |
| **Python / PySpark** | Processing language | Expressive, widely adopted in data engineering; PySpark enables distributed computation with familiar Python syntax |

---

## 📂 Project Structure

```
real-time-stock-data-engineering/
│
├── dags/                          # Apache Airflow DAG definitions
│   ├── batch_processing.py        # Schedules and triggers Spark batch jobs
│   └── streaming_with_kafka.py    # Manages Kafka consumer + Spark streaming lifecycle
│
├── jobs/                          # PySpark job scripts
│   ├── spark_batching_jobs.py     # Reads historical CSV from MinIO, transforms, writes Parquet
│   ├── spark_streaming_jobs.py    # Consumes Kafka topic, applies streaming transformations
│   └── Dockerfile.spark           # Custom Spark image with S3A + Kafka JARs pre-installed
│
├── Monitoring/
│   ├── grafana/
│   │   ├── dashboards/            # Pre-provisioned JSON dashboard definitions
│   │   ├── datasources/           # Prometheus datasource auto-configuration
│   │   └── provisioning/          # Grafana provisioning config (dashboards + datasources)
│   └── prometheus/
│       ├── prometheus.yml         # Scrape targets and intervals
│       └── rules/
│           └── alert_rules.yml    # Alerting rules (consumer lag, job failure, data freshness)
│
├── config/                        # Connection configs, Spark session settings, Kafka params
├── logs/                          # Airflow and Spark task execution logs
├── plugins/                       # Custom Airflow operators or hooks (if any)
├── testing/                       # Unit and integration tests
├── docker-compose.yml             # Full stack: Kafka, Spark, Airflow, MinIO, Grafana, Prometheus
├── .env                           # Environment variables (API keys, credentials, ports)
└── .gitignore
```

---

## 🔄 Data Pipeline Flow

The pipeline supports two execution modes — **streaming** for real-time tick data and **batch** for historical analysis — both feeding the same transformation and storage layer.

### Mode 1 — Real-Time Streaming

```
1. Stock tick data arrives via Alpha Vantage or Yahoo Finance API
        │
2. A Kafka Producer publishes records to the `stock-data` topic
        │
3. Spark Structured Streaming consumes the Kafka topic (micro-batch)
        │
4. PySpark applies transformations: schema enforcement, metric computation
        │
5. Enriched records are written as partitioned Parquet to MinIO
        │
6. Airflow DAG monitors the streaming job and handles restarts
        │
7. Prometheus scrapes metrics; Grafana displays consumer lag + throughput
```

### Mode 2 — Batch Processing

```
1. Historical IBM stock CSV files land in MinIO (raw zone)
        │
2. Airflow triggers the batch DAG on schedule
        │
3. Spark reads the CSV with an explicit schema (no schema inference in prod)
        │
4. PySpark transforms and computes financial metrics
        │
5. Output is written as partitioned Parquet to MinIO (processed zone)
        │
6. Downstream consumers (BI tools, ML models) query the processed zone
```

---

## 🧪 Features

### ⚡ Real-Time Streaming
- Kafka topic ingestion with configurable consumer group offsets
- Spark Structured Streaming with micro-batch processing
- Fault-tolerant with checkpointing for exactly-once semantics

### 🗂️ Batch Processing
- Reads raw historical CSVs from MinIO using Spark's S3A connector
- Explicit schema definition (avoids expensive schema inference in production)
- Outputs partitioned Parquet for efficient downstream querying

### 📡 Observability
- Prometheus scrapes Kafka consumer lag, Spark executor metrics, and Airflow task states
- Grafana dashboards provide at-a-glance pipeline health
- Alert rules fire on consumer lag spikes, job failures, and stale data

### 🐳 Fully Containerised
- Single `docker-compose up` starts the entire stack
- Custom Spark Docker image bundles required JARs (Hadoop AWS, Kafka connector)
- Environment variables managed via `.env` for easy configuration

---

## 📊 Transformations

All transformations are implemented in PySpark and applied consistently across both batch and streaming modes.

### Date Enrichment
Parses the raw `Date` string and extracts time components for partitioning and time-series analysis:

```python
df = df.withColumn('Date', to_date(col("Date"), "yyyy-MM-dd")) \
       .withColumn('year',  year('Date'))  \
       .withColumn('month', month('Date')) \
       .withColumn('day',   day('Date'))
```

### Daily Returns
Measures the percentage price change from market open to close — a core metric in financial analysis:

```
daily_returns = (Close - Open) / Open
```

```python
df = df.withColumn('daily_returns', (col("Close") - col("Open")) / col("Open"))
```

A positive value indicates a price increase during the trading session; negative indicates a decline.

### Price Range
Captures intraday volatility — the spread between the highest and lowest traded price:

```
price_range = High - Low
```

```python
df = df.withColumn('price_range', col("High") - col("Low"))
```

High price range signals high volatility; low range indicates consolidation.

### Typical Price
A smoothed price representation used in technical analysis (basis for indicators like VWAP):

```
typical_price = (High + Low + Close) / 3
```

```python
df = df.withColumn('typical_price', (col("High") + col("Low") + col("Close")) / 3)
```

### Final Schema

The enriched output written to MinIO contains:

| Column | Type | Description |
|---|---|---|
| `Date` | Date | Trading date |
| `year` | Integer | Extracted year (partition key) |
| `month` | Integer | Extracted month (partition key) |
| `day` | Integer | Extracted day |
| `Open` | Float | Opening price |
| `High` | Float | Intraday high |
| `Low` | Float | Intraday low |
| `Close` | Float | Closing price |
| `Volume` | Float | Trade volume |
| `daily_returns` | Float | % price change Open → Close |
| `price_range` | Float | Intraday volatility (High - Low) |
| `typical_price` | Float | Average of High, Low, Close |

Output is partitioned as `/year=YYYY/month=MM/` in MinIO for efficient predicate pushdown.

---

## 🐳 How to Run

### Prerequisites

- Docker & Docker Compose installed
- Alpha Vantage API key (free tier: [alphavantage.co](https://www.alphavantage.co/support/#api-key))
- Minimum 8GB RAM allocated to Docker

### 1. Clone the Repository

```bash
git clone https://github.com/<your-username>/real-time-stock-data-engineering.git
cd real-time-stock-data-engineering
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```env
# API
ALPHA_VANTAGE_API_KEY=your_key_here

# MinIO
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_ENDPOINT=http://minio:9000

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=stock-data

# Airflow
AIRFLOW_UID=50000
```

### 3. Start the Full Stack

```bash
docker-compose up -d
```

This starts: Zookeeper → Kafka → Schema Registry → Spark Master + Worker → MinIO → Airflow → Prometheus → Grafana.

Allow ~60 seconds for all services to become healthy.

### 4. Verify Services

```bash
docker-compose ps
```

| Service | URL | Default Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Spark UI | http://localhost:8081 | — |

### 5. Trigger the Pipelines

**Batch pipeline:**
```bash
# Via Airflow UI — enable the `batch_processing` DAG
# Or trigger manually:
docker exec airflow-scheduler airflow dags trigger batch_processing
```

**Streaming pipeline:**
```bash
# Via Airflow UI — enable the `streaming_with_kafka` DAG
docker exec airflow-scheduler airflow dags trigger streaming_with_kafka
```

### 6. Tear Down

```bash
docker-compose down -v   # -v removes volumes (clears MinIO data and Kafka offsets)
```

---

## 📡 Monitoring Setup

### Prometheus

Prometheus is configured to scrape metrics from:

- **Kafka** — broker throughput, consumer group lag, topic offsets
- **Spark** — executor CPU/memory, task duration, shuffle metrics
- **Airflow** — DAG run states, task success/failure rates

Scrape targets are defined in `Monitoring/prometheus/prometheus.yml`. Intervals are set to 15s for streaming jobs and 60s for batch.

### Grafana

Dashboards are auto-provisioned on startup from `Monitoring/grafana/dashboards/`. No manual import needed.

Key dashboards:
- **Pipeline Health** — Kafka consumer lag, Spark active tasks, Airflow DAG status
- **Stock Data Volume** — Records processed per minute (streaming) and per run (batch)
- **Infrastructure** — Container CPU, memory, and network I/O

### Alerting

Alert rules in `Monitoring/prometheus/rules/alert_rules.yml` cover:

| Alert | Condition | Severity |
|---|---|---|
| `KafkaConsumerLagHigh` | Lag > 1000 messages for 5 min | Warning |
| `SparkJobFailed` | Executor exits with non-zero code | Critical |
| `DataFreshnessBreach` | No new records written to MinIO in 30 min | Warning |

---

## 💡 Key Engineering Decisions

**Why Kafka instead of direct API polling?**
Kafka decouples the producer (API ingestor) from the consumer (Spark). This means the streaming job can be restarted, scaled, or replaced without losing data — Kafka retains messages for the configured retention period. It also enables multiple consumers (e.g., Spark + a separate alerting service) on the same stream.

**Why MinIO over a traditional database?**
Stock data is append-heavy and read in bulk (time-range queries). A columnar Parquet format on object storage is orders of magnitude more efficient for this access pattern than row-based databases. MinIO also provides an S3-compatible API, making a future migration to AWS S3 a configuration change, not a code change.

**Why Spark for both batch and streaming?**
Using a single compute engine for both modes means one codebase, one set of transformations, and one team skill set. Spark Structured Streaming uses the same DataFrame API as batch, so the transformation logic (`daily_returns`, `typical_price`, etc.) is written once and reused.

**Why Airflow for orchestration?**
Airflow's DAG model provides explicit dependency graphs, configurable retries, SLA monitoring, and a UI for operational visibility — capabilities that cron jobs or simple scripts cannot offer. It also integrates natively with Docker and Spark via operators.

**Why partitioned Parquet output?**
Partitioning by `/year/month/` allows query engines (Spark, Trino, Athena) to skip entire partitions via predicate pushdown — reducing I/O by 90%+ on time-bounded queries.

---

## 🚨 Challenges & Debugging

### 1. Spark Worker Discovery Failures
**Problem:** Spark jobs submitted to the master failed with `No workers available` errors intermittently.

**Root cause:** The Spark worker registered with its Docker container hostname, which was not resolvable from the master when using a custom network bridge.

**Fix:** Explicitly set `SPARK_WORKER_HOSTNAME` in `docker-compose.yml` to the service name, and ensured all Spark services were on the same Docker network with DNS resolution enabled.

---

### 2. MinIO S3A Configuration
**Problem:** PySpark could not read from or write to MinIO, throwing `java.io.IOException: No FileSystem for scheme: s3a`.

**Root cause:** The Spark Docker image did not include the `hadoop-aws` and `aws-java-sdk-bundle` JARs required for S3A support.

**Fix:** Added the JARs to the custom `Dockerfile.spark` and configured the SparkSession with:
```python
spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY")) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```
The `path.style.access=true` setting is critical — MinIO does not support virtual-hosted-style S3 URLs by default.

---

### 3. Kafka Connectivity from Spark
**Problem:** Spark Structured Streaming could not connect to Kafka, throwing `org.apache.kafka.common.errors.TimeoutException`.

**Root cause:** Kafka was advertising its internal Docker hostname (`kafka:9092`) as the bootstrap server. Spark, running in a separate container, could reach this hostname, but the advertised listener in Kafka's config was set to `localhost:9092` — unreachable from other containers.

**Fix:** Configured two listeners in Kafka:
```yaml
KAFKA_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:29092
KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:29092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
```
Spark connects via `kafka:9092` (internal); external tools connect via `localhost:29092`.

---

## 🔮 Future Improvements

- [ ] **dbt integration** — add a semantic/transformation layer on top of MinIO for modular, testable SQL models
- [ ] **Great Expectations** — data quality checks and schema validation between pipeline stages
- [ ] **Cloud deployment** — migrate to AWS (MSK + EMR + S3) or GCP (Pub/Sub + Dataproc + GCS) with Terraform IaC
- [ ] **Delta Lake** — replace raw Parquet with Delta format for ACID transactions, time travel, and schema evolution
- [ ] **Expand to multi-ticker** — generalise the pipeline beyond IBM to support a configurable list of stock symbols
- [ ] **ML integration** — feed enriched Parquet data into a feature store for price prediction models
- [ ] **CI/CD pipeline** — GitHub Actions for automated testing, Docker image builds, and DAG validation on every push

---

## 🧑‍💻 Author

**Breta**
Data Engineer | Nairobi, Kenya

Building data systems for African markets — from real-time pipelines to ML infrastructure.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat&logo=linkedin)](https://linkedin.com/in/<your-profile>)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=flat&logo=github)](https://github.com/<your-username>)

---

## 📄 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
