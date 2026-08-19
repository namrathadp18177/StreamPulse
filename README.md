# StreamPulse — Real-Time Streaming Analytics Platform

Fault-tolerant, event-driven analytics platform built on a Lambda Architecture.
A producer simulates high-throughput device events (50K+ events/minute target),
Apache Kafka buffers the stream, PySpark Structured Streaming performs
rolling-window anomaly detection per device, and a Streamlit dashboard renders
anomalies in near real time.

## Architecture

```
producer (Kafka producer)
        |
        v
   Kafka topic: events
        |
        v
PySpark Structured Streaming (anomaly_detector.py)
  - 30s sliding window, 5s slide, 20s watermark
  - per-device rolling z-score anomaly scoring
        |
        +--> Kafka topic: anomalies  --> Streamlit dashboard (live)
        +--> Parquet sink (cold/batch layer for replay & audits)
```

This is a Lambda Architecture: the Kafka -> anomalies path is the speed layer
(sub-second-to-seconds latency for the dashboard), and the Parquet sink is the
batch layer (durable, replayable, used for backfills or offline analysis).

## Components

- `producer/event_generator.py` — simulates device readings, injects anomalies
  at a configurable rate, publishes to Kafka.
- `streaming/anomaly_detector.py` — PySpark Structured Streaming job. Computes
  windowed mean/stddev per device and flags anomalies where the deviation
  exceeds `Z_THRESHOLD` (default 3.0).
- `dashboard/app.py` — Streamlit app that consumes the `anomalies` topic in a
  background thread and renders live charts + an anomaly table.

## Run locally with Docker Compose

```bash
docker compose up --build
```

This starts Zookeeper, Kafka, topic initialization, the producer, the Spark
streaming job, and the dashboard.

Dashboard: http://localhost:8501

## Run components individually (no Docker)

Requires a running Kafka broker on `localhost:9092`.

```bash
# 1. Producer
cd producer
pip install -r requirements.txt
python event_generator.py --rate 1000

# 2. Streaming job (requires Spark + Kafka connector package)
cd streaming
pip install -r requirements.txt
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 anomaly_detector.py

# 3. Dashboard
cd dashboard
pip install -r requirements.txt
streamlit run app.py
```

## Configuration (env vars)

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `SOURCE_TOPIC` | `events` | Raw event topic |
| `SINK_TOPIC` | `anomalies` | Scored/anomaly output topic |
| `Z_THRESHOLD` | `3.0` | Std-dev threshold for anomaly flag |
| `CHECKPOINT_DIR` | `/tmp/streampulse/checkpoints` | Spark streaming checkpoints |
| `PARQUET_OUT` | `/tmp/streampulse/anomalies_parquet` | Batch-layer sink path |


