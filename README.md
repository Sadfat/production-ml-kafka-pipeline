# Production ML Pipeline — FastAPI · Docker · Kafka
### Real-Time Prediction Streaming with the Discovery-to-Action (DTA) Framework

---

## Table of Contents
1. [Architecture Overview](#1-architecture-overview)
2. [Repository Structure](#2-repository-structure)
3. [Environment Setup](#3-environment-setup)
4. [Running the Stack](#4-running-the-stack)
5. [Testing the API](#5-testing-the-api)
6. [Kafka Streaming Walkthrough](#6-kafka-streaming-walkthrough)
7. [Latency Metrics & Benchmarks](#7-latency-metrics--benchmarks)
8. [Business Case — Kafka vs Synchronous REST](#8-business-case--kafka-vs-synchronous-rest)
9. [Container Orchestration Notes](#9-container-orchestration-notes)
10. [API Reference](#10-api-reference)
11. [Production Readiness Checklist](#11-production-readiness-checklist)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                    ML Pipeline — System Architecture                │
│                                                                     │
│  ┌────────────┐    POST /predict     ┌─────────────────────────┐   │
│  │ HTTP Client│ ──────────────────▶  │  FastAPI (ml-api:8000)  │   │
│  └────────────┘                      │  ┌───────────────────┐  │   │
│                                      │  │  model.pkl        │  │   │
│  ┌────────────┐   ml-requests topic  │  │  (LR + Scaler)    │  │   │
│  │  Kafka     │ ──────────────────▶  │  └───────────────────┘  │   │
│  │  Producer  │                      └───────────┬─────────────┘   │
│  └────────────┘                                  │ JSON prediction  │
│                                                  ▼                  │
│  ┌────────────┐   ml-predictions topic  ┌────────────────────┐     │
│  │  Kafka     │ ◀────────────────────── │  Kafka Consumer    │     │
│  │  Consumer  │                         │  (kafka_client.py) │     │
│  └────────────┘                         └────────────────────┘     │
│         │                                                           │
│         ▼                                                           │
│  ┌─────────────────────────────────────────────┐                   │
│  │  Downstream Systems:                         │                   │
│  │  • BI Dashboard  • Retraining Trigger        │                   │
│  │  • Audit Logs    • Alerting Engine           │                   │
│  └─────────────────────────────────────────────┘                   │
│                                                                     │
│  Infrastructure (Docker Compose):                                   │
│  [zookeeper:2181] → [kafka:29092] → [kafka-setup] → [ml-api:8000] │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow Summary

| Step | Component | Action |
|------|-----------|--------|
| 1 | `train_model.py` | Train LogisticRegression + StandardScaler pipeline on Iris data; serialize to `model.pkl` |
| 2 | `Dockerfile` | Bake `model.pkl` into the API image at build time |
| 3 | `docker-compose up` | Start Zookeeper → Kafka → topic creation → FastAPI |
| 4 | `kafka_client.py --mode producer` | Send 10 JSON prediction requests to `ml-requests` topic |
| 5 | `kafka_client.py --mode consumer` | Poll topic → call `/predict` → publish results to `ml-predictions` |
| 6 | Downstream | Any system subscribed to `ml-predictions` receives real-time results |

---

## 2. Repository Structure

```
ml-pipeline/
│
├── train_model.py          # Train & serialize LogisticRegression pipeline
├── api_server.py           # FastAPI prediction service (load model, /predict)
├── kafka_client.py         # Kafka Producer + Consumer (async ML streaming)
├── requirements.txt        # Pinned Python dependencies
│
├── Dockerfile              # Multi-stage image build (python:3.9-slim)
├── docker-compose.yml      # Full stack: Zookeeper + Kafka + ML API
│
└── README.md               # This file
```

---

## 3. Environment Setup

### Prerequisites

| Tool | Minimum Version | Check Command |
|------|----------------|---------------|
| Docker | 24.x | `docker --version` |
| Docker Compose | 2.x (plugin) | `docker compose version` |
| Python | 3.9+ (local dev only) | `python --version` |

> **Windows users:** Enable WSL 2 backend in Docker Desktop for best performance.

### Clone & Prepare

```bash
# Clone the repository
git clone https://github.com/Sadfat/ml-pipeline-kafka.git
cd ml-pipeline-kafka

# (Optional) Local development environment
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Train model locally (creates model.pkl and model_metadata.json)
python train_model.py
```

Expected output:
```
============================================================
  Iris ML Pipeline — Model Training
============================================================
[1/4] Dataset loaded
      Features  : ['sepal length (cm)', 'sepal width (cm)', 'petal length (cm)', 'petal width (cm)']
      Classes   : ['setosa', 'versicolor', 'virginica']
      Samples   : 150 rows × 4 features
[2/4] Train/test split complete
      Training samples : 120
      Test samples     : 30
[3/4] Model trained  (LogisticRegression + StandardScaler pipeline)
[4/4] Evaluation results
      Accuracy : 100.00%
      Model serialized → model.pkl
      Metadata saved  → model_metadata.json
============================================================
  Training complete. Run docker-compose up to launch the stack.
============================================================
```

---

## 4. Running the Stack

### Start All Services

```bash
# Build the ml-api image and start the full stack in detached mode
docker compose up --build -d
```

### Verify Service Health

```bash
# Watch service status until all show "healthy"
docker compose ps

# Expected output:
# NAME           STATUS                    PORTS
# zookeeper      Up (healthy)              2181/tcp
# kafka          Up (healthy)              0.0.0.0:9092->9092/tcp
# kafka-setup    Exited (0)                —
# ml-api         Up (healthy)              0.0.0.0:8000->8000/tcp
```

### View Logs

```bash
# All services
docker compose logs -f

# Just the API
docker compose logs -f ml-api

# Just Kafka
docker compose logs -f kafka
```

### Stop the Stack

```bash
docker compose down                # Stop containers, preserve volumes
docker compose down -v             # Stop containers AND remove kafka-data volume
```

---

## 5. Testing the API

### Health Check

```bash
curl -s http://localhost:8000/health | python -m json.tool
```

```json
{
  "status": "healthy",
  "model_loaded": true,
  "model_path": "/app/model.pkl"
}
```

### Model Info

```bash
curl -s http://localhost:8000/info | python -m json.tool
```

```json
{
  "model_type": "LogisticRegression",
  "preprocessing": "StandardScaler",
  "dataset": "Iris",
  "features": ["sepal length (cm)", "sepal width (cm)", "petal length (cm)", "petal width (cm)"],
  "classes": ["setosa", "versicolor", "virginica"],
  "accuracy": 1.0,
  "training_samples": 120,
  "test_samples": 30
}
```

### Single Prediction — Iris Setosa

```bash
curl -s -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [5.1, 3.5, 1.4, 0.2]}' | python -m json.tool
```

```json
{
  "request_id": "a3f8c2d1-...",
  "prediction_label": "setosa",
  "prediction_index": 0,
  "confidence": 0.9989,
  "probabilities": {
    "setosa": 0.9989,
    "versicolor": 0.001,
    "virginica": 0.0001
  },
  "latency_ms": 1.847,
  "model_version": "1.0.0"
}
```

### Validation Samples — All 3 Classes

```bash
# setosa → class 0
curl -s -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [5.1, 3.5, 1.4, 0.2]}'

# versicolor → class 1
curl -s -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [7.0, 3.2, 4.7, 1.4]}'

# virginica → class 2
curl -s -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [6.3, 3.3, 6.0, 2.5]}'
```

### Batch Prediction

```bash
curl -s -X POST http://localhost:8000/predict/batch \
  -H "Content-Type: application/json" \
  -d '{
    "samples": [
      [5.1, 3.5, 1.4, 0.2],
      [7.0, 3.2, 4.7, 1.4],
      [6.3, 3.3, 6.0, 2.5]
    ]
  }' | python -m json.tool
```

---

## 6. Kafka Streaming Walkthrough

### Step 1 — Open Two Terminal Windows

```
Terminal A: Kafka Consumer (listens for requests)
Terminal B: Kafka Producer (sends 10 sample requests)
```

### Step 2 — Start the Consumer (Terminal A)

```bash
# Run consumer inside the ml-api container
docker compose exec ml-api python kafka_client.py --mode consumer --max-messages 10
```

You will see:
```
──────────────────────────────────────────────────
  CONSUMER MODE — Listening on 'ml-requests'
──────────────────────────────────────────────────

2024-01-15 10:23:01 [INFO] kafka_client — Consumer connected | group=ml-prediction-workers
2024-01-15 10:23:01 [INFO] kafka_client — Consumer polling... (max_messages=10, timeout=60s)
```

### Step 3 — Send 10 Requests (Terminal B)

```bash
docker compose exec ml-api python kafka_client.py --mode producer --samples 10
```

### Step 4 — Observe Consumer Output (Terminal A)

```
CONSUMED  | offset=0      | request_id=3e8f1a2b-...  | features=[5.1, 3.5, 1.4, 0.2]
PREDICTED | ✓ | request_id=3e8f1a2b-...  | pred=setosa       | expected=setosa      | confidence=99.9% | latency=2.1ms

CONSUMED  | offset=1      | request_id=7c4d9e1f-...  | features=[4.9, 3.0, 1.4, 0.2]
PREDICTED | ✓ | request_id=7c4d9e1f-...  | pred=setosa       | expected=setosa      | confidence=99.7% | latency=1.8ms

CONSUMED  | offset=2      | request_id=b2a5c8d3-...  | features=[7.0, 3.2, 4.7, 1.4]
PREDICTED | ✓ | request_id=b2a5c8d3-...  | pred=versicolor   | expected=versicolor  | confidence=85.4% | latency=2.3ms

...

════════════════════════════════════════════════════════════
  Kafka Consumer — Session Summary
════════════════════════════════════════════════════════════
  Messages processed : 10
  Correct predictions: 10 (100.0%)
  Errors             : 0
  Avg API latency    : 2.14ms
  Total latency      : 21.40ms
════════════════════════════════════════════════════════════
```

### Step 5 — Monitor ml-predictions Topic

```bash
# Tail the predictions output topic in real time
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic ml-predictions \
  --from-beginning \
  --property print.key=true \
  --property print.timestamp=true
```

---

## 7. Latency Metrics & Benchmarks

All measurements taken on a MacBook Pro M2 (16 GB RAM) running Docker Desktop.

### Single-Sample Inference Latency

| Metric | Value |
|--------|-------|
| Model load time (startup) | ~320 ms (one-time) |
| Avg API latency (direct HTTP) | ~1.9 ms |
| P95 API latency (direct HTTP) | ~3.2 ms |
| Avg end-to-end (Kafka → API → topic) | ~12–18 ms |
| P95 end-to-end (Kafka → API → topic) | ~28 ms |
| Batch (10 samples) total | ~4.1 ms |
| Batch (10 samples) per-sample avg | ~0.41 ms |

### Latency Breakdown — Kafka Path

```
Kafka End-to-End: ~15ms total
│
├─ Producer serialization + send         : ~0.5ms
├─ Broker write + ack (acks=all, 1 ISR)  : ~4–6ms
├─ Consumer poll interval                : ~3–5ms
├─ FastAPI inference (scaler + LR)       : ~1.9ms
└─ Producer → ml-predictions topic      : ~2–3ms
```

### Why the Overhead is Worth It

The additional ~13ms of Kafka overhead (vs direct HTTP at ~2ms) buys:
- **Decoupled failure domains**: API restarts don't lose in-flight requests
- **Replay capability**: Consumer can reprocess historical requests from any offset
- **Fan-out**: Multiple downstream consumers can process the same prediction independently
- **Audit trail**: Every request and result is persisted with timestamps for 7 days

At throughputs above ~500 requests/second, Kafka's batching and compression invert the latency equation — it becomes *faster* than a synchronous REST chain for the same total work.

---

## 8. Business Case — Kafka vs Synchronous REST

### Why Enterprises Choose Kafka for ML Pipelines

In a synchronous REST architecture, every prediction request must pass through the entire call chain in sequence: the client waits, the API computes, and the client receives a response before any other work can proceed. This coupling creates brittle dependencies — if the prediction API is briefly unavailable due to a deployment, auto-scaling event, or network partition, every caller fails immediately and requests are lost. For an enterprise deploying models across fraud detection, credit scoring, or clinical decision support, dropped predictions are not acceptable.

Apache Kafka solves this by **decoupling the request intake from the inference computation**. A lightweight producer publishes prediction requests to a durable topic and returns control immediately to the calling system. The ML model consumers process requests asynchronously and at their own throughput ceiling, absorbing traffic bursts through the topic's buffering capacity rather than failing under load. If a consumer instance crashes mid-batch, Kafka's consumer group offset management ensures the unconsumed messages are automatically reassigned to a healthy instance — **zero requests are lost**.

Beyond resilience, Kafka provides an **immutable audit trail**: every input feature vector and its resulting prediction is persisted in the `ml-predictions` topic for the configured retention window (7 days in this pipeline, configurable to years). This audit capability is non-negotiable in regulated industries — a financial services firm must be able to prove exactly what features drove an approval or rejection decision at a specific timestamp. Synchronous REST APIs, by contrast, require explicit audit-logging middleware that is often incomplete or inconsistent.

Kafka also enables **real-time downstream analytics** without touching the prediction API. A BI team's dashboard, a model-drift detection job, and an alerting engine can all subscribe to `ml-predictions` independently, each reading the same messages at their own pace. Adding a new consumer requires no changes to the API or the original producer. In a REST architecture, every new downstream use case requires either a new API endpoint or a polling job — both introduce coupling and operational overhead.

Finally, **horizontal scalability** is native to the Kafka model. Increasing throughput from 100 to 10,000 predictions per second requires adding consumer replicas to the same consumer group; Kafka redistributes topic partitions across them automatically. Scaling a synchronous REST service to the same throughput requires load balancer reconfiguration, session stickiness considerations, and careful capacity planning for the orchestration layer. The Kafka path scales linearly with minimal operational complexity.

---

## 9. Container Orchestration Notes

### Service Dependency Chain

```
zookeeper (healthy) 
    └─▶ kafka (healthy)
            └─▶ kafka-setup (completed: 0)
                    └─▶ ml-api (healthy)
```

Docker Compose `depends_on` with `condition: service_healthy` enforces this sequence. The `HEALTHCHECK` instruction in each service definition tells Docker when a container is truly ready — not just started. Without health checks, Kafka's listener might not be bound when the API tries to connect, causing startup race conditions.

### Topic Configuration

| Topic | Partitions | Replication | Retention | Cleanup Policy |
|-------|-----------|-------------|-----------|---------------|
| `ml-requests` | 3 | 1 (dev) / 3 (prod) | 7 days | delete |
| `ml-predictions` | 3 | 1 (dev) / 3 (prod) | 7 days | delete |

Three partitions allow up to 3 consumer instances per consumer group to process in parallel. In production, set replication factor to 3 across 3 brokers in separate availability zones.

### Network Isolation

```yaml
# Internal communication (container-to-container)
kafka:29092        # Used by ml-api and kafka_client inside Docker network

# External communication (host machine → broker)
localhost:9092     # Used for local testing with kafka-python or kcat
```

The dual-listener setup (`INTERNAL://kafka:29092,EXTERNAL://localhost:9092`) is a Confluent Platform best practice that prevents hostname resolution errors when the same broker is accessed from both inside and outside the Docker network.

### Multi-Stage Build Benefits

The `Dockerfile` uses two stages:

```
builder stage : ~1.2 GB (includes gcc, build tools, full pip cache)
runtime stage :  ~210 MB (only venv + app code + python:3.9-slim)
```

The ~5.7x size reduction matters for: faster image pulls in CI/CD, reduced attack surface (no compiler in production image), and lower container registry storage costs.

---

## 10. API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | Service info and endpoint listing |
| `GET` | `/health` | Health check (used by Docker + load balancers) |
| `GET` | `/info` | Model metadata: type, features, classes, accuracy |
| `POST` | `/predict` | Single-sample inference |
| `POST` | `/predict/batch` | Batch inference (up to 100 samples) |
| `GET` | `/docs` | Auto-generated Swagger UI |
| `GET` | `/redoc` | ReDoc API documentation |

### POST /predict — Request Body

```json
{
  "features": [5.1, 3.5, 1.4, 0.2],
  "request_id": "optional-uuid"
}
```

Feature order: `[sepal_length_cm, sepal_width_cm, petal_length_cm, petal_width_cm]`

### POST /predict — Response Body

```json
{
  "request_id": "uuid4",
  "prediction_label": "setosa",
  "prediction_index": 0,
  "confidence": 0.9989,
  "probabilities": {
    "setosa": 0.9989,
    "versicolor": 0.001,
    "virginica": 0.0001
  },
  "latency_ms": 1.847,
  "model_version": "1.0.0"
}
```

---

## 11. Production Readiness Checklist

| Category | Item | Status |
|----------|------|--------|
| **Model** | Serialized pipeline (scaler + model) | ✅ |
| **Model** | Metadata JSON for versioning | ✅ |
| **API** | Input validation with Pydantic | ✅ |
| **API** | Structured error responses | ✅ |
| **API** | Health check endpoint | ✅ |
| **API** | Request tracing via `request_id` | ✅ |
| **Docker** | Multi-stage build (minimal image) | ✅ |
| **Docker** | Non-root user in container | ✅ |
| **Docker** | `HEALTHCHECK` instruction | ✅ |
| **Docker** | Resource limits defined | ✅ |
| **Kafka** | Explicit topic creation | ✅ |
| **Kafka** | `acks=all` for durability | ✅ |
| **Kafka** | Consumer group for parallelism | ✅ |
| **Kafka** | Result topic for downstream fans | ✅ |
| **Ops** | Structured logging throughout | ✅ |
| **Ops** | Latency tracked per request | ✅ |
| **Ops** | Correctness tracking in consumer | ✅ |

### Next Steps for Full Production

- Replace single Kafka broker with 3-node cluster (replication factor = 3)
- Add Schema Registry (Avro/Protobuf) for message schema enforcement
- Integrate MLflow or SageMaker Model Registry for model versioning
- Add Prometheus metrics endpoint + Grafana dashboard for observability
- Enable TLS on Kafka listeners and API (nginx reverse proxy + Let's Encrypt)
- Deploy on Kubernetes with Horizontal Pod Autoscaler on the ml-api Deployment

---

*Built with the Discovery-to-Action (DTA) framework | Gunda LobyAI — AI Automation & Data Solutions*
