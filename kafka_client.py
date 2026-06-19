"""
kafka_client.py
===============
Integrated Kafka Producer + Consumer for asynchronous ML prediction streaming.

Discovery-to-Action (DTA) Step: Technical Phase — Streaming Integration
─────────────────────────────────────────────────────────────────────────
ARCHITECTURE:
  Producer  → ml-requests topic  → Kafka Broker
  Consumer  ← ml-requests topic  ← Kafka Broker
  Consumer  → POST /predict      → FastAPI
  Consumer  → ml-predictions topic (logs results for downstream systems)

WHY KAFKA OVER SYNCHRONOUS REST:
  • Decoupling: Producer and Consumer operate independently; the API can be
    restarted or scaled without losing queued requests.
  • Audit trail: Every request and its prediction is durably persisted in
    Kafka topics with configurable retention — critical for regulated industries.
  • Real-time analytics: Downstream systems (dashboards, alerting, retraining
    triggers) can tap ml-predictions without touching the API.
  • Horizontal scalability: Add consumer replicas in the same consumer group
    to linearly scale throughput with no code changes.

USAGE:
  # Send 10 sample prediction requests
  python kafka_client.py --mode producer --samples 10

  # Start consuming and forwarding to FastAPI
  python kafka_client.py --mode consumer

  # Run both in separate terminals (or let Docker Compose handle it)
"""

import os
import sys
import json
import time
import uuid
import logging
import argparse
import requests
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaError

# ──────────────────────────────────────────────────────────────────────────────
# Configuration (overridden by Docker Compose environment variables)
# ──────────────────────────────────────────────────────────────────────────────
KAFKA_BROKER      = os.getenv("KAFKA_BROKER", "localhost:9092")
REQUEST_TOPIC     = os.getenv("KAFKA_REQUEST_TOPIC", "ml-requests")
PREDICTION_TOPIC  = os.getenv("KAFKA_PREDICTION_TOPIC", "ml-predictions")
API_URL           = os.getenv("API_URL", "http://localhost:8000/predict")
CONSUMER_GROUP    = os.getenv("KAFKA_CONSUMER_GROUP", "ml-prediction-workers")

# Iris sample data covering all 3 classes for validation
IRIS_SAMPLES = [
    # setosa (class 0)
    {"features": [5.1, 3.5, 1.4, 0.2], "expected": "setosa"},
    {"features": [4.9, 3.0, 1.4, 0.2], "expected": "setosa"},
    {"features": [4.7, 3.2, 1.3, 0.2], "expected": "setosa"},
    {"features": [5.4, 3.9, 1.7, 0.4], "expected": "setosa"},
    # versicolor (class 1)
    {"features": [7.0, 3.2, 4.7, 1.4], "expected": "versicolor"},
    {"features": [6.4, 3.2, 4.5, 1.5], "expected": "versicolor"},
    {"features": [5.5, 2.3, 4.0, 1.3], "expected": "versicolor"},
    # virginica (class 2)
    {"features": [6.3, 3.3, 6.0, 2.5], "expected": "virginica"},
    {"features": [5.8, 2.7, 5.1, 1.9], "expected": "virginica"},
    {"features": [7.1, 3.0, 5.9, 2.1], "expected": "virginica"},
]

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("kafka_client")


# ──────────────────────────────────────────────────────────────────────────────
# Utility: wait for Kafka to be ready (Docker startup sequencing)
# ──────────────────────────────────────────────────────────────────────────────
def wait_for_kafka(broker: str, retries: int = 10, delay: int = 5) -> bool:
    """
    Polls for Kafka broker availability before producing/consuming.
    Returns True if connected, False if all retries exhausted.
    """
    logger.info("Waiting for Kafka broker at %s ...", broker)
    for attempt in range(1, retries + 1):
        try:
            # A lightweight probe: create a producer and immediately close it
            probe = KafkaProducer(
                bootstrap_servers=[broker],
                request_timeout_ms=3000
            )
            probe.close()
            logger.info("Kafka broker ready after %d attempt(s)", attempt)
            return True
        except NoBrokersAvailable:
            logger.warning("Attempt %d/%d — broker not ready, retrying in %ds", attempt, retries, delay)
            time.sleep(delay)
    logger.error("Kafka broker unreachable after %d attempts", retries)
    return False


# ──────────────────────────────────────────────────────────────────────────────
# Producer: sends prediction requests to ml-requests topic
# ──────────────────────────────────────────────────────────────────────────────
class MLRequestProducer:
    """
    Kafka Producer that serializes prediction requests as JSON messages
    and publishes them to the ml-requests topic.

    Message schema:
    {
        "request_id": "uuid4",
        "timestamp": "ISO-8601",
        "features": [f1, f2, f3, f4],
        "expected_label": "setosa|versicolor|virginica"  # for validation only
    }
    """

    def __init__(self, broker: str = KAFKA_BROKER):
        self.broker = broker
        self.producer: Optional[KafkaProducer] = None

    def connect(self):
        self.producer = KafkaProducer(
            bootstrap_servers=[self.broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",                  # wait for all ISR replicas to acknowledge
            retries=3,
            compression_type="gzip",     # reduces network I/O at scale
            batch_size=16384,
            linger_ms=10                 # small batching window
        )
        logger.info("Producer connected to %s", self.broker)

    def send_request(self, features: list, expected_label: str = "unknown") -> str:
        """Publish a single prediction request. Returns the request_id."""
        request_id = str(uuid.uuid4())
        message = {
            "request_id": request_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "features": features,
            "expected_label": expected_label
        }

        future = self.producer.send(
            REQUEST_TOPIC,
            key=request_id,
            value=message
        )
        record_metadata = future.get(timeout=10)

        logger.info(
            "PRODUCED  | request_id=%-36s | features=%s | topic=%s | partition=%d | offset=%d",
            request_id, features, record_metadata.topic,
            record_metadata.partition, record_metadata.offset
        )
        return request_id

    def send_batch(self, samples: list) -> list:
        """Send multiple requests and return list of request_ids."""
        request_ids = []
        for sample in samples:
            rid = self.send_request(
                features=sample["features"],
                expected_label=sample.get("expected", "unknown")
            )
            request_ids.append(rid)
            time.sleep(0.1)  # gentle rate limiting for demo clarity

        self.producer.flush()
        logger.info("Batch of %d messages flushed to topic '%s'", len(samples), REQUEST_TOPIC)
        return request_ids

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer closed")


# ──────────────────────────────────────────────────────────────────────────────
# Consumer: reads from ml-requests, calls FastAPI, logs to ml-predictions
# ──────────────────────────────────────────────────────────────────────────────
class MLPredictionConsumer:
    """
    Kafka Consumer that:
    1. Reads prediction requests from ml-requests topic
    2. Forwards each request to the FastAPI /predict endpoint
    3. Publishes the prediction result to ml-predictions topic
    4. Tracks latency and correctness metrics

    Consumer group: ml-prediction-workers
    Multiple instances of this consumer can run in parallel — Kafka will
    distribute topic partitions across them automatically.
    """

    def __init__(self, broker: str = KAFKA_BROKER):
        self.broker = broker
        self.consumer: Optional[KafkaConsumer] = None
        self.result_producer: Optional[KafkaProducer] = None
        self.metrics = {
            "processed": 0,
            "correct": 0,
            "errors": 0,
            "total_latency_ms": 0.0
        }

    def connect(self):
        # Consumer: reads prediction requests
        self.consumer = KafkaConsumer(
            REQUEST_TOPIC,
            bootstrap_servers=[self.broker],
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",       # replay from start if new group
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )

        # Result producer: publishes prediction results downstream
        self.result_producer = KafkaProducer(
            bootstrap_servers=[self.broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=1
        )

        logger.info(
            "Consumer connected | group=%s | topic=%s → API=%s → topic=%s",
            CONSUMER_GROUP, REQUEST_TOPIC, API_URL, PREDICTION_TOPIC
        )

    def _call_api(self, request_id: str, features: list) -> dict:
        """HTTP call to FastAPI /predict with timeout."""
        payload = {"features": features, "request_id": request_id}
        response = requests.post(API_URL, json=payload, timeout=5)
        response.raise_for_status()
        return response.json()

    def _publish_result(self, result: dict):
        """Publish prediction result to ml-predictions topic for downstream consumers."""
        self.result_producer.send(
            PREDICTION_TOPIC,
            key=result.get("request_id", str(uuid.uuid4())),
            value=result
        )

    def consume(self, max_messages: int = 0, timeout_seconds: int = 60):
        """
        Poll for messages and process them.

        Args:
            max_messages: Stop after N messages (0 = run indefinitely)
            timeout_seconds: How long to wait for new messages before exiting
        """
        logger.info(
            "Consumer polling... (max_messages=%s, timeout=%ds)",
            max_messages if max_messages > 0 else "∞", timeout_seconds
        )

        last_message_time = time.time()
        count = 0

        try:
            for message in self.consumer:
                request = message.value
                request_id = request.get("request_id", "unknown")
                features = request.get("features", [])
                expected = request.get("expected_label", "unknown")

                logger.info(
                    "CONSUMED  | offset=%-6d | request_id=%-36s | features=%s",
                    message.offset, request_id, features
                )

                t_start = time.perf_counter()

                try:
                    # Forward to FastAPI
                    prediction = self._call_api(request_id, features)
                    api_latency_ms = round((time.perf_counter() - t_start) * 1000, 3)

                    predicted_label = prediction.get("prediction_label", "?")
                    confidence = prediction.get("confidence", 0)
                    is_correct = (predicted_label == expected)

                    # Enrich result with Kafka metadata and correctness flag
                    result = {
                        **prediction,
                        "expected_label": expected,
                        "is_correct": is_correct,
                        "kafka_offset": message.offset,
                        "kafka_partition": message.partition,
                        "consumed_at": datetime.now(timezone.utc).isoformat(),
                        "end_to_end_latency_ms": api_latency_ms
                    }

                    self._publish_result(result)

                    status_icon = "✓" if is_correct else "✗"
                    logger.info(
                        "PREDICTED | %s | request_id=%-36s | pred=%-12s | expected=%-12s | "
                        "confidence=%.1f%% | latency=%.1fms",
                        status_icon, request_id, predicted_label, expected,
                        confidence * 100, api_latency_ms
                    )

                    self.metrics["processed"] += 1
                    self.metrics["correct"] += int(is_correct)
                    self.metrics["total_latency_ms"] += api_latency_ms

                except requests.RequestException as e:
                    logger.error("API call failed for %s: %s", request_id, e)
                    self.metrics["errors"] += 1
                except Exception as e:
                    logger.exception("Unexpected error processing %s: %s", request_id, e)
                    self.metrics["errors"] += 1

                count += 1
                last_message_time = time.time()

                if max_messages > 0 and count >= max_messages:
                    logger.info("Reached max_messages limit (%d). Stopping.", max_messages)
                    break

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        finally:
            self._print_summary()
            self.close()

    def _print_summary(self):
        """Print a performance summary after consuming a batch."""
        m = self.metrics
        if m["processed"] == 0:
            logger.info("No messages processed.")
            return

        accuracy = m["correct"] / m["processed"] * 100
        avg_latency = m["total_latency_ms"] / m["processed"]

        print("\n" + "═" * 60)
        print("  Kafka Consumer — Session Summary")
        print("═" * 60)
        print(f"  Messages processed : {m['processed']}")
        print(f"  Correct predictions: {m['correct']} ({accuracy:.1f}%)")
        print(f"  Errors             : {m['errors']}")
        print(f"  Avg API latency    : {avg_latency:.2f}ms")
        print(f"  Total latency      : {m['total_latency_ms']:.2f}ms")
        print("═" * 60 + "\n")

    def close(self):
        if self.consumer:
            self.consumer.close()
        if self.result_producer:
            self.result_producer.flush()
            self.result_producer.close()
        logger.info("Consumer closed")


# ──────────────────────────────────────────────────────────────────────────────
# CLI Entry Point
# ──────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Kafka ML Pipeline Client — Producer and Consumer modes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python kafka_client.py --mode producer --samples 10
  python kafka_client.py --mode consumer --max-messages 10
  python kafka_client.py --mode both --samples 10
        """
    )
    parser.add_argument(
        "--mode",
        choices=["producer", "consumer", "both"],
        default="producer",
        help="Operating mode (default: producer)"
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=10,
        help="Number of sample requests to send in producer mode (default: 10)"
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Consumer: stop after N messages (0 = run indefinitely)"
    )
    parser.add_argument(
        "--broker",
        default=KAFKA_BROKER,
        help=f"Kafka broker address (default: {KAFKA_BROKER})"
    )
    args = parser.parse_args()

    # Wait for Kafka broker
    if not wait_for_kafka(args.broker):
        logger.error("Cannot connect to Kafka broker. Exiting.")
        sys.exit(1)

    if args.mode in ("producer", "both"):
        print(f"\n{'─' * 60}")
        print(f"  PRODUCER MODE — Sending {args.samples} requests to '{REQUEST_TOPIC}'")
        print(f"{'─' * 60}\n")

        producer = MLRequestProducer(broker=args.broker)
        producer.connect()

        # Cycle through IRIS_SAMPLES to hit requested count
        samples_to_send = []
        for i in range(args.samples):
            samples_to_send.append(IRIS_SAMPLES[i % len(IRIS_SAMPLES)])

        producer.send_batch(samples_to_send)
        producer.close()

        print(f"\n✓ {args.samples} messages sent to Kafka topic '{REQUEST_TOPIC}'")

    if args.mode in ("consumer", "both"):
        if args.mode == "both":
            time.sleep(1)  # brief pause to let messages settle in broker

        print(f"\n{'─' * 60}")
        print(f"  CONSUMER MODE — Listening on '{REQUEST_TOPIC}'")
        print(f"{'─' * 60}\n")

        consumer = MLPredictionConsumer(broker=args.broker)
        consumer.connect()
        consumer.consume(max_messages=args.max_messages if args.max_messages > 0 else args.samples)


if __name__ == "__main__":
    main()
