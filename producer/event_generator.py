"""
StreamPulse Event Generator
----------------------------
Simulates a high-throughput event stream (e.g. IoT sensor readings / transaction
events) and publishes to a Kafka topic. Injects occasional anomalous spikes so
the downstream anomaly detector has something real to catch.

Run:
    python event_generator.py --rate 1000 --topic events
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

DEVICE_IDS = [f"device_{i:03d}" for i in range(50)]


def make_event(device_id: str, force_anomaly: bool = False) -> dict:
    """Generate a single event. Baseline value is a noisy sine-ish signal;
    anomalies are injected as large spikes."""
    base_value = 50 + random.gauss(0, 5)
    if force_anomaly:
        base_value += random.choice([1, -1]) * random.uniform(40, 90)

    return {
        "event_id": str(uuid.uuid4()),
        "device_id": device_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "value": round(base_value, 2),
        "event_type": random.choice(["reading", "reading", "reading", "heartbeat"]),
    }


def run(bootstrap_servers: str, topic: str, events_per_second: int, anomaly_rate: float):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=5,
        batch_size=32768,
    )

    print(f"[producer] streaming to topic='{topic}' @ ~{events_per_second} events/sec")
    interval = 1.0 / events_per_second if events_per_second > 0 else 0

    sent = 0
    start = time.time()
    try:
        while True:
            device_id = random.choice(DEVICE_IDS)
            is_anomaly = random.random() < anomaly_rate
            event = make_event(device_id, force_anomaly=is_anomaly)
            producer.send(topic, key=device_id, value=event)
            sent += 1

            if sent % 5000 == 0:
                elapsed = time.time() - start
                print(f"[producer] sent={sent} elapsed={elapsed:.1f}s rate={sent/elapsed:.0f}/s")

            if interval:
                time.sleep(interval)
    except KeyboardInterrupt:
        print("[producer] shutting down")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="events")
    parser.add_argument("--rate", type=int, default=1000, help="target events per second")
    parser.add_argument("--anomaly-rate", type=float, default=0.01, help="fraction of events forced anomalous")
    args = parser.parse_args()

    run(args.bootstrap_servers, args.topic, args.rate, args.anomaly_rate)
