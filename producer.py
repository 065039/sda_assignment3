"""
producer.py
Streams the synthetic transaction data (sample_transactions.json) to a Kafka topic,
one message every 2-5 seconds, matching the "~1 event every 2-5 seconds per simulated
customer" cadence defined for the Transaction Generator source in Assignment 1.

Run:
    pip install kafka-python
    python producer.py
Requires a Kafka broker reachable at KAFKA_BROKER (default localhost:9092).
"""

import json
import time
import random
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC = "transactions"
DATA_FILE = "sample_transactions.json"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

def load_rows(path):
    with open(path) as f:
        return json.load(f)

def to_payload(row):
    # Strip the ground-truth fraud label before publishing —
    # that field only exists for offline calibration, not the live stream.
    payload = {k: v for k, v in row.items() if k != "is_fraud"}
    return payload

def run():
    rows = load_rows(DATA_FILE)
    print(f"Loaded {len(rows)} transactions from {DATA_FILE}")
    print(f"Streaming to topic '{TOPIC}' on {KAFKA_BROKER} ...\n")

    sent = 0
    try:
        for row in rows:
            payload = to_payload(row)
            key = payload["customer_id"]
            future = producer.send(TOPIC, key=key, value=payload)
            metadata = future.get(timeout=10)  # blocks until broker ack, confirms delivery
            sent += 1
            print(
                f"[{sent}] Sent -> partition={metadata.partition} "
                f"offset={metadata.offset} key={key} "
                f"txn={payload['transaction_id']} amount={payload['amount']}"
            )
            time.sleep(random.uniform(2, 5))
    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        producer.flush()
        producer.close()
        print(f"\nDone. {sent} messages sent to '{TOPIC}'.")

if __name__ == "__main__":
    run()
