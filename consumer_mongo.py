"""
consumer_mongo.py
Assignment 3 — Dashboard for Analysis of Consumed Data
Streaming Data Analytics

Consumes from Kafka topic 'transactions' (as published by producer.py)
and writes to MongoDB Atlas for use with MongoDB Atlas Charts.

Writes to two collections:
  1. transactions      — every raw event, as-is
  2. customer_profiles  — upserted running summary per customer
                          (last location, total spend, txn count, avg spend)

UPDATE MONGO_URI below with your own Atlas connection string before running.
"""

import json
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
MONGO_URI    = "mongodb+srv://usrname:pass@cluster1.hrxjzn5.mongodb.net/"
DB_NAME      = "fintech_fraud"
KAFKA_BROKER = "localhost:9092"
TOPIC        = "transactions"

# ---------------------------------------------------------------------------
# Connect
# ---------------------------------------------------------------------------
client   = MongoClient(MONGO_URI)
db       = client[DB_NAME]
txns     = db["transactions"]
profiles = db["customer_profiles"]

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    group_id="mongo-store-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print("MongoDB Storage Consumer started")
print(f"   Kafka  : {KAFKA_BROKER} -> {TOPIC}")
print(f"   MongoDB: {DB_NAME}.transactions | {DB_NAME}.customer_profiles")
print("-" * 60)

count = 0
for msg in consumer:
    txn = msg.value

    # 1. Append raw transaction, with server-side ingestion timestamp
    txns.insert_one({**txn, "ingested_at": datetime.utcnow()})

    # 2. Upsert customer_profiles — running totals + last known state
    amount = float(txn["amount"])
    profiles.update_one(
        {"customer_id": txn["customer_id"]},
        {
            "$set": {
                "last_location":   txn["location"],
                "last_merchant_category": txn["merchant_category"],
                "last_device_id":  txn["device_id"],
                "last_timestamp":  txn["timestamp"],
            },
            "$inc": {
                "total_spend": amount,
                "txn_count":   1,
            },
        },
        upsert=True,
    )

    count += 1
    print(f"[{count:04d}] Stored {txn['transaction_id']}  "
          f"{txn['customer_id']:<10} {txn['location']:<12} "
          f"Rs {amount:>10,.2f}  ({txn['merchant_category']})")

    # Keep avg_spend in sync after every write (simple, good enough at this volume)
    prof = profiles.find_one({"customer_id": txn["customer_id"]})
    profiles.update_one(
        {"customer_id": txn["customer_id"]},
        {"$set": {"avg_spend": round(prof["total_spend"] / prof["txn_count"], 2)}},
    )
