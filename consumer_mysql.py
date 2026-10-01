"""
consumer_mysql.py
Assignment 3 — Dashboard for Analysis of Consumed Data
Streaming Data Analytics

Consumes from Kafka topic 'transactions' (as published by producer.py)
and writes to a local MySQL database, for use with Grafana.

Run:
    pip install mysql-connector-python kafka-python
    python consumer_mysql.py
Requires MySQL running locally (default root/root — change via env vars below).
"""

import json
import os
from datetime import datetime
from decimal import Decimal

import mysql.connector
from kafka import KafkaConsumer

MYSQL_HOST     = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
DB_NAME        = "fintech_fraud"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC        = "transactions"
GROUP_ID     = "mysql-store-group"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id    VARCHAR(20) PRIMARY KEY,
    customer_id       VARCHAR(20),
    device_id         VARCHAR(20),
    amount            DECIMAL(12,2),
    currency          VARCHAR(10),
    merchant_category VARCHAR(50),
    location          VARCHAR(50),
    `timestamp`       DATETIME,
    ingested_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

UPSERT_SQL = """
INSERT INTO transactions (
    transaction_id, customer_id, device_id, amount, currency,
    merchant_category, location, `timestamp`
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    amount = VALUES(amount),
    merchant_category = VALUES(merchant_category),
    location = VALUES(location)
"""


def row_values(txn):
    return (
        txn["transaction_id"],
        txn["customer_id"],
        txn["device_id"],
        Decimal(str(txn["amount"])),
        txn["currency"],
        txn["merchant_category"],
        txn["location"],
        datetime.fromisoformat(txn["timestamp"]),
    )


db = mysql.connector.connect(
    host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASSWORD
)
cursor = db.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
cursor.execute(f"USE {DB_NAME}")
cursor.execute(CREATE_TABLE_SQL)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=GROUP_ID,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print("MySQL Storage Consumer started")
print(f"   Kafka: {TOPIC} ({GROUP_ID})")
print(f"   MySQL: {DB_NAME}.transactions")
print("-" * 60)

count = 0
try:
    for message in consumer:
        txn = message.value
        try:
            cursor.execute(UPSERT_SQL, row_values(txn))
            db.commit()
            consumer.commit()
        except mysql.connector.Error:
            db.rollback()
            raise

        count += 1
        print(f"[{count:04d}] Saved {txn['transaction_id']}  "
              f"{txn['customer_id']:<10} Rs {Decimal(str(txn['amount'])):>10,.2f}  "
              f"({txn['merchant_category']})")
except KeyboardInterrupt:
    print(f"\nStopped after saving {count} transactions.")
finally:
    consumer.close()
    cursor.close()
    db.close()
