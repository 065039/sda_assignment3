"""
generate_data.py
Creates sample transaction data for the Fintech Fraud Detection project (Assignment 1 pipeline).
Fields match the "Synthetic Transaction Generator" source defined in Assignment 1:
transaction_id, amount, merchant_category, location, device_id, customer_id, timestamp
Adds is_fraud as a hidden ground-truth label (not sent to Kafka in real time - used later
for calibration/validation, same role the Kaggle dataset plays in Assignment 1).
"""

import json
import csv
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker("en_IN")
Faker.seed(42)
random.seed(42)

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "fuel", "dining", "travel",
    "utilities", "pharmacy", "entertainment", "apparel", "atm_withdrawal"
]
CITIES = ["Delhi", "Mumbai", "Bengaluru", "Hyderabad", "Chennai",
          "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow"]

NUM_CUSTOMERS = 60
NUM_ROWS = 500

customer_ids = [f"CUST{str(i).zfill(4)}" for i in range(1, NUM_CUSTOMERS + 1)]
device_map = {c: f"DEV{random.randint(1000,9999)}" for c in customer_ids}

def make_transaction(i, ts):
    customer = random.choice(customer_ids)
    # 3% of rows deliberately look "fraud-like": high amount + odd hour
    is_fraud = random.random() < 0.03
    amount = round(random.uniform(20000, 95000), 2) if is_fraud else round(random.uniform(50, 15000), 2)
    return {
        "transaction_id": f"TXN{100000 + i}",
        "amount": amount,
        "currency": "INR",
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "location": random.choice(CITIES),
        "device_id": device_map[customer],
        "customer_id": customer,
        "timestamp": ts.isoformat(),
        "is_fraud": is_fraud  # ground-truth label, kept out of the live Kafka payload
    }

start = datetime.now() - timedelta(hours=2)
rows = []
ts = start
for i in range(NUM_ROWS):
    ts = ts + timedelta(seconds=random.randint(2, 5))
    rows.append(make_transaction(i, ts))

# JSON (one array) — used by producer.py to stream row by row
with open("sample_transactions.json", "w") as f:
    json.dump(rows, f, indent=2)

# CSV — for reference / inspection in Excel
with open("sample_transactions.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Generated {len(rows)} rows -> sample_transactions.json / sample_transactions.csv")
print(f"Fraud-flagged rows: {sum(r['is_fraud'] for r in rows)} ({sum(r['is_fraud'] for r in rows)/len(rows)*100:.1f}%)")
print("Sample row:")
print(json.dumps(rows[0], indent=2))
