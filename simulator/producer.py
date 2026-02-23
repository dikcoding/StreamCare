import os
import json
import time
import uuid
import random
from faker import Faker
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

# ----------------------------------------------------------
# Load Environment Variables
# ----------------------------------------------------------
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "skincare-events")
USER_COUNT = int(os.getenv("USER_COUNT", 20))
EVENT_INTERVAL_SECONDS = int(os.getenv("EVENT_INTERVAL_SECONDS", 1))

fake = Faker()

# ----------------------------------------------------------
# Kafka Producer Setup
# ----------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ----------------------------------------------------------
# SKINCARE PRODUCT LIST
# ----------------------------------------------------------
product_catalog = [
    {"product_name": "Skintific 5X Ceramide Moisturizer", "brand": "Skintific", "price": 89},
    {"product_name": "Wardah Hydrating Aloe Vera Gel", "brand": "Wardah", "price": 35},
    {"product_name": "Glad2Glow Niacinamide Serum", "brand": "Glad2Glow", "price": 45},
    {"product_name": "Hanasui Propolis Serum", "brand": "Hanasui", "price": 28},
    {"product_name": "Maybelline Fit Me Foundation", "brand": "Maybelline NY", "price": 120},
    {"product_name": "MS Glow Whitening Day Cream", "brand": "MS Glow", "price": 75},
    {"product_name": "Somethinc AHA BHA Peeling Solution", "brand": "Somethinc", "price": 99},
    {"product_name": "Skin1004 Madagascar Centella Ampoule", "brand": "Skin1004", "price": 150},
    {"product_name": "Make Over Powerstay Matte Foundation", "brand": "Make Over", "price": 145},
    {"product_name": "Garnier Micellar Water Pink", "brand": "Garnier", "price": 32}
]

# generate stable product_id
for p in product_catalog:
    key = f"{p['product_name']}::{p['brand']}"
    p["product_id"] = str(uuid.uuid5(uuid.NAMESPACE_DNS, key))

# ----------------------------------------------------------
# Metadata
# ----------------------------------------------------------
payment_methods = ["credit_card", "debit_card", "gopay", "ovo", "dana", "shopee_pay"]
devices = ["mobile", "desktop", "web"]
countries = ["ID", "SG", "MY", "TH", "PH"]

voucher_codes = [None, "NEWUSER10", "DISCOUNT5", "FLASHSALE", "BEAUTYDAY"]

user_ids = [str(uuid.uuid4()) for _ in range(USER_COUNT)]

# ----------------------------------------------------------
# Generate Event
# ----------------------------------------------------------
def generate_event():
    product = random.choice(product_catalog)
    qty = random.randint(1, 3)

    discount_rate = random.choice([0, 0.05, 0.10, 0.15])
    discount_amount = product["price"] * discount_rate

    shipping_cost = random.choice([0, 5, 7])

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(user_ids),
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "brand": product["brand"],
        "category": "skincare",
        "price": product["price"],
        "qty": qty,
        "discount_rate": discount_rate,
        "discount_amount": round(discount_amount, 2),
        "shipping_cost": shipping_cost,
        "voucher_code": random.choice(voucher_codes),
        "payment_method": random.choice(payment_methods),
        "device_type": random.choice(devices),
        "country": random.choice(countries),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

# ----------------------------------------------------------
# Main Loop
# ----------------------------------------------------------
if __name__ == "__main__":
    print("Starting Skincare Event Producer...")
    print(f"Simulating {USER_COUNT} unique users...")
    print(f"Catalog contains {len(product_catalog)} skincare products:\n")

    for p in product_catalog:
        print(f"- {p['product_name']} ({p['brand']}) — product_id={p['product_id']}")

    print("\nProducing streaming skincare events...\n")

    while True:
        event = generate_event()
        producer.send(KAFKA_TOPIC, event)
        print(f"[EVENT] {event['brand']} | {event['product_name']} | x{event['qty']} | {event['country']}")
        time.sleep(EVENT_INTERVAL_SECONDS)