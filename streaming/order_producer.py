"""
E-Commerce Real-Time Order & Review Event Producer
Streams synthetic order transactions and customer product reviews to Google Cloud Pub/Sub.
Zero-cost implementation: can stream to GCP Pub/Sub Free Tier or local emulator.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ and not os.path.exists(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]):
    os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "dataeng-505315")

TOPIC_ID = "ecom-order-events-topic"

# Sample Products & Product Categories
PRODUCTS = [
    {"id": "PROD-101", "name": "Wireless Noise-Canceling Headphones", "category": "Electronics", "price": 149.99},
    {"id": "PROD-102", "name": "Ergonomic Mechanical Keyboard", "category": "Electronics", "price": 89.50},
    {"id": "PROD-103", "name": "Organic Cold-Pressed Coffee Beans 1kg", "category": "Grocery", "price": 24.99},
    {"id": "PROD-104", "name": "Stainless Steel Thermal Water Bottle", "category": "Home & Kitchen", "price": 19.95},
    {"id": "PROD-105", "name": "Ultra-Wide Gaming Monitor 34-inch", "category": "Electronics", "price": 450.00},
    {"id": "PROD-106", "name": "Smart Fitness Tracker Watch", "category": "Wearables", "price": 79.99},
]

CITIES = ["New York, NY", "San Francisco, CA", "Chicago, IL", "Austin, TX", "Seattle, WA", "London, UK", "Tokyo, JP"]

SAMPLE_REVIEWS = [
    {"rating": 5, "text": "Absolutely amazing quality! Fast shipping and works beyond expectations."},
    {"rating": 4, "text": "Good value for money. Very sturdy build, though shipping took 3 days."},
    {"rating": 1, "text": "Terrible product! Stopped working after 2 hours. Demanding a full refund."},
    {"rating": 5, "text": "Exceeded all expectations! Fantastic design and premium materials."},
    {"rating": 2, "text": "Disappointed with the size and poor packaging. Customer support was slow."},
    {"rating": 3, "text": "Average experience. Nothing extraordinary but gets the job done."},
    {"rating": 1, "text": "Suspicious transaction flag: Received damaged item and wrong color."},
]

def generate_order_event():
    """Generates a single synthetic e-commerce order payload."""
    product = random.choice(PRODUCTS)
    review = random.choice(SAMPLE_REVIEWS)
    quantity = random.randint(1, 4)
    total_amount = round(product["price"] * quantity, 2)
    
    event = {
        "event_id": str(uuid.uuid4()),
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "unit_price": product["price"],
        "quantity": quantity,
        "total_amount": total_amount,
        "rating": review["rating"],
        "review_text": review["text"],
        "customer_city": random.choice(CITIES),
        "payment_method": random.choice(["CREDIT_CARD", "PAYPAL", "APPLE_PAY", "CRYPTO"]),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "ingestion_source": "streaming_producer_v1"
    }
    return event

def stream_events(max_events=50, delay_seconds=1.0):
    """Streams events to GCP Pub/Sub or prints locally if Pub/Sub is unreachable."""
    print(f"=== Starting E-Commerce Event Stream Producer ===")
    print(f"Target GCP Project: {PROJECT_ID}")
    print(f"Target Pub/Sub Topic: {TOPIC_ID}\n")

    publisher = None
    topic_path = None

    try:
        from google.cloud import pubsub_v1
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
        print("OK: Connected to Google Cloud Pub/Sub Publisher Client")
    except Exception as e:
        print(f"[INFO] GCP Pub/Sub client note: {e}")
        print("  (Events will be logged locally to stdout & local buffer file)")

    published_count = 0
    buffer_file = "streaming/local_event_buffer.jsonl"
    
    os.makedirs("streaming", exist_ok=True)
    with open(buffer_file, "a", encoding="utf-8") as f_out:
        for i in range(1, max_events + 1):
            event = generate_order_event()
            payload_bytes = json.dumps(event).encode("utf-8")
            
            # Write to local file buffer (guarantees local zero-cost testing)
            f_out.write(json.dumps(event) + "\n")
            f_out.flush()

            if publisher and topic_path:
                try:
                    future = publisher.publish(topic_path, payload_bytes, order_id=event["order_id"])
                    message_id = future.result(timeout=5)
                    print(f"[{i}/{max_events}] Published to Pub/Sub MsgID: {message_id} | Order: {event['order_id']} | ${event['total_amount']}")
                except Exception as pub_err:
                    print(f"[{i}/{max_events}] Saved to local buffer (Pub/Sub publish: {pub_err}) | Order: {event['order_id']}")
            else:
                print(f"[{i}/{max_events}] Streamed Event -> Order: {event['order_id']} | Customer: {event['customer_id']} | ${event['total_amount']} | Rating: {event['rating']} Stars")

            published_count += 1
            time.sleep(delay_seconds)

    print(f"\n[SUCCESS] Stream completed. Total events produced: {published_count}")
    print(f"  Local buffer location: {buffer_file}")

if __name__ == "__main__":
    stream_events(max_events=30, delay_seconds=0.1)

