from kafka import KafkaProducer
import json
import time
import random
import uuid
from datetime import datetime, timedelta

# Kafka config
KAFKA_BROKER = 'localhost:9092'  # Change to your broker address if needed
TOPIC_NAME = 'orders'

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Sample data
customers = [
    {"customer_id": "C987", "customer_name": "Ali Raza", "customer_region": "Punjab"},
    {"customer_id": "C987", "customer_name": "Ali Raza", "customer_region": random.choice(["Punjab", "Sindh"])},
    {"customer_id": "C124", "customer_name": "Emma Stone", "customer_region": "Sindh"},
    {"customer_id": "C432", "customer_name": "John Doe", "customer_region": "KPK"}
]

products = [
    {"product_id": "P123", "product_name": "Wireless Mouse", "category": "Accessories", "price": 2500},
    {"product_id": "P456", "product_name": "Keyboard", "category": "Accessories", "price": 3000},
    {"product_id": "P789", "product_name": "Webcam", "category": "Electronics", "price": 7500}
]

# Generate and send 100 mock orders
for i in range(100):
    customer = random.choice(customers)
    product = random.choice(products)
    quantity = random.randint(1, 5)

    order = {
        "order_id": f"O{str(uuid.uuid4())[:8]}",
        "order_timestamp": (datetime.utcnow() - timedelta(minutes=i)).isoformat() + "Z",
        "customer_id": customer["customer_id"],
        "customer_name": customer["customer_name"],
        "customer_region": customer["customer_region"],
        "product_id": product["product_id"],
        "product_name": product["product_name"],
        "category": product["category"],
        "price": product["price"],
        "quantity": quantity
    }

    print(f"Sending order {i+1}: {order}")
    producer.send(TOPIC_NAME, order)
    time.sleep(1)

producer.flush()
producer.close()
