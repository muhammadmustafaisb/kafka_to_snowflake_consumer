from kafka import KafkaConsumer
import json
import snowflake.connector
from datetime import datetime
import hashlib

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'orders'

# Snowflake configuration
SNOWFLAKE_ACCOUNT = 'xxxxxxxxx'
SNOWFLAKE_USER = 'xxxxxxxxx'
SNOWFLAKE_PASSWORD = 'xxxxxxxxxx'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'E_COMMERCE'
SNOWFLAKE_SCHEMA = 'E_COMMERCE'

# Initialize Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA
)
cursor = conn.cursor()

# Insert helper function to handle upserts
def upsert_dimension(table, key_column, key_value, columns, values):
    placeholders = ', '.join(['%s'] * len(values))
    column_list = ', '.join(columns)
    sql = f"""
    MERGE INTO {table} t
    USING (SELECT %s AS {key_column}) s
    ON t.{key_column} = s.{key_column}
    WHEN NOT MATCHED THEN
      INSERT ({key_column}, {column_list}) VALUES (%s, {placeholders});
    """
    cursor.execute(sql, [key_value, key_value] + list(values))

# Start consuming messages from Kafka
print("Consuming Kafka messages and inserting into Snowflake...")

for message in consumer:
    order = message.value
    print(f"Processing order: {order['order_id']}")

    # Parse fields
    order_id = order["order_id"]
    order_ts = datetime.fromisoformat(order["order_timestamp"].replace("Z", ""))
    customer_id = order["customer_id"]
    customer_name = order["customer_name"]
    customer_region = order["customer_region"]
    product_id = order["product_id"]
    product_name = order["product_name"]
    category = order["category"]
    price = float(order["price"])
    quantity = int(order["quantity"])
    total_amount = price * quantity
    date_id = order_ts.date()

    # --- Upsert into dimension tables ---
    upsert_dimension("DIM_CUSTOMER", "CUSTOMER_ID", customer_id,
                     ["CUSTOMER_NAME", "CUSTOMER_REGION"],
                     [customer_name, customer_region])

    upsert_dimension("DIM_PRODUCT", "PRODUCT_ID", product_id,
                     ["PRODUCT_NAME", "CATEGORY"],
                     [product_name, category])

    upsert_dimension("DIM_DATE", "DATE_ID", date_id,
                     ["DAY", "MONTH", "YEAR", "WEEKDAY"],
                     [order_ts.day, order_ts.month, order_ts.year, order_ts.strftime("%A")])

    # --- Insert into fact table (NOW WITH REGION) ---
    sql_fact = """
    INSERT INTO FACT_ORDERS (
        ORDER_ID, ORDER_TIMESTAMP, CUSTOMER_ID, PRODUCT_ID, DATE_ID,
        PRICE, QUANTITY, TOTAL_AMOUNT, REGION
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.execute(sql_fact, (
        order_id, order_ts, customer_id, product_id, date_id,
        price, quantity, total_amount, customer_region  # REGION added
    ))

    conn.commit()
