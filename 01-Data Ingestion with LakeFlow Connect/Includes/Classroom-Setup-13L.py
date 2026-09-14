# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

import json
import base64
import random
from datetime import datetime
from pathlib import Path


# Function to create one event with nested array
def create_event(i: int) -> dict:
    value_dict = {
        "user_id": f"user_{random.randint(1000, 9999)}",
        "event_type": random.choice(["click", "purchase", "view"]),
        "event_timestamp": datetime.now().isoformat(),
        "items": [
            {
                "item_id": f"item_{random.randint(100, 999)}",
                "quantity": random.randint(1, 5),
                "price_usd": round(random.uniform(10.0, 100.0), 2)
            }
            for _ in range(random.randint(1, 3))  # 1 to 3 items per event
        ]
    }

    # Encode value as JSON string, then base64
    encoded_value = base64.b64encode(json.dumps(value_dict).encode()).decode()

    return {
        "key": f"event_{i}",
        "timestamp": datetime.now().timestamp(),
        "value": encoded_value
    }

# Create 100 records
events = [create_event(i) for i in range(100)]


# Output file path
output_path = Path(f'/Volumes/{my_catalog}/data_ingestion/landing_folder/json_demo_files/lab_kafka_events.json')

# Write to file
with open(output_path, "w") as f:
    for event in events:
        json.dump(event, f)
        f.write("\n")

print(f"Generated file: {output_path.resolve()}")

# COMMAND ----------

import json
import base64
import random
from datetime import datetime
from pathlib import Path

def create_valid_event(i: int) -> dict:
    value_payload = {
        "user_id": f"user_{random.randint(1000, 9999)}",
        "event_type": random.choice(["click", "purchase", "view"]),
        "event_timestamp": datetime.now().isoformat(),
        "items": [
            {
                "item_id": f"item_{random.randint(100, 999)}",
                "quantity": random.randint(1, 5),
                "price_usd": round(random.uniform(10.0, 100.0), 2)
            }
            for _ in range(random.randint(1, 3))
        ]
    }
    value_json = json.dumps(value_payload)
    value_base64 = base64.b64encode(value_json.encode('utf-8')).decode('utf-8')

    return {
        "key": f"event_{i}",
        "timestamp": datetime.now().timestamp(),  # correct timestamp
        "value": value_base64
    }

def create_malformed_event(i: int) -> dict:
    value_payload = {
        "user_id": f"user_{random.randint(1000, 9999)}",
        "event_type": "malformed_event",
        "event_timestamp": datetime.now().isoformat(),
        "items": []
    }
    value_json = json.dumps(value_payload)
    value_base64 = base64.b64encode(value_json.encode('utf-8')).decode('utf-8')

    return {
        "key": f"event_{i}",
        "timestamp": "ERROR",  # <-- invalid timestamp
        "value": value_base64
    }

# Create 99 valid events
events = [create_valid_event(i) for i in range(99)]

# Add 1 malformed event at a random position
malformed_event = create_malformed_event(99)
insert_position = random.randint(0, 99)
events.insert(insert_position, malformed_event)


# Output file path
output_path = Path(f'/Volumes/{my_catalog}/data_ingestion/landing_folder/json_demo_files/lab_kafka_events.json')

with open(output_path, "w") as f:
    for event in events:
        json.dump(event, f)
        f.write("\n")

print(f"File written with 1 malformed 'timestamp' at position {insert_position}: {output_path.resolve()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create solution table
# MAGIC CREATE OR REPLACE TABLE lab13_lab_kafka_events_raw
# MAGIC AS
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   cast(unbase64(value) as STRING) as decoded_value
# MAGIC FROM read_files(
# MAGIC       '/Volumes/' || my_catalog || '/data_ingestion/landing_folder/json_demo_files/lab_kafka_events.json',
# MAGIC         format => "json", 
# MAGIC         schema => '''
# MAGIC           key STRING, 
# MAGIC           timestamp DOUBLE, 
# MAGIC           value STRING
# MAGIC         ''',
# MAGIC         rescueddatacolumn => '_rescued_data'
# MAGIC       );
# MAGIC
# MAGIC
# MAGIC -- Create the solution table with the correct data types
# MAGIC DROP TABLE IF EXISTS lab13_lab_kafka_events_flattened_solution;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE lab13_lab_kafka_events_flattened_solution
# MAGIC AS
# MAGIC SELECT 
# MAGIC   key,
# MAGIC   timestamp,
# MAGIC   decoded_value:user_id,
# MAGIC   decoded_value:event_type,
# MAGIC   cast(decoded_value:event_timestamp AS TIMESTAMP),
# MAGIC   from_json(decoded_value:items,'ARRAY<STRUCT<item_id: STRING, price_usd: DOUBLE, quantity: BIGINT>>') AS items
# MAGIC FROM lab13_lab_kafka_events_raw;
# MAGIC
# MAGIC
# MAGIC DROP TABLE IF EXISTS lab13_lab_kafka_events_raw;