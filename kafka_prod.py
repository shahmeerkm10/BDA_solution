from kafka import KafkaProducer
import pandas as pd
import json

# Kafka configuration
topic_name = "agriculture_topic"
bootstrap_servers = "localhost:9092"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# CSV file path
csv_file_path = "E:/shahmeerdockerwd/shahmeerhadoop/agricultural_production_census_2007.csv"

# Chunk size for streaming
chunk_size = 1000  # Adjust based on your memory capacity

# Stream data to Kafka
try:
    for chunk in pd.read_csv(csv_file_path, chunksize=chunk_size):
        records = chunk.to_dict(orient="records")  # Convert to list of dicts
        for record in records:
            producer.send(topic_name, value=record)
        print(f"Sent {len(records)} records to Kafka")
except Exception as e:
    print(f"Error: {e}")
finally:
    producer.close()
