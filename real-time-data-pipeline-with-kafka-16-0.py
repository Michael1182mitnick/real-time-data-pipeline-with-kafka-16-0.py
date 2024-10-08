# Consumer (Process Data)
# Set up a real-time data pipeline using Apache Kafka to ingest, process, and analyze streaming data from multiple sources.

from kafka import KafkaConsumer
import json

# Create a Kafka consumer
consumer = KafkaConsumer(
    'streaming-data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def consume_data():
    print("Starting consumer...")
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        print(f"Received data: {data}")

        # You can add processing logic here (e.g., filter, aggregate, store in DB)
        # For example, log high-temperature events:
        if data['temperature'] > 80:
            print(f"ALERT: High temperature detected! {data}")


if __name__ == "__main__":
    consume_data()
