import time
import json
import csv
from kafka import KafkaProducer

# Initialize the Kafka Producer
# Assumes Kafka is running locally on default port 9092
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

TOPIC_NAME = 'ecommerce_reviews'
DATA_FILE = 'data/dummy_reviews.csv'

print(f"Starting to stream data to Kafka topic: {TOPIC_NAME}...")

def stream_data():
    with open(DATA_FILE, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Send the row as a JSON payload to Kafka
            producer.send(TOPIC_NAME, value=row)
            print(f"Sent: {row['product_name']} - {row['review_text'][:30]}...")
            
            # Sleep to simulate real-time human reviews trickling in
            time.sleep(1)

if __name__ == "__main__":
    try:
        stream_data()
    except KeyboardInterrupt:
        print("Streaming stopped manually.")
    finally:
        producer.close()