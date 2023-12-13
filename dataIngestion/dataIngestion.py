import os
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Environment variables for configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka-1:9092')
TOPIC_VIEWS = os.getenv('TOPIC_VIEWS', 'views')
TOPIC_PURCHASES = os.getenv('TOPIC_PURCHASES', 'purchases')
DATASET_FILE = os.getenv('DATASET_FILE', '/usr/src/app/2019-Oct.csv')

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to send record to Kafka
def send_record(topic, record):
    try:
        producer.send(topic, record)
        producer.flush()
    except KafkaError as e:
        print(f"Failed to send record to Kafka: {e}")

# Reading and sending data
try:
    with open(DATASET_FILE, 'r') as file:
        for line_number, line in enumerate(file):
            if line_number == 0:
                # Skip the header line
                continue
            record = line.strip().split(',')
            event_type = record[1]  # Assuming second column is event_type
            if event_type == 'view':
                send_record(TOPIC_VIEWS, record)
            elif event_type == 'purchase':
                send_record(TOPIC_PURCHASES, record)
except FileNotFoundError:
    print(f"File not found: {DATASET_FILE}")
except Exception as e:
    print(f"Error processing file: {e}")

# Close the producer
producer.close()
print('Data ingestion completed successfully.')