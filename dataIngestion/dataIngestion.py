import json
import os
import sys
import pandas as pd
from kafka.errors import KafkaError
from kafka import KafkaProducer
import argparse

# Kafka configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka-1:9092')
TOPIC_VIEWS = os.getenv('TOPIC_VIEWS', 'views')
TOPIC_PURCHASES = os.getenv('TOPIC_PURCHASES', 'purchases')
DATASET_FILE = os.getenv('DATASET_FILE', '/usr/src/app/2019-Oct.csv')

parser = argparse.ArgumentParser(description='Data Ingestion Script')
parser.add_argument('--instance_id', type=int, required=True, help='Instance ID')
parser.add_argument('--total_instances', type=int, required=True, help='Total number of instances')

# Parse arguments
args = parser.parse_args()

# Access arguments
instance_id = args.instance_id
total_instances = args.total_instances
print(f"instance_id: {instance_id}")
print(f"total_instances: {total_instances}")


def send_record(topic, record):
    try:
        producer.send(topic, value=record)
    except KafkaError as e:
        print(f"Failed to send record to Kafka: {e}")


def split_and_process_data(file_path, instance_id, total_instances):
    try:
        df = pd.read_csv(file_path)
        total_rows = len(df)
        rows_per_instance = total_rows // total_instances
        start_row = rows_per_instance * instance_id
        end_row = start_row + rows_per_instance if instance_id < total_instances - 1 else total_rows
        print(f"Start row: {start_row}")
        print(f"End row: {end_row}")
        # Processing the assigned portion of the dataset
        for index, row in df.iloc[start_row:end_row].iterrows():
            processed_data = row.to_json()
            event_type = row[1]
            if event_type == 'view':
                send_record(TOPIC_VIEWS, processed_data)
            elif event_type == 'purchase':
                send_record(TOPIC_PURCHASES, processed_data)
        producer.flush()
    except Exception as e:
        print(f"Exception: {e}")

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# Call the data processing function
split_and_process_data(DATASET_FILE, instance_id, total_instances)
