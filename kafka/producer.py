import csv
import json
import os
import time

from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVER",
    "localhost:9092"
)

KAFKA_POLICY_TOPIC = os.getenv(
    "KAFKA_POLICY_TOPIC",
    "insurance-policy"
)

POLICY_FILE = os.getenv(
    "POLICY_INPUT_PATH",
    "data/train.csv"
)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )


def send_policy_records(producer):
    print(f"Reading policy file: {POLICY_FILE}")
    print(f"Sending records to topic: {KAFKA_POLICY_TOPIC}")

    with open(POLICY_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for count, row in enumerate(reader, start=1):
            producer.send(
                KAFKA_POLICY_TOPIC,
                value=row
            )

            if count % 10000 == 0:
                producer.flush()
                print(f"Sent {count} records")

            # Small delay prevents overwhelming the local broker
            if count % 1000 == 0:
                time.sleep(0.1)

    producer.flush()

    print(f"Finished sending {count} policy records")


def main():
    producer = create_producer()

    try:
        send_policy_records(producer)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
