import csv
import json
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

KAFKA_BOOTSTRAP_SERVER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVER",
    "localhost:9092"
)

KAFKA_CLAIMS_TOPIC = os.getenv(
    "KAFKA_CLAIMS_TOPIC",
    "insurance-claims"
)

CLAIMS_FILES = [
    os.getenv("CLAIMS_CHENNAI_PATH"),
    os.getenv("CLAIMS_BANGALORE_PATH"),
    os.getenv("CLAIMS_HYDERABAD_PATH"),
    os.getenv("CLAIMS_MUMBAI_PATH"),
]


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )


def send_claims(producer):
    total_records = 0

    for file_path in CLAIMS_FILES:
        if not file_path:
            raise ValueError("A claims path is missing in .env")

        print(f"Reading: {file_path}")

        with open(file_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            for row in reader:
                producer.send(
                    KAFKA_CLAIMS_TOPIC,
                    value=row
                )

                total_records += 1

    producer.flush()

    print(f"Finished sending {total_records} claim records")


def main():
    producer = create_producer()

    try:
        send_claims(producer)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
