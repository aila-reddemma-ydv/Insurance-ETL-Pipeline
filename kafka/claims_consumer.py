import csv
import json
import os

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP_SERVER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVER",
    "localhost:9092"
)

KAFKA_CLAIMS_TOPIC = os.getenv(
    "KAFKA_CLAIMS_TOPIC",
    "insurance-claims"
)

RAW_CLAIMS_OUTPUT_PATH = os.getenv(
    "RAW_CLAIMS_OUTPUT_PATH"
)

CLAIMS_COLUMNS = [
    "claim_id",
    "customer_id",
    "claim_amount",
    "claim_date",
    "claim_status",
    "branch",
]


def create_consumer():
    return KafkaConsumer(
        KAFKA_CLAIMS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        group_id="insurance-claims-consumer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        consumer_timeout_ms=10000
    )


def consume_claims(consumer):
    if not RAW_CLAIMS_OUTPUT_PATH:
        raise ValueError(
            "RAW_CLAIMS_OUTPUT_PATH is missing in .env"
        )

    os.makedirs(
        os.path.dirname(RAW_CLAIMS_OUTPUT_PATH),
        exist_ok=True
    )

    count = 0

    with open(
        RAW_CLAIMS_OUTPUT_PATH,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=CLAIMS_COLUMNS
        )

        writer.writeheader()

        for message in consumer:
            writer.writerow(message.value)

            count += 1

    consumer.commit()

    print(f"Finished consuming {count} claim records")
    print(
        f"Raw claims written to: "
        f"{RAW_CLAIMS_OUTPUT_PATH}"
    )


def main():
    consumer = create_consumer()

    try:
        consume_claims(consumer)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
