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

KAFKA_POLICY_TOPIC = os.getenv(
    "KAFKA_POLICY_TOPIC",
    "insurance-policy"
)

RAW_POLICY_OUTPUT_PATH = os.getenv(
    "RAW_POLICY_OUTPUT_PATH"
)


POLICY_COLUMNS = [
    "id",
    "Gender",
    "Age",
    "Driving_License",
    "Region_Code",
    "Previously_Insured",
    "Vehicle_Age",
    "Vehicle_Damage",
    "Annual_Premium",
    "Policy_Sales_Channel",
    "Vintage",
    "Response",
]

def create_consumer():
    return KafkaConsumer(
        KAFKA_POLICY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        group_id="insurance-policy-consumer-group",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        consumer_timeout_ms=10000
    )

def consume_policy_records(consumer):
    output_directory = os.path.dirname(RAW_POLICY_OUTPUT_PATH)

    os.makedirs(
        output_directory,
        exist_ok=True
    )

    record_count = 0

    with open(
        RAW_POLICY_OUTPUT_PATH,
        mode="w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=POLICY_COLUMNS,
            extrasaction="ignore"
        )

        writer.writeheader()

        try:
            for message in consumer:
                writer.writerow(message.value)

                record_count += 1

                if record_count % 10000 == 0:
                    print(
                        f"Consumed {record_count} records"
                    )

        except StopIteration:
            pass

    consumer.commit()

    print(
        f"Finished consuming {record_count} records"
    )

    print(
        f"Raw policy data written to: "
        f"{RAW_POLICY_OUTPUT_PATH}"
    )


def main():
    if not RAW_POLICY_OUTPUT_PATH:
        raise ValueError(
            "RAW_POLICY_OUTPUT_PATH is missing in .env"
        )

    consumer = create_consumer()

    try:
        consume_policy_records(consumer)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
