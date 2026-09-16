import csv
import glob
import os

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE", "insurance_db"),
    "connection_timeout": 10
}


def connect_database():
    print("Connecting to MySQL...", flush=True)

    connection = mysql.connector.connect(**DB_CONFIG)

    print("Connected to MySQL", flush=True)

    return connection


def load_premium(cursor):
    files = glob.glob(
        "insurance-data/processed/premium/part-*.csv"
    )

    count = 0

    for file_path in files:
        with open(
            file_path,
            "r",
            encoding="utf-8"
        ) as file:

            reader = csv.DictReader(file)

            for row in reader:
                cursor.execute(
                    """
                    INSERT IGNORE  INTO premium
                    (
                        id,
                        Base_Premium,
                        Final_Premium,
                        Premium_Status
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        int(row["id"]),
                        float(row["Base_Premium"]),
                        float(row["Final_Premium"]),
                        row["Premium_Status"]
                    )
                )

                count += 1

    return count


def load_claims(cursor):
    files = glob.glob(
        "insurance-data/processed/claims/part-*.csv"
    )

    count = 0

    for file_path in files:
        with open(
            file_path,
            "r",
            encoding="utf-8"
        ) as file:

            reader = csv.DictReader(file)

            for row in reader:
                cursor.execute(
                    """
                    INSERT IGNORE INTO claims
                    (
                        customer_id,
                        Total_Claim_Amount
                    )
                    VALUES (%s, %s)
                    """,
                    (
                        int(row["customer_id"]),
                        float(row["Total_Claim_Amount"])
                    )
                )

                count += 1

    return count


def load_risk(cursor):
    files = glob.glob(
        "insurance-data/processed/risk/part-*.csv"
    )

    count = 0

    for file_path in files:
        with open(
            file_path,
            "r",
            encoding="utf-8"
        ) as file:

            reader = csv.DictReader(file)

            for row in reader:
                cursor.execute(
                    """
                    INSERT IGNORE INTO risk
                    (
                        id,
                        Risk_Score,
                        Risk_Level
                    )
                    VALUES (%s, %s, %s)
                    """,
                    (
                        int(row["id"]),
                        int(row["Risk_Score"]),
                        row["Risk_Level"]
                    )
                )

                count += 1

    return count


def main():
    print("Starting database loader...", flush=True)

    print("Connecting to MySQL...", flush=True)
    connection = connect_database()
    print("Connected to MySQL", flush=True)

    cursor = connection.cursor()

    try:
        print("Loading premium...", flush=True)
        premium_count = load_premium(cursor)
        connection.commit()
        print(
            f"Premium records inserted: {premium_count}",
            flush=True
        )

        print("Loading claims...", flush=True)
        claims_count = load_claims(cursor)
        connection.commit()
        print(
            f"Claims records inserted: {claims_count}",
            flush=True
        )

        print("Loading risk...", flush=True)
        risk_count = load_risk(cursor)
        connection.commit()
        print(
            f"Risk records inserted: {risk_count}",
            flush=True
        )

        print("Database loading completed.", flush=True)

    except Exception as error:
        connection.rollback()
        print(f"ERROR: {error}", flush=True)
        raise

    finally:
        cursor.close()
        connection.close()
        print("Database connection closed.", flush=True)

if __name__ == "__main__":
    main()
