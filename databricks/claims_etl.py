import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum

load_dotenv()


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Insurance Claims ETL")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )


def read_csv(spark, path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"file://{path}")
    )


def read_claims_data(spark):
    paths = [
        os.getenv("CLAIMS_CHENNAI_PATH"),
        os.getenv("CLAIMS_BANGALORE_PATH"),
        os.getenv("CLAIMS_HYDERABAD_PATH"),
        os.getenv("CLAIMS_MUMBAI_PATH"),
    ]

    if any(path is None for path in paths):
        raise ValueError("One or more claims paths are missing in .env")

    dataframes = [read_csv(spark, path) for path in paths]

    claims_df = dataframes[0]

    for df in dataframes[1:]:
        claims_df = claims_df.unionByName(df)

    return claims_df


def validate_claim_columns(df):
    required_columns = [
        "claim_id",
        "customer_id",
        "claim_amount",
        "claim_date",
        "claim_status",
        "branch",
    ]

    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required claim columns: {missing_columns}"
        )

    return df


def clean_claims_data(df):
    return (
        df.dropDuplicates(["claim_id"])
        .dropna(
            subset=[
                "claim_id",
                "customer_id",
                "claim_amount",
                "claim_date",
                "claim_status",
                "branch",
            ]
        )
    )


def validate_customer_ids(claims_df, policy_df):
    valid_customers = (
        policy_df
        .select("id")
        .dropDuplicates()
    )

    return (
        claims_df
        .join(
            valid_customers,
            claims_df.customer_id == valid_customers.id,
            "inner",
        )
        .drop("id")
    )


def aggregate_claims(valid_claims):
    return (
        valid_claims
        .groupBy("customer_id")
        .agg(
            spark_sum("claim_amount").alias("Total_Claim_Amount")
        )
    )


def main():
    spark = create_spark_session()

    policy_path = os.getenv("POLICY_INPUT_PATH")
    output_path = os.getenv("CLAIMS_OUTPUT_PATH")

    if not policy_path:
        raise ValueError("POLICY_INPUT_PATH is missing in .env")

    if not output_path:
        raise ValueError("CLAIMS_OUTPUT_PATH is missing in .env")

    try:
        print("Reading policy data...")

        policy_df = read_csv(
            spark,
            policy_path
        )

        print("Reading claims data...")

        claims_df = read_claims_data(spark)

        print("Validating claim columns...")

        claims_df = validate_claim_columns(claims_df)

        print("Cleaning claims...")

        claims_df = clean_claims_data(claims_df)

        print("Validating customer IDs...")

        valid_claims = validate_customer_ids(
            claims_df,
            policy_df
        )

        print("Aggregating claims...")

        claims_summary = aggregate_claims(valid_claims)

        print(
            "Total claims after duplicate/null cleaning:",
            claims_df.count()
        )

        print(
            "Valid claims after customer validation:",
            valid_claims.count()
        )

        print(
            "Customers with claims:",
            claims_summary.count()
        )

        claims_summary.show(10, truncate=False)

        print("Writing claims output...")

        (
            claims_summary.write
            .mode("overwrite")
            .option("header", True)
            .csv(f"file://{output_path}")
        )

        print(f"Claims output written to: {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
