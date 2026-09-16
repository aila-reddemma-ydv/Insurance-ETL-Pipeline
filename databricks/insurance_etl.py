import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round as spark_round

load_dotenv()


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Insurance ETL Pipeline")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )


def read_policy_data(spark, input_path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )


def validate_policy_data(df):
    required_columns = [
        "id",
        "Age",
        "Previously_Insured",
        "Vehicle_Age",
        "Vehicle_Damage",
        "Annual_Premium"
    ]

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    return df


def clean_policy_data(df):
    # Remove duplicate policy records
    df = df.dropDuplicates(["id"])

    # Remove records with missing values in required fields
    df = df.dropna(subset=[
        "id",
        "Age",
        "Previously_Insured",
        "Vehicle_Age",
        "Vehicle_Damage",
        "Annual_Premium"
    ])

    return df


def calculate_premium(df):
    # Start with the base annual premium
    df = df.withColumn(
        "Base_Premium",
        col("Annual_Premium")
    )

    # Calculate the total adjustment percentage
    adjustment = (
        when(col("Age") > 60, 0.15).otherwise(0.0)
        + when(col("Vehicle_Age") == "> 2 Years", 0.10).otherwise(0.0)
        + when(col("Vehicle_Damage") == "Yes", 0.08).otherwise(0.0)
        + when(col("Previously_Insured") == 0, 0.05).otherwise(0.0)
    )

    df = df.withColumn(
        "Adjustment_Percentage",
        adjustment
    )

    # Calculate final premium
    df = df.withColumn(
        "Final_Premium",
        spark_round(
            col("Base_Premium") *
            (1 + col("Adjustment_Percentage")),
            2
        )
    )

    # Assign premium status
    df = df.withColumn(
        "Premium_Status",
        when(col("Final_Premium") > 60000, "High")
        .otherwise("Normal")
    )

    return df


def select_premium_output(df):
    return df.select(
        "id",
        "Base_Premium",
        "Final_Premium",
        "Premium_Status"
    )


def main():
    spark = create_spark_session()

    input_path = os.getenv("POLICY_INPUT_PATH")
    output_path = os.getenv("PREMIUM_OUTPUT_PATH")

    if not input_path:
        raise ValueError("POLICY_INPUT_PATH is missing in .env")

    if not output_path:
        raise ValueError("PREMIUM_OUTPUT_PATH is missing in .env")

    # Convert local path into a file URI so Spark does not use HDFS
    input_path = f"file://{input_path}"
    output_path = f"file://{output_path}"

    try:
        print("Reading policy data...")
        policy_df = read_policy_data(spark, input_path)

        print("Validating policy data...")
        policy_df = validate_policy_data(policy_df)

        print("Cleaning policy data...")
        policy_df = clean_policy_data(policy_df)

        print("Calculating final premiums...")
        premium_df = calculate_premium(policy_df)

        premium_output = select_premium_output(premium_df)

        print("Policy record count:", policy_df.count())
        print("Premium output count:", premium_output.count())

        premium_output.show(10, truncate=False)

        premium_output.write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(output_path)

        print(f"Premium output written to: {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
