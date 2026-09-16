import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

load_dotenv()


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Insurance Risk ETL")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )


def read_policy_data(spark, path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"file://{path}")
    )


def validate_columns(df):
    required_columns = [
        "id",
        "Age",
        "Vehicle_Age",
        "Vehicle_Damage",
        "Previously_Insured",
        "Annual_Premium",
    ]

    missing_columns = [
        column for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    return df


def clean_policy_data(df):
    return (
        df.dropDuplicates(["id"])
        .dropna(
            subset=[
                "id",
                "Age",
                "Vehicle_Age",
                "Vehicle_Damage",
                "Previously_Insured",
                "Annual_Premium",
            ]
        )
    )


def calculate_risk(df):
    risk_score = (
        when(col("Age") > 60, 30).otherwise(0)
        + when(col("Vehicle_Age") == "> 2 Years", 20).otherwise(0)
        + when(col("Vehicle_Damage") == "Yes", 25).otherwise(0)
        + when(col("Previously_Insured") == 0, 15).otherwise(0)
        + when(col("Annual_Premium") > 50000, 10).otherwise(0)
    )

    df = df.withColumn("Risk_Score", risk_score)

    df = df.withColumn(
        "Risk_Level",
        when(col("Risk_Score") <= 30, "Low")
        .when(col("Risk_Score") <= 60, "Medium")
        .otherwise("High")
    )

    return df


def main():
    spark = create_spark_session()

    policy_path = os.getenv("POLICY_INPUT_PATH")
    output_path = os.getenv("RISK_OUTPUT_PATH")

    if not policy_path:
        raise ValueError("POLICY_INPUT_PATH is missing in .env")

    if not output_path:
        raise ValueError("RISK_OUTPUT_PATH is missing in .env")

    try:
        print("Reading policy data...")

        policy_df = read_policy_data(
            spark,
            policy_path
        )

        print("Validating policy data...")

        policy_df = validate_columns(policy_df)

        print("Cleaning policy data...")

        policy_df = clean_policy_data(policy_df)

        print("Calculating risk score...")

        risk_df = calculate_risk(policy_df)

        risk_output = risk_df.select(
            "id",
            "Risk_Score",
            "Risk_Level"
        )

        print("Policy records:", policy_df.count())
        print("Risk records:", risk_output.count())

        print("Sample risk output:")
        risk_output.show(10, truncate=False)

        print("Writing risk output...")

        (
            risk_output.write
            .mode("overwrite")
            .option("header", True)
            .csv(f"file://{output_path}")
        )

        print(f"Risk output written to: {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
