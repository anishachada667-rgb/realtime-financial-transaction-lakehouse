from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    to_timestamp,
    trim,
    upper,
    when,
)


VALID_CURRENCIES = ["USD", "EUR", "GBP", "CAD"]
VALID_STATUSES = ["APPROVED", "DECLINED", "PENDING"]


def main():
    print("NovaPay Data Quality Pipeline")
    print("-----------------------------")

    project_root = Path(__file__).resolve().parents[2]

    bronze_path = project_root / "data" / "bronze"
    silver_path = project_root / "data" / "silver"
    quarantine_path = project_root / "data" / "quarantine"

    spark = (
        SparkSession.builder
        .appName("NovaPayDataQuality")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(str(bronze_path))

    bronze_count = df.count()

    print(f"Bronze records received: {bronze_count:,}")

    # Standardize selected fields before validation.
    standardized_df = (
        df
        .withColumn("currency", upper(trim(col("currency"))))
        .withColumn("status", upper(trim(col("status"))))
        .withColumn(
            "event_timestamp_parsed",
            to_timestamp(col("event_timestamp"))
        )
    )

    # Build a readable error reason for every invalid record.
    validated_df = standardized_df.withColumn(
        "error_reason",
        concat_ws(
            "; ",
            when(
                col("transaction_id").isNull(),
                lit("MISSING_TRANSACTION_ID")
            ),
            when(
                col("customer_id").isNull(),
                lit("MISSING_CUSTOMER_ID")
            ),
            when(
                col("amount").isNull(),
                lit("MISSING_AMOUNT")
            ),
            when(
                col("amount") <= 0,
                lit("INVALID_AMOUNT")
            ),
            when(
                ~col("currency").isin(VALID_CURRENCIES),
                lit("INVALID_CURRENCY")
            ),
            when(
                ~col("status").isin(VALID_STATUSES),
                lit("INVALID_STATUS")
            ),
            when(
                col("event_timestamp_parsed").isNull(),
                lit("INVALID_OR_MISSING_TIMESTAMP")
            ),
        )
    )

    invalid_df = validated_df.filter(
        col("error_reason") != ""
    )

    valid_before_dedup_df = validated_df.filter(
        col("error_reason") == ""
    )

    invalid_count = invalid_df.count()
    valid_before_dedup_count = valid_before_dedup_df.count()

    # Remove duplicate transactions from the valid dataset.
    silver_df = (
        valid_before_dedup_df
        .dropDuplicates(["transaction_id"])
        .drop("error_reason", "event_timestamp")
        .withColumnRenamed(
            "event_timestamp_parsed",
            "event_timestamp"
        )
    )

    silver_count = silver_df.count()

    duplicate_count = valid_before_dedup_count - silver_count

    quarantine_df = invalid_df.drop(
        "event_timestamp_parsed"
    )

    print(f"Valid before deduplication: {valid_before_dedup_count:,}")
    print(f"Invalid / quarantined: {invalid_count:,}")
    print(f"Duplicates removed from valid data: {duplicate_count:,}")
    print(f"Final Silver records: {silver_count:,}")

    silver_df.write.mode("overwrite").parquet(
        str(silver_path)
    )

    quarantine_df.write.mode("overwrite").parquet(
        str(quarantine_path)
    )

    print()
    print("Data quality processing completed successfully.")
    print(f"Silver path: {silver_path}")
    print(f"Quarantine path: {quarantine_path}")

    spark.stop()


if __name__ == "__main__":
    main()