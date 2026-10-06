from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    lit,
    round,
    sum as spark_sum,
    to_date,
    when,
)


def main():
    print("NovaPay Gold Analytics Pipeline")
    print("-------------------------------")

    project_root = Path(__file__).resolve().parents[2]

    silver_path = project_root / "data" / "silver"
    gold_path = project_root / "data" / "gold"

    spark = (
        SparkSession.builder
        .appName("NovaPayGoldAnalytics")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(str(silver_path))

    print(f"Silver records received: {df.count():,}")

    # --------------------------------------------------
    # Risk scoring
    # --------------------------------------------------

    risk_df = (
        df
        .withColumn(
            "risk_score",
            when(col("amount") > 5000, lit(30)).otherwise(lit(0))
            +
            when(col("country") != "US", lit(20)).otherwise(lit(0))
            +
            when(col("status") == "DECLINED", lit(20)).otherwise(lit(0))
        )
        .withColumn(
            "risk_level",
            when(col("risk_score") >= 60, "HIGH")
            .when(col("risk_score") >= 30, "MEDIUM")
            .otherwise("LOW")
        )
    )

    risk_output = gold_path / "risk_scored_transactions"

    risk_df.write.mode("overwrite").parquet(
        str(risk_output)
    )

    # --------------------------------------------------
    # Daily transaction metrics
    # --------------------------------------------------

    daily_df = (
        risk_df
        .withColumn(
            "transaction_date",
            to_date(col("event_timestamp"))
        )
        .groupBy("transaction_date")
        .agg(
            count("*").alias("transaction_count"),
            round(
                spark_sum("amount"), 2
            ).alias("total_transaction_amount"),
            round(
                avg("amount"), 2
            ).alias("average_transaction_amount"),
        )
        .orderBy("transaction_date")
    )

    daily_df.write.mode("overwrite").parquet(
        str(gold_path / "daily_transaction_metrics")
    )

    # --------------------------------------------------
    # Merchant metrics
    # --------------------------------------------------

    merchant_df = (
        risk_df
        .groupBy(
            "merchant_id",
            "merchant_category"
        )
        .agg(
            count("*").alias("transaction_count"),
            round(
                spark_sum("amount"), 2
            ).alias("total_transaction_amount"),
            round(
                avg("amount"), 2
            ).alias("average_transaction_amount"),
        )
        .orderBy(
            col("total_transaction_amount").desc()
        )
    )

    merchant_df.write.mode("overwrite").parquet(
        str(gold_path / "merchant_transaction_metrics")
    )

    # --------------------------------------------------
    # Country metrics
    # --------------------------------------------------

    country_df = (
        risk_df
        .groupBy("country")
        .agg(
            count("*").alias("transaction_count"),
            round(
                spark_sum("amount"), 2
            ).alias("total_transaction_amount"),
            round(
                avg("amount"), 2
            ).alias("average_transaction_amount"),
        )
        .orderBy(
            col("total_transaction_amount").desc()
        )
    )

    country_df.write.mode("overwrite").parquet(
        str(gold_path / "country_transaction_metrics")
    )

    # --------------------------------------------------
    # Payment method metrics
    # --------------------------------------------------

    payment_df = (
        risk_df
        .groupBy("payment_method")
        .agg(
            count("*").alias("transaction_count"),
            round(
                spark_sum("amount"), 2
            ).alias("total_transaction_amount"),
        )
        .orderBy(
            col("transaction_count").desc()
        )
    )

    payment_df.write.mode("overwrite").parquet(
        str(gold_path / "payment_method_metrics")
    )

    # --------------------------------------------------
    # Risk summary
    # --------------------------------------------------

    risk_summary_df = (
        risk_df
        .groupBy("risk_level")
        .agg(
            count("*").alias("transaction_count"),
            round(
                spark_sum("amount"), 2
            ).alias("total_transaction_amount"),
        )
        .orderBy("risk_level")
    )

    risk_summary_df.write.mode("overwrite").parquet(
        str(gold_path / "risk_summary")
    )

    print()
    print("Risk Summary:")
    risk_summary_df.show(truncate=False)

    print("Country Metrics:")
    country_df.show(truncate=False)

    print("Payment Method Metrics:")
    payment_df.show(truncate=False)

    print("Gold analytics processing completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()