from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = "172.23.208.1:9092"
KAFKA_TOPIC = "financial-transactions"


TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("event_timestamp", StringType(), True),
    ]
)


def main():
    print("NovaPay Bronze Streaming Pipeline")
    print("--------------------------------")

    project_root = Path(__file__).resolve().parents[2]

    bronze_path = project_root / "data" / "bronze"
    checkpoint_path = project_root / "data" / "checkpoints" / "bronze"

    bronze_path.mkdir(parents=True, exist_ok=True)
    checkpoint_path.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("NovaPayBronzeStreaming")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark version: {spark.version}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print(f"Bronze path: {bronze_path}")
    print()

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        kafka_df
        .select(
            from_json(
                col("value").cast("string"),
                TRANSACTION_SCHEMA
            ).alias("transaction"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "transaction.*",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
    )

    print("Bronze schema:")
    parsed_df.printSchema()

    query = (
        parsed_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option(
            "checkpointLocation",
            str(checkpoint_path)
        )
        .trigger(availableNow=True)
        .start(str(bronze_path))
    )

    query.awaitTermination()

    print()
    print("Bronze streaming ingestion completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()