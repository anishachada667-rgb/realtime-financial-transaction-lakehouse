from pyspark.sql import SparkSession


KAFKA_BOOTSTRAP_SERVERS = "172.23.208.1:9092"
KAFKA_TOPIC = "financial-transactions"


def main():
    print("NovaPay Kafka -> Spark Structured Streaming")
    print("-------------------------------------------")

    spark = (
        SparkSession.builder
        .appName("NovaPayKafkaStreaming")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark version: {spark.version}")
    print(f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print()

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option(
            "subscribe",
            KAFKA_TOPIC
        )
        .option(
            "startingOffsets",
            "earliest"
        )
        .load()
    )

    messages_df = kafka_df.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS kafka_value",
        "topic",
        "partition",
        "offset",
        "timestamp"
    )

    print("Kafka streaming schema:")
    messages_df.printSchema()

    query = (
        messages_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    print()
    print("Kafka -> Spark streaming test completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()