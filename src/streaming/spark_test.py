from pyspark.sql import SparkSession


def main():
    print("NovaPay Spark Environment Test")
    print("------------------------------")

    spark = (
        SparkSession.builder
        .appName("NovaPaySparkTest")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark version: {spark.version}")

    data = [
        ("TXN-001", "CUST-1001", 250.50, "USD", "APPROVED"),
        ("TXN-002", "CUST-1002", 1200.00, "USD", "DECLINED"),
        ("TXN-003", "CUST-1003", 75.25, "CAD", "APPROVED"),
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "amount",
        "currency",
        "status",
    ]

    df = spark.createDataFrame(data, columns)

    print()
    print("Spark DataFrame:")
    df.show(truncate=False)

    print(f"Record count: {df.count()}")

    approved_count = df.filter(
        df.status == "APPROVED"
    ).count()

    print(f"Approved transactions: {approved_count}")

    spark.stop()

    print()
    print("Spark test completed successfully.")


if __name__ == "__main__":
    main()