"""
NovaPay Financial - Azure Databricks Transaction Lakehouse

Cloud architecture:
Python Producer
    -> Azure Event Hubs
    -> Azure Databricks / Spark Structured Streaming
    -> Bronze Delta
    -> Data Quality
    -> Silver + Quarantine
    -> Gold Analytics
    -> Rule-Based Risk Scoring

Credentials are retrieved securely from Unity Catalog secrets.
No Azure credentials are stored in this source file.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)


# ============================================================
# 1. CONFIGURATION
# ============================================================

CATALOG = "novapay_databricks_anisha"
SCHEMA = "novapay"

EVENTHUB_NAMESPACE = "novapay-eventhub-anisha"
EVENTHUB_NAME = "financial-transactions"

SECRET_KEY = "eventhub-consumer-connection"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_transactions"
STREAMING_BRONZE_TABLE = (
    f"{CATALOG}.{SCHEMA}.bronze_transactions_streaming"
)
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_transactions"
QUARANTINE_TABLE = f"{CATALOG}.{SCHEMA}.quarantine_transactions"

GOLD_RISK_TABLE = (
    f"{CATALOG}.{SCHEMA}.gold_risk_scored_transactions"
)
GOLD_RISK_SUMMARY = f"{CATALOG}.{SCHEMA}.gold_risk_summary"
GOLD_COUNTRY = f"{CATALOG}.{SCHEMA}.gold_country_metrics"
GOLD_PAYMENT = f"{CATALOG}.{SCHEMA}.gold_payment_method_metrics"
GOLD_MERCHANT = f"{CATALOG}.{SCHEMA}.gold_merchant_metrics"
GOLD_DAILY = f"{CATALOG}.{SCHEMA}.gold_daily_metrics"

CHECKPOINT_VOLUME = (
    f"{CATALOG}.{SCHEMA}.streaming_checkpoints"
)

CHECKPOINT_PATH = (
    f"/Volumes/{CATALOG}/{SCHEMA}/"
    "streaming_checkpoints/bronze_eventhub"
)


# ============================================================
# 2. CREATE UNITY CATALOG OBJECTS
# ============================================================

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}"
)

spark.sql(
    f"CREATE VOLUME IF NOT EXISTS {CHECKPOINT_VOLUME}"
)


# ============================================================
# 3. SECURE EVENT HUBS CONNECTION
# ============================================================

# The connection string is stored securely in Unity Catalog.
# Never print connection_string or jaas_config.

connection_string = dbutils.secrets.get(
    catalog=CATALOG,
    schema=SCHEMA,
    key=SECRET_KEY,
)

bootstrap_servers = (
    f"{EVENTHUB_NAMESPACE}.servicebus.windows.net:9093"
)

# Azure Databricks Serverless uses the shaded Kafka client.
jaas_config = (
    "kafkashaded.org.apache.kafka.common.security.plain."
    "PlainLoginModule required "
    'username="$ConnectionString" '
    f'password="{connection_string}";'
)


# ============================================================
# 4. TRANSACTION SCHEMA
# ============================================================

transaction_schema = StructType(
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


# ============================================================
# 5. BOUNDED EVENT HUBS READ
# ============================================================

eventhub_df = (
    spark.read
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        bootstrap_servers,
    )
    .option("subscribe", EVENTHUB_NAME)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        jaas_config,
    )
    .load()
)

bronze_df = (
    eventhub_df
    .select(
        F.from_json(
            F.col("value").cast("string"),
            transaction_schema,
        ).alias("transaction"),
        F.col("partition").alias("eventhub_partition"),
        F.col("offset").alias("eventhub_offset"),
        F.col("timestamp").alias("eventhub_timestamp"),
    )
    .select(
        "transaction.*",
        "eventhub_partition",
        "eventhub_offset",
        "eventhub_timestamp",
    )
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp(),
    )
)

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_TABLE)
)


# ============================================================
# 6. DATA QUALITY
# ============================================================

validated_df = (
    bronze_df
    .withColumn(
        "validation_error",
        F.when(
            F.col("transaction_id").isNull()
            | (F.trim(F.col("transaction_id")) == ""),
            F.lit("MISSING_TRANSACTION_ID"),
        )
        .when(
            F.col("customer_id").isNull()
            | (F.trim(F.col("customer_id")) == ""),
            F.lit("MISSING_CUSTOMER_ID"),
        )
        .when(
            F.col("amount").isNull(),
            F.lit("MISSING_AMOUNT"),
        )
        .when(
            F.col("amount") <= 0,
            F.lit("INVALID_AMOUNT"),
        )
        .when(
            F.col("currency").isNull()
            | ~F.col("currency").isin(
                "USD",
                "EUR",
                "GBP",
                "CAD",
            ),
            F.lit("INVALID_CURRENCY"),
        )
        .when(
            F.col("status").isNull()
            | ~F.col("status").isin(
                "APPROVED",
                "DECLINED",
                "PENDING",
            ),
            F.lit("INVALID_STATUS"),
        )
        .when(
            F.col("event_timestamp").isNull()
            | (F.trim(F.col("event_timestamp")) == ""),
            F.lit("MISSING_EVENT_TIMESTAMP"),
        )
    )
)

valid_df = validated_df.filter(
    F.col("validation_error").isNull()
)

quarantine_df = validated_df.filter(
    F.col("validation_error").isNotNull()
)

silver_df = valid_df.dropDuplicates(
    ["transaction_id"]
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_TABLE)
)

(
    quarantine_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(QUARANTINE_TABLE)
)


# ============================================================
# 7. RULE-BASED RISK SCORING
# ============================================================

risk_df = (
    silver_df
    .withColumn(
        "risk_score",
        F.when(F.col("amount") > 5000, 30)
        .otherwise(0)
        + F.when(F.col("country") != "US", 20)
        .otherwise(0)
        + F.when(F.col("status") == "DECLINED", 20)
        .otherwise(0),
    )
    .withColumn(
        "risk_level",
        F.when(
            F.col("risk_score") >= 60,
            "HIGH",
        )
        .when(
            F.col("risk_score") >= 30,
            "MEDIUM",
        )
        .otherwise("LOW"),
    )
)

(
    risk_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_RISK_TABLE)
)


# ============================================================
# 8. GOLD ANALYTICAL TABLES
# ============================================================

risk_summary_df = (
    risk_df
    .groupBy("risk_level")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(
            F.sum("amount"),
            2,
        ).alias("total_transaction_value"),
    )
)

(
    risk_summary_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_RISK_SUMMARY)
)


country_metrics_df = (
    risk_df
    .groupBy("country")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(
            F.sum("amount"),
            2,
        ).alias("total_transaction_value"),
        F.round(
            F.avg("amount"),
            2,
        ).alias("average_transaction_value"),
    )
)

(
    country_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_COUNTRY)
)


payment_metrics_df = (
    risk_df
    .groupBy("payment_method")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(
            F.sum("amount"),
            2,
        ).alias("total_transaction_value"),
    )
)

(
    payment_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_PAYMENT)
)


merchant_metrics_df = (
    risk_df
    .groupBy("merchant_category")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(
            F.sum("amount"),
            2,
        ).alias("total_transaction_value"),
    )
)

(
    merchant_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_MERCHANT)
)


daily_metrics_df = (
    risk_df
    .withColumn(
        "transaction_date",
        F.to_date(
            F.to_timestamp("event_timestamp")
        ),
    )
    .groupBy("transaction_date")
    .agg(
        F.count("*").alias("transaction_count"),
        F.round(
            F.sum("amount"),
            2,
        ).alias("total_transaction_value"),
    )
)

(
    daily_metrics_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_DAILY)
)


# ============================================================
# 9. SPARK STRUCTURED STREAMING
# ============================================================

streaming_source = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        bootstrap_servers,
    )
    .option("subscribe", EVENTHUB_NAME)
    .option("startingOffsets", "earliest")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        jaas_config,
    )
    .load()
)

streaming_bronze_df = (
    streaming_source
    .select(
        F.from_json(
            F.col("value").cast("string"),
            transaction_schema,
        ).alias("transaction"),
        F.col("partition").alias("eventhub_partition"),
        F.col("offset").alias("eventhub_offset"),
        F.col("timestamp").alias("eventhub_timestamp"),
    )
    .select(
        "transaction.*",
        "eventhub_partition",
        "eventhub_offset",
        "eventhub_timestamp",
    )
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp(),
    )
)

stream_query = (
    streaming_bronze_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH,
    )
    .trigger(availableNow=True)
    .toTable(STREAMING_BRONZE_TABLE)
)

stream_query.awaitTermination()


# ============================================================
# 10. FINAL VALIDATION
# ============================================================

bronze_count = spark.table(
    BRONZE_TABLE
).count()

streaming_bronze_count = spark.table(
    STREAMING_BRONZE_TABLE
).count()

silver_count = spark.table(
    SILVER_TABLE
).count()

quarantine_count = spark.table(
    QUARANTINE_TABLE
).count()

gold_count = spark.table(
    GOLD_RISK_TABLE
).count()

risk_counts = {
    row["risk_level"]: row["count"]
    for row in (
        spark.table(GOLD_RISK_TABLE)
        .groupBy("risk_level")
        .count()
        .collect()
    )
}

print("NOVAPAY CLOUD LAKEHOUSE - FINAL VALIDATION")
print(f"Bronze Batch:      {bronze_count:,}")
print(f"Bronze Streaming:  {streaming_bronze_count:,}")
print(f"Silver:            {silver_count:,}")
print(f"Quarantine:        {quarantine_count:,}")
print(f"Gold Risk:         {gold_count:,}")
print(f"HIGH Risk:         {risk_counts.get('HIGH', 0):,}")
print(f"MEDIUM Risk:       {risk_counts.get('MEDIUM', 0):,}")
print(f"LOW Risk:          {risk_counts.get('LOW', 0):,}")
print("Azure lakehouse validation completed successfully")