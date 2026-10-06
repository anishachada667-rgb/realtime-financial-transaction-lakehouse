from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    trim,
    upper,
    when,
)


VALID_CURRENCIES = ["USD", "EUR", "GBP", "CAD"]
VALID_STATUSES = ["APPROVED", "DECLINED", "PENDING"]


def add_validation_errors(df):
    return (
        df
        .withColumn("currency", upper(trim(col("currency"))))
        .withColumn("status", upper(trim(col("status"))))
        .withColumn(
            "error_reason",
            concat_ws(
                "; ",
                when(
                    col("transaction_id").isNull(),
                    lit("MISSING_TRANSACTION_ID"),
                ),
                when(
                    col("customer_id").isNull(),
                    lit("MISSING_CUSTOMER_ID"),
                ),
                when(
                    col("amount").isNull(),
                    lit("MISSING_AMOUNT"),
                ),
                when(
                    col("amount") <= 0,
                    lit("INVALID_AMOUNT"),
                ),
                when(
                    ~col("currency").isin(VALID_CURRENCIES),
                    lit("INVALID_CURRENCY"),
                ),
                when(
                    ~col("status").isin(VALID_STATUSES),
                    lit("INVALID_STATUS"),
                ),
            ),
        )
    )


def remove_duplicates(df):
    return df.dropDuplicates(["transaction_id"])


def add_risk_score(df):
    return (
        df
        .withColumn(
            "risk_score",
            when(col("amount") > 5000, lit(30)).otherwise(lit(0))
            + when(col("country") != "US", lit(20)).otherwise(lit(0))
            + when(col("status") == "DECLINED", lit(20)).otherwise(lit(0)),
        )
        .withColumn(
            "risk_level",
            when(col("risk_score") >= 60, "HIGH")
            .when(col("risk_score") >= 30, "MEDIUM")
            .otherwise("LOW"),
        )
    )