import pytest

from pyspark.sql import SparkSession

from src.quality.rules import (
    add_risk_score,
    add_validation_errors,
    remove_duplicates,
)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("NovaPayTests")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


def test_valid_transaction_is_accepted(spark):
    data = [
        (
            "TXN-001",
            "CUST-001",
            100.00,
            "USD",
            "APPROVED",
            "US",
        )
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "amount",
        "currency",
        "status",
        "country",
    ]

    df = spark.createDataFrame(data, columns)

    result = add_validation_errors(df).first()

    assert result.error_reason == ""


def test_negative_amount_is_rejected(spark):
    data = [
        (
            "TXN-002",
            "CUST-002",
            -100.00,
            "USD",
            "APPROVED",
            "US",
        )
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "amount",
        "currency",
        "status",
        "country",
    ]

    df = spark.createDataFrame(data, columns)

    result = add_validation_errors(df).first()

    assert "INVALID_AMOUNT" in result.error_reason


def test_missing_customer_is_rejected(spark):
    data = [
        (
            "TXN-003",
            None,
            200.00,
            "USD",
            "APPROVED",
            "US",
        )
    ]

    schema = """
        transaction_id STRING,
        customer_id STRING,
        amount DOUBLE,
        currency STRING,
        status STRING,
        country STRING
    """

    df = spark.createDataFrame(data, schema)

    result = add_validation_errors(df).first()

    assert "MISSING_CUSTOMER_ID" in result.error_reason


def test_invalid_currency_is_rejected(spark):
    data = [
        (
            "TXN-004",
            "CUST-004",
            300.00,
            "XYZ",
            "APPROVED",
            "US",
        )
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "amount",
        "currency",
        "status",
        "country",
    ]

    df = spark.createDataFrame(data, columns)

    result = add_validation_errors(df).first()

    assert "INVALID_CURRENCY" in result.error_reason


def test_duplicate_transaction_is_removed(spark):
    data = [
        ("TXN-005", "CUST-005"),
        ("TXN-005", "CUST-005"),
    ]

    columns = [
        "transaction_id",
        "customer_id",
    ]

    df = spark.createDataFrame(data, columns)

    result = remove_duplicates(df)

    assert result.count() == 1


def test_high_value_transaction_has_elevated_risk(spark):
    data = [
        (
            "TXN-006",
            7500.00,
            "US",
            "APPROVED",
        )
    ]

    columns = [
        "transaction_id",
        "amount",
        "country",
        "status",
    ]

    df = spark.createDataFrame(data, columns)

    result = add_risk_score(df).first()

    assert result.risk_score == 30
    assert result.risk_level == "MEDIUM"