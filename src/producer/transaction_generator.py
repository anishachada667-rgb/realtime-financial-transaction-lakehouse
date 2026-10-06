import json
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path


NUM_TRANSACTIONS = 1000

MERCHANT_CATEGORIES = [
    "GROCERY",
    "RESTAURANT",
    "ELECTRONICS",
    "TRAVEL",
    "FUEL",
    "HEALTHCARE",
    "ENTERTAINMENT",
    "RETAIL",
]

TRANSACTION_TYPES = [
    "PURCHASE",
    "REFUND",
    "TRANSFER",
    "WITHDRAWAL",
]

CURRENCIES = [
    "USD",
    "EUR",
    "GBP",
    "CAD",
]

PAYMENT_METHODS = [
    "CREDIT_CARD",
    "DEBIT_CARD",
    "BANK_TRANSFER",
    "DIGITAL_WALLET",
]

LOCATIONS = [
    ("US", "Cincinnati"),
    ("US", "New York"),
    ("US", "Chicago"),
    ("US", "Dallas"),
    ("US", "San Francisco"),
    ("CA", "Toronto"),
    ("GB", "London"),
    ("DE", "Berlin"),
]

DEVICE_TYPES = [
    "MOBILE",
    "WEB",
    "POS",
]

STATUSES = [
    "APPROVED",
    "DECLINED",
    "PENDING",
]


def generate_transaction():
    country, city = random.choice(LOCATIONS)

    transaction = {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "account_id": f"ACC-{random.randint(10000, 99999)}",
        "merchant_id": f"MER-{random.randint(100, 999)}",
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "amount": round(random.uniform(1.00, 10000.00), 2),
        "currency": random.choice(CURRENCIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "country": country,
        "city": city,
        "device_type": random.choice(DEVICE_TYPES),
        "status": random.choices(
            STATUSES,
            weights=[85, 10, 5],
            k=1,
        )[0],
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return transaction


def introduce_bad_data(transactions):
    """
    Introduce controlled data-quality problems into approximately
    5% of generated transactions.
    """

    bad_record_count = max(1, int(len(transactions) * 0.05))

    selected_indexes = random.sample(
        range(len(transactions)),
        bad_record_count,
    )

    error_types = [
        "MISSING_CUSTOMER",
        "NEGATIVE_AMOUNT",
        "INVALID_CURRENCY",
        "INVALID_STATUS",
        "MISSING_TIMESTAMP",
    ]

    for index in selected_indexes:
        error_type = random.choice(error_types)
        transaction = transactions[index]

        if error_type == "MISSING_CUSTOMER":
            transaction["customer_id"] = None

        elif error_type == "NEGATIVE_AMOUNT":
            transaction["amount"] = -abs(transaction["amount"])

        elif error_type == "INVALID_CURRENCY":
            transaction["currency"] = "XYZ"

        elif error_type == "INVALID_STATUS":
            transaction["status"] = "UNKNOWN"

        elif error_type == "MISSING_TIMESTAMP":
            transaction["event_timestamp"] = None

    return transactions


def introduce_duplicates(transactions, duplicate_count=10):
    """
    Add duplicate transactions to simulate duplicate events
    arriving from the source system.
    """

    duplicate_records = random.sample(transactions, duplicate_count)

    for transaction in duplicate_records:
        transactions.append(transaction.copy())

    return transactions


def generate_transactions(count):
    transactions = [
        generate_transaction()
        for _ in range(count)
    ]

    transactions = introduce_bad_data(transactions)
    transactions = introduce_duplicates(transactions)

    return transactions


def save_transactions(transactions):
    project_root = Path(__file__).resolve().parents[2]

    output_directory = project_root / "data" / "raw"
    output_directory.mkdir(parents=True, exist_ok=True)

    output_file = output_directory / "transactions.json"

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(transactions, file, indent=2)

    return output_file


def main():
    print("NovaPay Financial Transaction Generator")
    print("---------------------------------------")
    print(f"Generating {NUM_TRANSACTIONS:,} transactions...")

    transactions = generate_transactions(NUM_TRANSACTIONS)

    output_file = save_transactions(transactions)

    print(f"Base transactions generated: {NUM_TRANSACTIONS:,}")
    print(f"Duplicate records added: {len(transactions) - NUM_TRANSACTIONS:,}")
    print(f"Total records written: {len(transactions):,}")
    print(f"Output file: {output_file}")


if __name__ == "__main__":
    main()