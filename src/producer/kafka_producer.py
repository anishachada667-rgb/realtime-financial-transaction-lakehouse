import json
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "financial-transactions"


def create_producer():
    """
    Create and return a Kafka producer.
    """

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3,
    )


def load_transactions():
    """
    Load generated transactions from the raw JSON file.
    """

    project_root = Path(__file__).resolve().parents[2]
    input_file = project_root / "data" / "raw" / "transactions.json"

    if not input_file.exists():
        raise FileNotFoundError(
            f"Transaction file not found: {input_file}\n"
            "Run transaction_generator.py first."
        )

    with input_file.open("r", encoding="utf-8") as file:
        return json.load(file)


def publish_transactions(producer, transactions):
    """
    Publish transactions to the Kafka topic.
    """

    successful = 0
    failed = 0

    for transaction in transactions:
        try:
            transaction_id = transaction["transaction_id"]

            future = producer.send(
                KAFKA_TOPIC,
                key=transaction_id,
                value=transaction,
            )

            metadata = future.get(timeout=10)

            successful += 1

            if successful <= 5:
                print(
                    f"Sent {transaction_id} "
                    f"-> partition {metadata.partition}, "
                    f"offset {metadata.offset}"
                )

        except (KafkaError, KeyError) as error:
            failed += 1
            print(f"Failed to publish transaction: {error}")

    producer.flush()

    return successful, failed


def main():
    print("NovaPay Kafka Transaction Producer")
    print("---------------------------------")

    transactions = load_transactions()

    print(f"Loaded {len(transactions):,} transaction records.")
    print(f"Kafka broker: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")
    print()

    start_time = time.perf_counter()

    producer = create_producer()

    try:
        successful, failed = publish_transactions(
            producer,
            transactions,
        )
    finally:
        producer.close()

    elapsed_time = time.perf_counter() - start_time

    print()
    print("Publishing complete.")
    print(f"Successful messages: {successful:,}")
    print(f"Failed messages: {failed:,}")
    print(f"Elapsed time: {elapsed_time:.2f} seconds")

    if elapsed_time > 0:
        throughput = successful / elapsed_time
        print(f"Producer throughput: {throughput:,.2f} messages/second")


if __name__ == "__main__":
    main()