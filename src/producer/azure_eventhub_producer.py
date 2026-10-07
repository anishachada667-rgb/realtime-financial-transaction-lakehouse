import json
import os
import time
from pathlib import Path

from azure.eventhub import EventData, EventHubProducerClient


EVENT_HUB_NAME = "financial-transactions"

CONNECTION_STRING = os.getenv("AZURE_EVENTHUB_CONNECTION_STRING")

if not CONNECTION_STRING:
    raise ValueError(
        "AZURE_EVENTHUB_CONNECTION_STRING environment variable is not set."
    )


# Project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

TRANSACTION_FILE = PROJECT_ROOT / "data" / "raw" / "transactions.json"


def load_transactions():
    with open(TRANSACTION_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def main():
    transactions = load_transactions()

    print("=" * 60)
    print("NovaPay Azure Event Hubs Producer")
    print("=" * 60)
    print(f"Event Hub: {EVENT_HUB_NAME}")
    print(f"Transactions loaded: {len(transactions)}")
    print()

    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENT_HUB_NAME,
    )

    successful = 0
    start_time = time.time()

    try:
        event_batch = producer.create_batch()

        for transaction in transactions:
            event = EventData(json.dumps(transaction))

            try:
                event_batch.add(event)
            except ValueError:
                producer.send_batch(event_batch)
                successful += len(event_batch)

                event_batch = producer.create_batch()
                event_batch.add(event)

        if len(event_batch) > 0:
            producer.send_batch(event_batch)
            successful += len(event_batch)

    finally:
        producer.close()

    elapsed = time.time() - start_time
    throughput = successful / elapsed if elapsed > 0 else 0

    print("=" * 60)
    print("Azure Event Hubs Publishing Results")
    print("=" * 60)
    print(f"Successful events: {successful}")
    print(f"Failed events: {len(transactions) - successful}")
    print(f"Elapsed time: {elapsed:.2f} seconds")
    print(f"Producer throughput: {throughput:.2f} events/sec")
    print("=" * 60)


if __name__ == "__main__":
    main()