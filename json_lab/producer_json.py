#write your json producer code here
import csv
import json
import time
from datetime import datetime
from kafka import KafkaProducer


TOPIC_NAME = "transactions_csv_stream"
BOOTSTRAP_SERVERS = "localhost:9092"
CSV_FILE_PATH = "transactions.csv"
STREAM_DELAY = 0.5


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def csv_row_to_json(row):
    """
    Convert CSV row to typed JSON object
    Raise ValueError if malformed
    """
    return {
        "transaction_id": int(row["transaction_id"]),
        "user_id": int(row["user_id"]),
        "amount": float(row["amount"]),
        "timestamp": datetime.strptime(
            row["timestamp"], "%Y-%m-%d %H:%M:%S"
        ).isoformat()
    }

with open(CSV_FILE_PATH, newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)

    for line_number, row in enumerate(reader, start=1):
        try:
            json_message = csv_row_to_json(row)

            producer.send(TOPIC_NAME, value=json_message)
            print(f"[PRODUCER] Sent line {line_number}: {json_message}")

            time.sleep(STREAM_DELAY)

        except Exception as e:
            print(
                f"[ERROR] Malformed record at line {line_number} "
                f"→ {row} | Error: {e}"
            )

producer.flush()
producer.close()

