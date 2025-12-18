import csv
import json
import time
from kafka import KafkaProducer


TOPIC_NAME = "transactions_csv_stream"
BOOTSTRAP_SERVERS = "localhost:9092"
CSV_FILE_PATH = "transactions_clean.csv"

STREAM_DELAY = 0.5 


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


with open(CSV_FILE_PATH, newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        producer.send(TOPIC_NAME, value=row)
        print(f"[PRODUCER] Sent: {row}")
        time.sleep(STREAM_DELAY) 

producer.flush()
producer.close()
