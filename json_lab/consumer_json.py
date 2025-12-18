# Write your json consumer code here
import json
from kafka import KafkaConsumer


TOPIC_NAME = "transactions_csv_stream"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "json_consumer_group"


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("📥 JSON Consumer started...")


for message in consumer:
    try:
        event = message.value

      
        transaction_id = event["transaction_id"]
        user_id = event["user_id"]
        amount = event["amount"]
        timestamp = event["timestamp"]

        print(
            f"[CONSUMER] Offset={message.offset} | "
            f"Transaction={transaction_id} | "
            f"User={user_id} | "
            f"Amount={amount} | "
            f"Time={timestamp}"
        )

    except Exception as e:
        print(
            f"[ERROR] Malformed event at offset {message.offset} "
            f"→ Raw={message.value} | Error={e}"
        )
