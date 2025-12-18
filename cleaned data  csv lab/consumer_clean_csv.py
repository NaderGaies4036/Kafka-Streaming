import json
from kafka import KafkaConsumer

TOPIC_NAME = "transactions_csv_stream"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "csv_consumer_group"


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,   
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("📥 Consumer started...")


for message in consumer:
    print(
        f"[CONSUMER] Partition={message.partition} "
        f"Offset={message.offset} "
        f"Value={message.value}"
    )
