import csv
import json
from datetime import datetime

CSV_FILE_PATH = "transactions.csv"
JSON_FILE_PATH = "transactions.json"

def csv_row_to_json(row):
    return {
        "transaction_id": int(row["transaction_id"]),
        "user_id": int(row["user_id"]),
        "amount": float(row["amount"]),
        "timestamp": datetime.strptime(
            row["timestamp"], "%Y-%m-%d %H:%M:%S"
        ).isoformat()
    }

json_rows = []

with open(CSV_FILE_PATH, newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        json_rows.append(csv_row_to_json(row))

with open(JSON_FILE_PATH, "w", encoding="utf-8") as jsonfile:
    json.dump(json_rows, jsonfile, indent=2)

print(f"✅ Conversion terminée : {JSON_FILE_PATH}")
