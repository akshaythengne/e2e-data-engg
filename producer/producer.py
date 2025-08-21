import os
import json
import time
import requests
from kafka import KafkaProducer
from wal_manager import write_wal, read_wal, clear_wal
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

BROKER = "localhost:9092"
TOPIC = os.getenv("KAFKA_TOPIC")

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_crypto_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    r = requests.get(url)
    data = r.json()
    if "bitcoin" not in data:
        return None  # skip this cycle if API fails
    return {
        "symbol": "BTC",
        "price_usd": data["bitcoin"]["usd"],
        "ts": datetime.utcnow().isoformat()  # Current UTC timestamp
    }

def send_message(msg):
    producer.send(TOPIC, msg)
    producer.flush()

def replay_wal():
    print("Replaying WAL...")
    for entry in read_wal():
        send_message(json.loads(entry))
    clear_wal()

if __name__ == "__main__":
    replay_wal() #whenever the script starts, replay WAL if exists, this way we don't lose 
    #messages that were not sent to Kafka

    while True:
        msg = fetch_crypto_price()
        if msg:
            write_wal(json.dumps(msg))
            send_message(msg)
            print(f"Produced: {msg}")
            time.sleep(100) # Sleep for 10 seconds before next fetch
        else:
            print("Skipped - API response invalid")
            time.sleep(10)
