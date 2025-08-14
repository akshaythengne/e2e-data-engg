import os
import json
import time
import requests
from kafka import KafkaProducer
from wal_manager import write_wal, read_wal, clear_wal
from dotenv import load_dotenv

load_dotenv()

BROKER = os.getenv("KAFKA_BROKER")
TOPIC = os.getenv("KAFKA_TOPIC")

producer = KafkaProducer(
    bootstrap_servers=[BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_crypto_price():
    url = "https://api.coindesk.com/v1/bpi/currentprice/BTC.json"
    r = requests.get(url)
    data = r.json()
    return {
        "symbol": "BTC",
        "price_usd": data["bpi"]["USD"]["rate_float"],
        "ts": data["time"]["updatedISO"]
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
        write_wal(json.dumps(msg))
        send_message(msg)
        print(f"Produced: {msg}")
        time.sleep(10)
