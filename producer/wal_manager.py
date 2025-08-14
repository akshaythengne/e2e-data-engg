import os
from datetime import datetime

WAL_FILE = "wal.log"

def write_wal(message: str):
    """Append message to WAL before sending to Kafka."""
    with open(WAL_FILE, "a") as f:
        f.write(f"{datetime.utcnow().isoformat()}|{message}\n")

def read_wal():
    """Read WAL entries for replay."""
    if not os.path.exists(WAL_FILE):
        return []
    with open(WAL_FILE, "r") as f:
        return [line.strip().split("|", 1)[1] for line in f.readlines()]

def clear_wal():
    """Clear WAL after successful replay."""
    if os.path.exists(WAL_FILE):
        os.remove(WAL_FILE)
