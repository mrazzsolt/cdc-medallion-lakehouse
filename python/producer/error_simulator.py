import json
import os
import random
import signal
import time

from confluent_kafka import Producer
from datetime import datetime

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

TOPICS = [
    "cdc.APP.CUSTOMERS",
    "cdc.APP.ORDERS",
    "cdc.APP.PRODUCTS",
    "cdc.APP.PRODUCT_CATEGORIES",
    "cdc.APP.PRODUCTS_X_CATEGORY",
]

SLEEP_SECONDS = 200.0
ERRORS_PER_BATCH = 2

ERROR_GENERATORS = {
    "invalid_json": lambda: b"this is not json at all }{",
    "missing_after": lambda: json.dumps({"op": "c", "after": None, "before": None}).encode(),
    "missing_before_on_delete": lambda: json.dumps({"op": "d", "after": None, "before": None}).encode(),
    "not_a_dict_envelope": lambda: json.dumps([1, 2, 3]).encode(),
    "empty_bytes": lambda: b"",
    "truncated_avro": lambda: b"\x00\x00\x00\x00\x01\xDE\xAD\xBE\xEF",  # magic byte de érvénytelen avro
    "unknown_op_no_after": lambda: json.dumps({"op": "x", "after": None}).encode(),
}

running = True

def handle_shutdown(sig, frame):
    global running
    print("\nShutdown signal received.")
    running = False

signal.signal(signal.SIGINT,  handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

def delivery_report(err, msg):
    if err:
        print(f"[DELIVERY ERROR] {err}")
    else:
        print(f"[SENT] {msg.topic()} | {msg.value()[:60]}...")

def run():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    cycle    = 0

    print("=" * 55)
    print(" CDC Error Simulator")
    print(f" Errors/batch : {ERRORS_PER_BATCH}")
    print(f" Interval : {SLEEP_SECONDS}s")
    print(" Press Ctrl+C to stop")
    print("=" * 55)

    while running:
        cycle += 1
        ts = datetime.now().strftime("%H:%M:%S")
        sent = []

        for _ in range(ERRORS_PER_BATCH):
            topic = random.choice(TOPICS)
            error_type = random.choice(list(ERROR_GENERATORS.keys()))
            bad_payload = ERROR_GENERATORS[error_type]()

            producer.produce(topic, value=bad_payload, callback=delivery_report)
            sent.append(f"{topic.split('.')[-1]}:{error_type}")

        producer.poll(0)
        print(f"[{ts}] Cycle #{cycle:04d} → {' | '.join(sent)}")
        time.sleep(SLEEP_SECONDS)

    producer.flush()
    print("Error simulator stopped cleanly.")

if __name__ == "__main__":
    run()
