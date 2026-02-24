import os
import io
import time
import logging
import struct
import requests
import json

from datetime import datetime, timezone
from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("bronze_writer")

KAFKA_BOOTSTRAP     = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
MINIO_ENDPOINT      = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY    = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY    = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET        = os.environ.get("MINIO_BUCKET", "datalake")

TOPICS = [
    "cdc.APP.CUSTOMERS",
    "cdc.APP.ORDERS",
    "cdc.APP.PRODUCTS",
    "cdc.APP.PRODUCT_CATEGORIES",
    "cdc.APP.PRODUCTS_X_CATEGORY",
]

FLUSH_INTERVAL_SEC = 30
BATCH_SIZE = 500

# ---------------------------------------------------------------------------
# MinIO
# ---------------------------------------------------------------------------
def get_minio_client() -> Minio:
    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                   secret_key=MINIO_SECRET_KEY, secure=False)
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        log.info(f"Created bucket: {MINIO_BUCKET}")
    return client

def flush_to_minio(client: Minio, table_name: str, records: list[dict]) -> None:
    if not records:
        return
    now = datetime.now(timezone.utc)
    partition = (f"year={now.year:04d}/month={now.month:02d}/"
                 f"day={now.day:02d}/hour={now.hour:02d}")
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    object_key = f"bronze/{table_name.lower()}/{partition}/{table_name.lower()}_{timestamp}.parquet"
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(records), buf,
                   compression="snappy", write_statistics=True, row_group_size=50_000)
    buf.seek(0)
    size = buf.getbuffer().nbytes
    client.put_object(MINIO_BUCKET, object_key, data=buf, length=size,
                      content_type="application/octet-stream")
    log.info(f"Flushed {len(records)} records → s3://{MINIO_BUCKET}/{object_key} ({size/1024:.1f} KB)")

# ---------------------------------------------------------------------------
# Schema registry
# ---------------------------------------------------------------------------
_schema_cache = {}

def get_schema(schema_id: int) -> dict:
    if schema_id not in _schema_cache:
        resp = requests.get(f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}")
        resp.raise_for_status()
        import fastavro.schema
        _schema_cache[schema_id] = fastavro.schema.parse_schema(
            json.loads(resp.json()["schema"]))
    return _schema_cache[schema_id]

# ---------------------------------------------------------------------------
# Parser — (record, error_reason) tuple-t ad vissza
# ---------------------------------------------------------------------------
def parse_debezium_message(raw_value: bytes) -> tuple[dict | None, str | None]:
    if raw_value is None:
        return None, "null message value"
    try:
        if raw_value[0] == 0:
            schema_id = struct.unpack(">I", raw_value[1:5])[0]
            schema = get_schema(schema_id)
            import fastavro
            envelope = fastavro.schemaless_reader(io.BytesIO(raw_value[5:]), schema)
        else:
            envelope = json.loads(raw_value)
    except Exception as e:
        return None, f"ParseError: {e}"

    if not isinstance(envelope, dict):
        return None, f"envelope is not a dict (got {type(envelope).__name__})"

    op = envelope.get("op")
    after = envelope.get("after")
    before = envelope.get("before")

    if op == "d":
        if not before:
            return None, "delete op missing 'before' payload"
        record = dict(before)
        record["_op"] = "delete"
        record["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        return record, None

    if after:
        record = dict(after)
        record["_op"] = op or "r"
        record["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        return record, None

    return None, f"no 'after' payload for op='{op}'"

# ---------------------------------------------------------------------------
# DLQ producer helper
# ---------------------------------------------------------------------------
def send_to_dlq(producer: Producer, topic: str, raw_value: bytes, reason: str) -> None:
    dlq_topic = f"dlq.{topic}"
    headers = [("kafka_dlt-exception-message", reason.encode("utf-8"))]
    producer.produce(dlq_topic, value=raw_value or b"", headers=headers)
    producer.poll(0)
    log.warning(f"→ DLQ [{dlq_topic}] reason: {reason}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("Starting Bronze Writer...")
    minio_client = get_minio_client()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "bronze-writer-v2",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms": 30_000,
        "heartbeat.interval.ms": 10_000,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    consumer.subscribe(TOPICS)
    log.info(f"Subscribed to topics: {TOPICS}")

    buffers: dict[str, list[dict]] = defaultdict(list)
    last_flush: datetime = datetime.now(timezone.utc)
    committed: bool = False

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                code = msg.error().code()
                if code == KafkaError._PARTITION_EOF:
                    log.debug(f"EOF: {msg.topic()} [{msg.partition()}]")
                elif code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    log.warning("Topics not yet available, waiting 5s...")
                    time.sleep(5)
                    continue
                elif code == KafkaError._ALL_BROKERS_DOWN:
                    log.error("Brokers down, waiting 10s...")
                    time.sleep(10)
                    continue
                else:
                    log.error(f"Kafka error: {msg.error()}")
                    time.sleep(5)
                    continue
            else:
                record, error_reason = parse_debezium_message(msg.value())

                if error_reason:
                    send_to_dlq(producer, msg.topic(), msg.value(), error_reason)
                    consumer.commit(asynchronous=False)
                    continue

                table = msg.topic().split(".")[-1]
                buffers[table].append(record)
                committed = False

            now     = datetime.now(timezone.utc)
            elapsed = (now - last_flush).total_seconds()
            total   = sum(len(v) for v in buffers.values())

            if (elapsed >= FLUSH_INTERVAL_SEC or total >= BATCH_SIZE) and total > 0:
                for table, records in buffers.items():
                    flush_to_minio(minio_client, table, records)
                consumer.commit(asynchronous=False)
                committed = True
                buffers.clear()
                last_flush = datetime.now(timezone.utc)

    except KeyboardInterrupt:
        log.info("Shutting down...")
    finally:
        if buffers:
            for table, records in buffers.items():
                flush_to_minio(minio_client, table, records)
            if not committed:
                log.info("Nothing to commit on shutdown.")
            else:
                try:
                    consumer.commit(asynchronous=False)
                except KafkaException as e:
                    log.warning(f"Final commit skipped: {e}")
        producer.flush()
        consumer.close()
        log.info("Consumer closed.")

if __name__ == "__main__":
    main()
