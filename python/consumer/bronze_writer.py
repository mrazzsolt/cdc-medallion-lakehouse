import os
import io
import time
import logging
from datetime import datetime, timezone
from collections import defaultdict

import struct
import requests
import json

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from confluent_kafka import Consumer, KafkaError, KafkaException

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("bronze_writer")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SCHEMA_REGISTRY   = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
MINIO_ENDPOINT    = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY  = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY  = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET      = os.environ.get("MINIO_BUCKET", "datalake")

TOPICS = [
    "cdc.APP.CUSTOMERS",
    "cdc.APP.ORDERS",
    "cdc.APP.PRODUCTS",
    "cdc.APP.PRODUCT_CATEGORIES",
    "cdc.APP.PRODUCTS_X_CATEGORY",
]

FLUSH_INTERVAL_SEC = 30
BATCH_SIZE         = 500

# ---------------------------------------------------------------------------
# MinIO client
# ---------------------------------------------------------------------------
def get_minio_client() -> Minio:
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        log.info(f"Created bucket: {MINIO_BUCKET}")
    return client


# ---------------------------------------------------------------------------
# Parquet write to MinIO
# ---------------------------------------------------------------------------
def flush_to_minio(client: Minio, table_name: str, records: list[dict]) -> None:
    if not records:
        return

    now = datetime.now(timezone.utc)
    partition = (
        f"year={now.year:04d}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}"
    )
    timestamp  = now.strftime("%Y%m%d_%H%M%S")
    object_key = f"bronze/{table_name.lower()}/{partition}/{table_name.lower()}_{timestamp}.parquet"

    arrow_table = pa.Table.from_pylist(records)
    buf = io.BytesIO()
    pq.write_table(
        arrow_table,
        buf,
        compression="snappy",
        write_statistics=True,
        row_group_size=50_000,
    )
    buf.seek(0)
    size = buf.getbuffer().nbytes

    client.put_object(
        MINIO_BUCKET,
        object_key,
        data=buf,
        length=size,
        content_type="application/octet-stream"
    )
    log.info(f"Flushed {len(records)} records → s3://{MINIO_BUCKET}/{object_key} ({size/1024:.1f} KB)")


# ---------------------------------------------------------------------------
# Debezium envelope parser
# ---------------------------------------------------------------------------
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
_schema_cache = {}

def get_schema(schema_id: int) -> dict:
    if schema_id not in _schema_cache:
        resp = requests.get(f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}")
        resp.raise_for_status()
        import fastavro.schema
        _schema_cache[schema_id] = fastavro.schema.parse_schema(
            json.loads(resp.json()["schema"])
        )
    return _schema_cache[schema_id]

def parse_debezium_message(raw_value: bytes) -> dict | None:
    if raw_value is None:
        return None
    try:
        # Avro: magic byte (0x00) + 4 bytes schema_id + avro payload
        if raw_value[0] == 0:
            schema_id = struct.unpack(">I", raw_value[1:5])[0]
            schema = get_schema(schema_id)
            import fastavro
            envelope = fastavro.schemaless_reader(
                io.BytesIO(raw_value[5:]), schema
            )
        else:
            envelope = json.loads(raw_value)
    except Exception as e:
        log.warning(f"Failed to parse message: {e}")
        return None

    if isinstance(envelope, dict):
        op    = envelope.get("op")
        after = envelope.get("after")
        before = envelope.get("before")

        if op == "d":
            record = dict(before) if before else {}
            record["_op"]          = "delete"
            record["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            return record

        if after:
            record = dict(after)
            record["_op"]          = op or "r"
            record["_ingested_at"] = datetime.now(timezone.utc).isoformat()
            return record

    return None


# ---------------------------------------------------------------------------
# Main consumer loop
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("Starting Bronze Writer...")

    minio_client = get_minio_client()

    consumer = Consumer({
        "bootstrap.servers":    KAFKA_BOOTSTRAP,
        "group.id":             "bronze-writer-v2",
        "auto.offset.reset":    "earliest",
        "enable.auto.commit":   False,
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms":   30_000,
        "heartbeat.interval.ms": 10_000,
    })
    consumer.subscribe(TOPICS)
    log.info(f"Subscribed to topics: {TOPICS}")

    buffers: dict[str, list[dict]] = defaultdict(list)
    last_flush = datetime.now(timezone.utc)
    committed = False

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            #log.info(f"Poll result: {msg}")

            if msg is None:
                pass
            elif msg.error():
                error_code = msg.error().code()
                if error_code == KafkaError._PARTITION_EOF:
                    log.debug(f"EOF: {msg.topic()} [{msg.partition()}]")
                elif error_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    log.warning("Topics not yet available, waiting 5s...")
                    time.sleep(5)
                    # NE hívj unsubscribe/subscribe-ot! Csak várj és pollozz tovább
                    continue
                elif error_code == KafkaError._ALL_BROKERS_DOWN:
                    log.error("Brokers down, waiting 10s...")
                    time.sleep(10)
                    continue
                else:
                    log.error(f"Kafka error: {msg.error()}")
                    time.sleep(5)
                    continue
                
            else:
                record = parse_debezium_message(msg.value())
                if record:
                    table = msg.topic().split(".")[-1]
                    buffers[table].append(record)
                    committed = False

            now            = datetime.now(timezone.utc)
            elapsed        = (now - last_flush).total_seconds()
            total_buffered = sum(len(v) for v in buffers.values())

            if (elapsed >= FLUSH_INTERVAL_SEC or total_buffered >= BATCH_SIZE) and total_buffered > 0:
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
        consumer.close()
        log.info("Consumer closed.")


if __name__ == "__main__":
    main()
