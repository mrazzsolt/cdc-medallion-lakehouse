import os
import io
import json
import logging
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, KafkaError
from minio import Minio

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("dlq_handler")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "datalake")

DLQ_TOPICS = [
    "dlq.cdc.APP.CUSTOMERS",
    "dlq.cdc.APP.ORDERS",
    "dlq.cdc.APP.PRODUCTS",
    "dlq.cdc.APP.PRODUCT_CATEGORIES",
    "dlq.cdc.APP.PRODUCTS_X_CATEGORY",
]

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
    return client


# ---------------------------------------------------------------------------
# DLQ record archiver
# ---------------------------------------------------------------------------
def archive_dlq_record(client: Minio, topic: str, raw_value: bytes, error_reason: str) -> None:
    now = datetime.now(timezone.utc)
    table_name = topic.split(".")[-1].lower()
    partition = (
        f"year={now.year:04d}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}"
    )
    timestamp = now.strftime("%Y%m%d_%H%M%S_%f")
    object_key = f"dlq/{table_name}/{partition}/{timestamp}.json"

    payload = {
        "topic": topic,
        "error_reason": error_reason,
        "raw_value": raw_value.decode("utf-8", errors="replace") if raw_value else None,
        "archived_at": now.isoformat(),
    }
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    client.put_object(
        MINIO_BUCKET,
        object_key,
        data=io.BytesIO(data),
        length=len(data),
        content_type="application/json"
    )
    log.warning(f"DLQ archived → s3://{MINIO_BUCKET}/{object_key} | reason: {error_reason}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    log.info("Starting DLQ Handler...")
    minio_client = get_minio_client()

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "dlq-handler",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(DLQ_TOPICS)
    log.info(f"Subscribed to DLQ topics: {DLQ_TOPICS}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error(f"Kafka error: {msg.error()}")
                continue

            # Parse error reason from headers
            headers = dict(msg.headers() or [])
            error_reason = (
                headers.get("kafka_dlt-exception-message", b"unknown")
                .decode("utf-8", errors="replace")
            )

            archive_dlq_record(minio_client, msg.topic(), msg.value(), error_reason)
            consumer.commit(asynchronous=False)

    except KeyboardInterrupt:
        log.info("Shutting down DLQ Handler...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
