import io
import json
import struct
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
def make_avro_bytes(schema_id: int, avro_payload: bytes) -> bytes:
    """Avro wire format: 0x00 + 4-byte schema_id (big-endian) + payload"""
    return b"\x00" + struct.pack(">I", schema_id) + avro_payload


def make_json_envelope(op: str, after: dict = None, before: dict = None) -> bytes:
    return json.dumps({"op": op, "after": after, "before": before}).encode()


# ---------------------------------------------------------------------------
# parse_debezium_message tests
# ---------------------------------------------------------------------------
class TestParseDebeziumMessage:

    def test_returns_none_for_none_value(self):
        from bronze_writer import parse_debezium_message
        assert parse_debezium_message(None) is None

    def test_parses_read_op(self):
        from bronze_writer import parse_debezium_message
        raw = make_json_envelope("r", after={"id": 1, "name": "Alice"})
        result = parse_debezium_message(raw)
        assert result["id"] == 1
        assert result["name"] == "Alice"
        assert result["_op"] == "r"
        assert "_ingested_at" in result

    def test_parses_insert_op(self):
        from bronze_writer import parse_debezium_message
        raw = make_json_envelope("c", after={"id": 2, "name": "Bob"})
        result = parse_debezium_message(raw)
        assert result["_op"] == "c"

    def test_parses_update_op(self):
        from bronze_writer import parse_debezium_message
        raw = make_json_envelope("u", after={"id": 3, "name": "Updated"})
        result = parse_debezium_message(raw)
        assert result["_op"] == "u"
        assert result["name"] == "Updated"

    def test_parses_delete_op_uses_before(self):
        from bronze_writer import parse_debezium_message
        raw = make_json_envelope("d", before={"id": 4, "name": "Deleted"})
        result = parse_debezium_message(raw)
        assert result["_op"] == "delete"
        assert result["id"] == 4

    def test_returns_none_for_invalid_json(self):
        from bronze_writer import parse_debezium_message
        result = parse_debezium_message(b"not-valid-json-or-avro")
        assert result is None

    def test_ingested_at_is_iso_format(self):
        from bronze_writer import parse_debezium_message
        raw = make_json_envelope("r", after={"id": 1})
        result = parse_debezium_message(raw)
        # Should not raise
        datetime.fromisoformat(result["_ingested_at"])


# ---------------------------------------------------------------------------
# flush_to_minio tests
# ---------------------------------------------------------------------------
class TestFlushToMinio:

    def test_flush_uploads_parquet(self):
        from bronze_writer import flush_to_minio

        mock_client = MagicMock()
        records = [{"id": i, "name": f"User{i}", "_op": "r"} for i in range(10)]

        flush_to_minio(mock_client, "CUSTOMERS", records)

        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args
        assert "bronze/customers/" in call_kwargs[0][1]
        assert call_kwargs[0][0] == "datalake"

    def test_flush_skips_empty_records(self):
        from bronze_writer import flush_to_minio

        mock_client = MagicMock()
        flush_to_minio(mock_client, "CUSTOMERS", [])
        mock_client.put_object.assert_not_called()

    def test_flush_object_key_contains_table_name(self):
        from bronze_writer import flush_to_minio

        mock_client = MagicMock()
        records = [{"id": 1, "_op": "r"}]
        flush_to_minio(mock_client, "ORDERS", records)

        object_key = mock_client.put_object.call_args[0][1]
        assert "orders" in object_key

    def test_flush_object_key_contains_parquet_extension(self):
        from bronze_writer import flush_to_minio

        mock_client = MagicMock()
        records = [{"id": 1, "_op": "r"}]
        flush_to_minio(mock_client, "PRODUCTS", records)

        object_key = mock_client.put_object.call_args[0][1]
        assert object_key.endswith(".parquet")


# ---------------------------------------------------------------------------
# dlq_handler tests
# ---------------------------------------------------------------------------
class TestDlqHandler:

    def test_archive_dlq_record_uploads_json(self):
        from dlq_handler import archive_dlq_record

        mock_client = MagicMock()
        archive_dlq_record(
            mock_client,
            topic="dlq.cdc.APP.ORDERS",
            raw_value=b'{"bad": "data"}',
            error_reason="ParseError: invalid schema"
        )

        mock_client.put_object.assert_called_once()
        object_key = mock_client.put_object.call_args[0][1]
        assert "dlq/orders/" in object_key
        assert object_key.endswith(".json")

    def test_archive_dlq_record_contains_error_reason(self):
        from dlq_handler import archive_dlq_record

        mock_client = MagicMock()
        archive_dlq_record(
            mock_client,
            topic="dlq.cdc.APP.CUSTOMERS",
            raw_value=b"raw",
            error_reason="NullPointerException"
        )

        data_arg = mock_client.put_object.call_args[1]["data"]
        content = json.loads(data_arg.read())
        assert content["error_reason"] == "NullPointerException"
        assert content["topic"] == "dlq.cdc.APP.CUSTOMERS"

    def test_archive_dlq_handles_none_value(self):
        from dlq_handler import archive_dlq_record

        mock_client = MagicMock()
        # Should not raise
        archive_dlq_record(mock_client, "dlq.cdc.APP.PRODUCTS", None, "empty message")
        mock_client.put_object.assert_called_once()
