import base64
import json

from app.consumer.lambda_function import lambda_handler

test_data = {
    "event_uuid": "78d1b928-37e0-41a0-bf41-ea9d86b27e5a",
    "event_name": "payment:order:completed",
    "created_at": 1712511721.022075,
}
test_records = {
    "records": [{"recordId": "test-record-id", "data": base64.b64encode(json.dumps(test_data).encode("utf-8"))}]
}


def test_created_datetime():
    result = lambda_handler(test_records, None)

    assert len(result["records"]) == 1

    result = result["records"][0]
    assert result["recordId"] == "test-record-id"

    result = json.loads(base64.b64decode(result["data"]).decode("utf-8"))
    assert result["event_uuid"] == test_data["event_uuid"]
    assert result["event_name"] == test_data["event_name"]
    assert result["created_at"] == test_data["created_at"]
    assert result["created_datetime"] == "2024-04-07T19:42:01"


def test_event_type_with_subtype():
    result = lambda_handler(test_records, None)

    assert len(result["records"]) == 1

    result = result["records"][0]
    assert result["recordId"] == "test-record-id"

    result = json.loads(base64.b64decode(result["data"]).decode("utf-8"))
    assert result["event_uuid"] == test_data["event_uuid"]
    assert result["event_name"] == test_data["event_name"]
    assert result["created_at"] == test_data["created_at"]
    assert result["created_datetime"] == "2024-04-07T19:42:01"
    assert result["event_type"] == "payment"
    assert result["event_subtype"] == "order"


def test_event_type_without_subtype():
    test_data = {
        "event_uuid": "78d1b928-37e0-41a0-bf41-ea9d86b27e5a",
        "event_name": "account:created",
        "created_at": 1712511721.022075,
    }
    test_records = {
        "records": [{"recordId": "test-record-id", "data": base64.b64encode(json.dumps(test_data).encode("utf-8"))}]
    }

    result = lambda_handler(test_records, None)
    assert len(result["records"]) == 1

    result = result["records"][0]
    assert result["recordId"] == "test-record-id"

    result = json.loads(base64.b64decode(result["data"]).decode("utf-8"))
    assert result["event_uuid"] == test_data["event_uuid"]
    assert result["event_name"] == test_data["event_name"]
    assert result["created_at"] == test_data["created_at"]
    assert result["created_datetime"] == "2024-04-07T19:42:01"
    assert result["event_type"] == "account"
    assert result["event_subtype"] == ""


def test_partition_key():
    result = lambda_handler(test_records, None)

    assert len(result["records"]) == 1

    result = result["records"][0]
    assert result["recordId"] == "test-record-id"

    assert result["metadata"]["partitionKeys"] == {
        "year": 2024,
        "month": 4,
        "day": 7,
        "hour": 19,
    }
