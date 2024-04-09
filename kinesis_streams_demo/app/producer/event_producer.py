""" Random event data producer module for kinesis streaming demo.
"""

import datetime
import json
import logging
import os
import random
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from typing import Any, List

import boto3

AWS_REGION = os.getenv("AWS_REGION") or "eu-west-1"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY") or "localstack"
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY") or "localstack"
KINESIS_ENDPOINT = os.getenv("KINESIS_ENDPOINT") or "http://localhost:4566"
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME") or "dev-demo-event-stream"
LOG_LEVEL = os.getenv("LOG_LEVEL") or "INFO"

EVENT_NAMES = [
    "account:created",
    "account:removed",
    "lesson:started",
    "lesson:finished",
    "payment:order:created",
    "payment:order:completed",
]

logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)


def prepare_event() -> dict:
    """Prepares a random event data with fields uniquie
    identifier 'event_uuid', evet name 'event_name' and
    creation time of the event 'created_at'

    Returns
    -------
    dict
        created event
    """
    return {
        "event_uuid": str(uuid.uuid4()),
        "event_name": get_random_value(EVENT_NAMES),
        "created_at": datetime.datetime.now().timestamp(),
    }


def get_random_value(values: List[Any]) -> Any:
    """Gets a random element from given list of values

    Parameters
    ----------
    values:  List[Any]
        value list to get one of its elements

    Returns
    -------
    Any
        A random element fro mthe given values
    """
    return values[random.randint(0, len(values) - 1)]


def put_record(event: dict, retry_count: int = 1):
    """Send put_record request to kinesis for given event.
    It tries to send same request max 3 times in case of failure.

    Parameters
    ----------
    event:  dict
        event data
    retry_count:  int, default=1
        retry count of the current put_record request
    """
    try:
        # send event
        logger.info("Sending event: %s, retry_count: %d", event["event_uuid"], retry_count)
        logger.info("Sending event: %s, retry_count: %d", event, retry_count)
        kinesis = boto3.Session(region_name=AWS_REGION).client(
            "kinesis",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            endpoint_url=KINESIS_ENDPOINT,
        )
        kinesis.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=json.dumps(event),
            PartitionKey=event["event_uuid"],
        )
        logger.info("Sent event: %s", event)
    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            "Failed to send event: %s, retry_count: %d, %s error occured: %s",
            event["event_uuid"],
            retry_count,
            type(e),
            e,
        )
        retry_count += 1
        if retry_count <= 3:
            put_record(event, retry_count)
        else:
            logger.error("Failed to send event in all retries: %s", event["event_uuid"])
            logger.error("%s error occured: %s", type(e), e)


def run():
    """Prepares a random event data and send to kinesis data stream."""
    start_time = time.time()
    while time.time() - start_time <= 3600:  # runs producer for 1 hour duration
        event = prepare_event()
        put_record(event)


if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=5) as executor:
        executor.submit(run)
