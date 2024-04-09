"""Lambda function to process and transform kinesis firehose stream data.
"""

import base64
import json
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel("INFO")


UTF_8 = "utf-8"


def lambda_handler(event, context):  # pylint: disable=unused-argument
    """Process and adds 'created_datetime', 'event_type'
    and 'event_subtype' additional fields into event

    Parameters
    ----------
    event:
        current event to process
    context: lambda function context

    Returns
    -------
    Processed event.
    """
    output = []

    for record in event.get("records"):
        logger.info("Processing record: %s", str(record.get("recordId")))
        payload = base64.b64decode(record.get("data")).decode(UTF_8)
        payload = json.loads(payload)
        logger.info(
            "Processing record: %s, event_uuid: %s",
            str(record.get("recordId")),
            payload.get("event_uuid"),
        )

        # add extra fields
        created_time = datetime.fromtimestamp(int(payload.get("created_at")))
        payload["created_datetime"] = created_time.isoformat()
        event_type = ""
        event_subtype = ""
        if ":" in payload["event_name"]:
            splitted_event_name = payload.get("event_name").split(":")
            event_type = splitted_event_name[0]
            if len(splitted_event_name) > 2:
                event_subtype = splitted_event_name[1]
        payload["event_type"] = event_type
        payload["event_subtype"] = event_subtype

        # prepare partition key
        partition_keys = {
            "year": created_time.year,
            "month": created_time.month,
            "day": created_time.day,
            "hour": created_time.hour,
        }

        # append procesed event to result
        output_record = {
            "recordId": record.get("recordId"),
            "result": "Ok",
            "data": base64.b64encode(json.dumps(payload).encode(UTF_8)).decode(UTF_8),
            "metadata": {"partitionKeys": partition_keys},
        }
        output.append(output_record)
        logger.debug(
            "Processed record: %s, event_uuid: %s",
            str(record.get("recordId")),
            payload.get("event_uuid"),
        )

    logger.info("Successfully processed %d records.", len(event.get("records")))
    return {"records": output}
