from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

import json
from datetime import datetime, timezone, timedelta

from fastapi_cloud_tasks.providers.gcp.exceptions import BadMethodException

def gcp_create_delay_task(
    client: tasks_v2.CloudTasksClient,
    queue_path: str,
    endpoint_url: str,
    http_method: str,
    body: dict | None = None,
    delay_seconds: int = 0,
    timeout: float = 10.0,
    headers: dict | None = None
):

    http_request = tasks_v2.HttpRequest(
        url = endpoint_url,
        http_method = _convert_http_method_type(http_method),
        headers = headers
    )

    if body:
        http_request.body = json.dumps(body).encode()
    print(str(delay_seconds) + "seconds")
    scheduled_date = _get_scheduled_delay_date(delay_seconds=delay_seconds)

    delay_task = tasks_v2.Task(
        http_request=http_request,
        schedule_time=scheduled_date
    )

    response = client.create_task(
        task=delay_task,
        parent=queue_path,
        timeout=timeout
    )

    return response


def _get_scheduled_delay_date(delay_seconds: int):
    timestamp = timestamp_pb2.Timestamp()
    current_date = datetime.now(timezone.utc)

    if delay_seconds <= 0:
        timestamp.FromDatetime(current_date)
        return timestamp
    
    scheduled_delay_date = current_date + timedelta(seconds=delay_seconds)
    timestamp.FromDatetime(scheduled_delay_date)  # Mutates timestamp in place
    return timestamp


def _convert_http_method_type(http_method):
    methodMap = {
        "POST": tasks_v2.HttpMethod.POST,
        "GET": tasks_v2.HttpMethod.GET,
        "HEAD": tasks_v2.HttpMethod.HEAD,
        "PUT": tasks_v2.HttpMethod.PUT,
        "DELETE": tasks_v2.HttpMethod.DELETE,
        "PATCH": tasks_v2.HttpMethod.PATCH,
        "OPTIONS": tasks_v2.HttpMethod.OPTIONS,
    }
    # Only crash if we're being bound
    """
    if len(methods) > 1:
        raise BadMethodException("Can't trigger task with multiple methods")
    """
    method = methodMap.get(http_method, None)

    if method is None:
        raise BadMethodException(f"Unknown method {http_method}")
    return method