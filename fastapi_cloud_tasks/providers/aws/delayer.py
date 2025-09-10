import boto3
import uuid
from datetime import datetime, timedelta, timezone
from fastapi.routing import APIRoute
from fastapi import Request, Response
from typing import Callable, Type, Optional, Dict, Any
import logging
import json
import boto3

from fastapi_cloud_tasks.providers.aws.utils import deploy_lambda

logger = logging.getLogger(__name__)

def aws_create_delay_task(
    sqs_client,
    lambda_client,
    role_arn,
    lambda_arn,
    queue_url,
    endpoint_url: str,
    body: Dict[str, Any],
    delay_seconds: int,
    http_method: str = "POST",
    headers: Optional[Dict[str, str]] = None
):
    # create queue
    
    
    # create message with http request info
    message_payload = {
        "endpoint_url": endpoint_url,
        "http_method": http_method,
        "headers": headers or {},
        "body": body
    }

    # send message with per-message delay
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message_payload),
        DelaySeconds=delay_seconds
    )

    print(f"Message pushed with {delay_seconds}s delay")


def create_eventbridge_schedule(role_arn: str, delay_seconds):
    pass

def create_api_destination(
        *,
        api_destination_name: str | None = None,
        endpoint_url: str, 
        http_method: str,
    ):

    client = boto3.client('events')

    unique_id = uuid.uuid4().hex[:8]
    api_destination_name = api_destination_name or f"DelayTask-{unique_id}"

    connection_name = f"DelayTaskConnection-{unique_id}"

    conn = client.create_connection(
            Name=connection_name,
            AuthorizationType="NO_AUTH",
            AuthParameters={}
    )
    connection_arn = conn["ConnectionArn"]

    response = client.create_api_destination(
        Name=api_destination_name,
        InvocationEndpoint=endpoint_url,
        HttpMethod=http_method,
        ConnectionArn=connection_arn,
        InvocationRateLimitPerSecond=5
    )
    api_destination_arn = response["ApiDestinationArn"]
    return api_destination_arn







    

    


    
