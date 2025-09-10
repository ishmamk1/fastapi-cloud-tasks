from typing import Callable, Type

from fastapi import Request, Response
from fastapi.routing import APIRoute
from google.cloud import tasks_v2

from fastapi_cloud_tasks.providers.gcp.utils import validate_queue
from fastapi_cloud_tasks.providers.gcp.delayer import gcp_create_delay_task

import boto3
from fastapi_cloud_tasks.providers.aws.delayer import aws_create_delay_task

from fastapi_cloud_tasks.providers.aws.utils import create_sqs_lambda_role, deploy_lambda, link_lambda_sqs, create_sqs_queue

import logging

logger = logging.getLogger(__name__)

def GCPDelayedRouteBuilder(
    *,
    base_url: str,
    queue_path: str,
    client: tasks_v2.CloudTasksClient | None = None,
    auto_create_queue: bool = True,
) -> Type[APIRoute]:
    
    client = client or tasks_v2.CloudTasksClient()
    
    if auto_create_queue:
        validate_queue(client=client, queue_path=queue_path)
    
    class DelayedRoute(APIRoute):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.base_url = base_url
            self.queue_path = queue_path
            self.client = client
            self.url_endpoint = f"{self.base_url}{self.path}"

        def get_route_handler(self) -> Callable:
            original_route_handler = super().get_route_handler()
            self.endpoint.delay = self.delay

            async def custom_route_handler(request: Request) -> Response:
                endpoint_url = str(request.url)
                print(f"Endpoint url: {endpoint_url}")
                response: Response = await original_route_handler(request)
                return response

            return custom_route_handler

        
        def delay(self, delay_seconds: int = 0, timeout_seconds: float = 10.0, body: dict | None = None, headers: dict | None = None):
            try:
                http_method = list(self.methods)[0] if self.methods else "POST"

                gcp_create_delay_task(
                    client=self.client,
                    queue_path=self.queue_path,
                    endpoint_url=f"{self.base_url}{self.path}",
                    body=body or {},
                    delay_seconds=delay_seconds,
                    timeout=timeout_seconds,
                    http_method=http_method,
                    headers=headers or {}
                )
            except Exception as exc:
                logger.exception("Failed to enqueue Cloud Task: %s", exc)

    return DelayedRoute

def AWSDelayedRouteBuilder(
    *,
    base_url: str,
    sqs_client=None,
    lambda_client=None,
) -> Type[APIRoute]:
    
    sqs_client = sqs_client or boto3.client("sqs")
    lambda_client = lambda_client or boto3.client("lambda")

    class DelayedRoute(APIRoute):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.base_url = base_url
            self.sqs_client = sqs_client
            self.lambda_client = lambda_client
            self.url_endpoint = f"{self.base_url}{self.path}"
            self.role_arn = create_sqs_lambda_role()
            self.lambda_arn = deploy_lambda(function_name="DelayerLambda", role_arn=self.role_arn)
            self.queue_url = create_sqs_queue(queue_name="Delay-Queue")

            link_lambda_sqs(lambda_arn=self.lambda_arn, queue_url=self.queue_url)
        
        def get_route_handler(self) -> Callable:
            original_route_handler = super().get_route_handler()
            self.endpoint.delay = self.delay

            async def custom_route_handler(request: Request) -> Response:
                endpoint_url = str(request.url)
                print(f"Endpoint url: {endpoint_url}")
                response: Response = await original_route_handler(request)
                return response

            return custom_route_handler

        
        def delay(self, delay_seconds: int = 0, body: dict | None = None, headers: dict | None = None):
            try:
                http_method = list(self.methods)[0] if self.methods else "POST"

                aws_create_delay_task(
                    sqs_client=sqs_client,
                    lambda_client=lambda_client,
                    endpoint_url=self.url_endpoint,
                    body=body or {},
                    delay_seconds=delay_seconds,
                    http_method=http_method,
                    headers=headers or {},
                    role_arn=self.role_arn,
                    lambda_arn=self.lambda_arn,
                    queue_url=self.queue_url
                )
            except Exception as exc:
                logger.exception("Failed to enqueue Cloud Task: %s", exc)

    return DelayedRoute