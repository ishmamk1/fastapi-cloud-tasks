from typing import Callable, Type

from fastapi import Request, Response
from fastapi.routing import APIRoute
from google.cloud import tasks_v2

from fastapi_cloud_tasks.providers.gcp.utils import validate_queue
from fastapi_cloud_tasks.providers.gcp.delayer import gcp_create_delay_task

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

        
        def delay(self, delay_seconds: int = 0, timeout_seconds: float = 10.0, body: dict | None = None):
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
                    headers={ 
                        "Content-Type": "application/json",
                        "ngrok-skip-browser-warning": "true"
                    }
                )
            except Exception as exc:
                logger.exception("Failed to enqueue Cloud Task: %s", exc)

    return DelayedRoute