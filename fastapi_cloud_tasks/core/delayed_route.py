from typing import Callable, Type

from fastapi import Request, Response
from fastapi.routing import APIRoute
from google.cloud import tasks_v2

from fastapi_cloud_tasks.providers.gcp.utils import validate_queue

class NormalRoute(APIRoute):
    def get_route_handler(self) -> Callable:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            endpoint_url = str(request.url)
            print(f"Endpoint url: {endpoint_url}")
            response: Response = await original_route_handler(request)
            return response

        return custom_route_handler

def DelayedRouteBuilder(
    *,
    base_url: str,
    queue_path: str,
    task_creation_timeout: float = 10.0,
    client: tasks_v2.CloudTasksClient | None = None,
    auto_create_queue: bool = True,
) -> Type[APIRoute]:
    
    if client is None:
        client = tasks_v2.CloudTasksClient()
    
    if auto_create_queue:
        validate_queue(client=client, queue_path=queue_path)
    
    class DelayedRoute(APIRoute):
        def get_route_handler(self) -> Callable:
            original_route_handler = super().get_route_handler()

            async def custom_route_handler(request: Request) -> Response:
                endpoint_url = str(request.url)
                print(f"Endpoint url: {endpoint_url}")
                response: Response = await original_route_handler(request)
                return response

            return custom_route_handler

    return DelayedRoute