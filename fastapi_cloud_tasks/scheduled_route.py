from typing import Callable, Type

from fastapi import Request, Response
from fastapi.routing import APIRoute
from google.cloud import scheduler_v1

from fastapi_cloud_tasks.providers.gcp.utils import validate_queue
from fastapi_cloud_tasks.providers.gcp.delayer import gcp_create_delay_task
from fastapi_cloud_tasks.providers.gcp.scheduler import gcp_create_scheduler_job

import logging

logger = logging.getLogger(__name__)
# TODO: FIX TO INCLUDE BODY AND ABSTRACT HEADERS
def GCPScheduleRouteBuilder(
    base_url: str,
    location_path: str,
    job_create_timeout: float = 10.0,
    client: scheduler_v1.CloudSchedulerClient | None = None
) -> Type[APIRoute]:
    
    # validate params
    client = client or scheduler_v1.CloudSchedulerClient()

    
    
    class ScheduleRoute(APIRoute):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.base_url = base_url
            self.location_path = location_path
            self.client = client
            self.endpoint_url = f"{self.base_url}{self.path}"
            self.http_method = list(self.methods)[0] if self.methods else "POST"
        
        def get_route_handler(self) -> Callable:
            original_route_handler = super().get_route_handler()
            self.endpoint.schedule = self.schedule

            async def custom_route_handler(request: Request) -> Response:
                endpoint_url = str(request.url)
                print(f"Endpoint url: {endpoint_url}")
                response: Response = await original_route_handler(request)
                return response

            return custom_route_handler

        # create schedule function
        def schedule(
            self, *,
            name: str = "",
            schedule: str,
            client = client,
            timezone: str = "UTC",
            retry_config: scheduler_v1.RetryConfig = None,
            force: bool = False
        ):
            gcp_create_scheduler_job(
                name=name,
                schedule=schedule,
                client=client,
                location_path=self.location_path,
                endpoint_url=self.endpoint_url,
                base_url=self.base_url,
                http_method=self.http_method,
                timeout=job_create_timeout,
                time_zone=timezone,
                force=force,
                headers={ 
                    "Content-Type": "application/json",
                    "ngrok-skip-browser-warning": "true"
                },
                retry_config=retry_config
            )


    return ScheduleRoute