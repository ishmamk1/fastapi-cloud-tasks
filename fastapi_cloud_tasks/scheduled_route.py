from typing import Callable, Type

from fastapi import Request, Response
from fastapi.routing import APIRoute
from google.cloud import scheduler_v1

from fastapi_cloud_tasks.providers.gcp.scheduler import gcp_create_scheduler_job, gcp_update_scheduler_job, gcp_delete_scheduler_job

from fastapi_cloud_tasks.providers.aws.utils import deploy_lambda, create_aws_cloud_tasks_role
from fastapi_cloud_tasks.providers.aws.scheduler import aws_schedule_job

import logging

logger = logging.getLogger(__name__)

def GCPScheduleRouteBuilder(
    base_url: str,
    location_path: str,
    job_create_timeout: float = 10.0,
    client: scheduler_v1.CloudSchedulerClient | None = None
) -> Type[APIRoute]:
    
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
            self.endpoint.update_schedule = self.update_schedule_job
            self.endpoint.delete_schedule = self.delete_schedule_job

            async def custom_route_handler(request: Request) -> Response:
                endpoint_url = str(request.url)
                print(f"Endpoint url: {endpoint_url}")
                response: Response = await original_route_handler(request)
                return response

            return custom_route_handler

        def schedule(
            self,
            *,
            name: str = "",
            schedule: str,
            client = client,
            timezone: str = "UTC",
            retry_config: scheduler_v1.RetryConfig = None,
            headers: dict | None = None,
            body: dict | None = None,  # <-- allow body here
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
                headers=headers,
                body=body,
                retry_config=retry_config
            )

        def update_schedule_job(
            self,
            *,
            name: str,
            client: scheduler_v1.CloudSchedulerClient = client,
            schedule: str,
            update_mask: list[str] | None = None,
            **kwargs,
        ):  
            if update_mask is None:
                update_mask = ["schedule"] + list(kwargs.keys())
            gcp_update_scheduler_job(
                name=name,
                client = client,
                schedule=schedule,
                update_mask=update_mask,
                location_path=self.location_path,
                **kwargs
            )
        
        def delete_schedule_job(
            self,
            *,
            name,
            client = client,
        ):
            gcp_delete_scheduler_job(
                name=name,
                client=client,
                location_path=self.location_path
            )

    return ScheduleRoute

def AWSScheduleRouteBuilder(
    base_url: str,
) -> Type[APIRoute]:

    class ScheduleRoute(APIRoute):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.base_url = base_url
            self.endpoint_url = f"{self.base_url}{self.path}"
            self.http_method = list(self.methods)[0] if self.methods else "POST"
            self.role_arn = create_aws_cloud_tasks_role()
            self.lambda_arn = deploy_lambda(function_name="DelayerLambda", role_arn=self.role_arn)
        
        def get_route_handler(self) -> Callable:
            original_route_handler = super().get_route_handler()
            self.endpoint.schedule = self.schedule
            self.endpoint.update_schedule = self.update_schedule_job
            self.endpoint.delete_schedule = self.delete_schedule_job

            async def custom_route_handler(request: Request) -> Response:
                endpoint_url = str(request.url)
                print(f"Endpoint url: {endpoint_url}")
                response: Response = await original_route_handler(request)
                return response

            return custom_route_handler

        def schedule(
            self,
            *,
            name: str = "",
            schedule: str,
            headers: dict | None = None,
            body: dict | None = None,
        ):
            aws_schedule_job(
                name=name,
                endpoint_url=self.endpoint_url,
                schedule=schedule,
                headers=headers,
                body=body, 
                http_method=self.http_method,
                lambda_arn=self.lambda_arn
            )

        def update_schedule_job(
            self,
            *,
            name: str,
            schedule: str,
            update_mask: list[str] | None = None,
            **kwargs,
        ):  
            pass
        
        def delete_schedule_job(
            self,
            *,
            name,
        ):
            pass

    return ScheduleRoute