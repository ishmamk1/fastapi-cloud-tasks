from google.cloud import scheduler_v1
from google.protobuf import duration_pb2, field_mask_pb2
from google.api_core.exceptions import AlreadyExists
import logging
import uuid

from fastapi_cloud_tasks.providers.gcp.utils import map_http_method_to_http_type

# TODO: FIX TO INCLUDE BODY AND ABSTRACT HEADERS
logger = logging.getLogger(__name__)

def gcp_create_scheduler_job(
    *,
    name: str = "",
    schedule: str,
    base_url: str,
    location_path: str,
    client: scheduler_v1.CloudSchedulerClient,
    endpoint_url: str,
    http_method: str,
    timeout: float = 10.0,
    retry_config: scheduler_v1.RetryConfig = None,
    time_zone: str = "UTC",
    headers: dict | None = None,
    body: dict | None = None
):
    if not name:
        unique_id = uuid.uuid4()
        name = f"fastapi-cloud-tasks-job-{unique_id}"
    
    if not retry_config:
        retry_config = _build_default_retry_config()
    
    request = _create_request(http_method, endpoint_url, headers, body)
    job_name = f"{location_path}/jobs/{name}"
    logger.info(job_name, schedule, base_url, location_path, client, endpoint_url, http_method)

    scheduled_job = scheduler_v1.Job(
        name=job_name,
        http_target=request,
        schedule=schedule,
        retry_config=retry_config,
        time_zone=time_zone
    )

    job_request = scheduler_v1.CreateJobRequest(
        parent=location_path,
        job=scheduled_job
    )

    try:
        response = client.create_job(request=job_request, timeout=timeout)
        logger.info(f"Created Cloud Scheduler job: {response.name}")
        return response
    except AlreadyExists:
        logger.info(f"Job {job_name} already exists. Updating instead.")

        update_mask = {"paths": ["schedule", "http_target", "retry_config", "time_zone"]}
        update_request = scheduler_v1.UpdateJobRequest(job=scheduled_job, update_mask=update_mask)
        response = client.update_job(request=update_request, timeout=timeout)

        logger.info(f"Updated Cloud Scheduler job: {response.name}")
        return response

def gcp_update_scheduler_job(
    name: str,
    schedule: str,
    location_path: str,
    client: scheduler_v1.CloudSchedulerClient,
    update_mask: list[str] | None = None,
    **kwargs,
):

    job_name = f"{location_path}/jobs/{name}"

    job = scheduler_v1.Job(name=job_name, schedule=schedule, **kwargs)

    if update_mask is None:
        update_mask = ["schedule"] + list(kwargs.keys())

    mask = field_mask_pb2.FieldMask(paths=update_mask)
    request = scheduler_v1.UpdateJobRequest(job=job, update_mask=mask)

    return client.update_job(request=request)

    
def gcp_delete_scheduler_job(
    name: str,
    location_path: str,
    client: scheduler_v1.CloudSchedulerClient,
    timeout: float | None = None,
    metadata: list | None = None,
):  
    job_name = f"{location_path}/jobs/{name}"

    client.delete_job(
        name=job_name,
        timeout=timeout,
        metadata=metadata or {},
    )


def _build_default_retry_config() -> scheduler_v1.RetryConfig:
    retry_config = scheduler_v1.RetryConfig (
            retry_count=3,
            max_retry_duration = duration_pb2.Duration(seconds=0),
            min_backoff_duration = duration_pb2.Duration(seconds=5),
            max_backoff_duration = duration_pb2.Duration(seconds=60),
            max_doublings=5
        )

    return retry_config
    

def _create_request(http_method: str, endpoint_url: str, headers: dict | None = None, body: dict | None = None) -> scheduler_v1.HttpTarget:
    try:
        request = scheduler_v1.HttpTarget()
        request.http_method = map_http_method_to_http_type(http_method)
        request.uri = endpoint_url
        
        if headers:
            request.headers = headers
        
        if body:
            request.body = body
        
        return request
    except Exception as error:
        raise error