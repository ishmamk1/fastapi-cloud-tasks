from google.cloud import scheduler_v1
from google.protobuf import duration_pb2
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
    force: bool = False,
    headers: dict | None = None,
):
    if not name:
        unique_id = uuid.uuid4()
        name = f"fastapi-cloud-tasks-job-{unique_id}"
    
    if not retry_config:
        retry_config = _build_default_retry_config()
    
    request = _create_request(http_method, endpoint_url, headers)
    job_name = f"{location_path}/jobs/{name}"
    print(job_name, schedule, base_url, location_path, client, endpoint_url, http_method)

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

    response = client.create_job(request=job_request, timeout=timeout)
    logger.info(f"Created Cloud Scheduler job: {response.name}")
    return response

    if force:
        pass


def _delete_existing_job():
    pass

def _has_job_changed(request: scheduler_v1.CreateJobRequest):
    pass


def _build_default_retry_config() -> scheduler_v1.RetryConfig:
    retry_config = scheduler_v1.RetryConfig (
            retry_count=3,
            max_retry_duration = duration_pb2.Duration(seconds=0),
            min_backoff_duration = duration_pb2.Duration(seconds=5),
            max_backoff_duration = duration_pb2.Duration(seconds=60),
            max_doublings=5
        )

    return retry_config
    

def _create_request(http_method: str, endpoint_url: str, headers: dict | None = None) -> scheduler_v1.HttpTarget:
    try:
        request = scheduler_v1.HttpTarget()
        request.http_method = map_http_method_to_http_type(http_method)
        request.uri = endpoint_url
        
        if headers:
            request.headers = headers
        
        return request
    except Exception as error:
        raise error