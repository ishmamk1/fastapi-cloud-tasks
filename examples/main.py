from fastapi import FastAPI, APIRouter
from fastapi_cloud_tasks.hello_route import HelloRoute

from fastapi_cloud_tasks.delayed_route import GCPDelayedRouteBuilder
from fastapi_cloud_tasks.scheduled_route import GCPScheduleRouteBuilder
from google.cloud import tasks_v2, scheduler_v1

import time

app = FastAPI()

queue_path = "projects/fastapi-cloud-tasks/locations/us-east4/queues/tests"
base_url = "https://cce0661f8962.ngrok-free.app"
location_path = "projects/fastapi-cloud-tasks/locations/us-east4"

DelayedRoute = GCPDelayedRouteBuilder(
    base_url=base_url,
    queue_path=queue_path,
    auto_create_queue=True,
    client=tasks_v2.CloudTasksClient()
)

ScheduledRoute = GCPScheduleRouteBuilder(
    base_url=base_url,
    location_path=location_path,
    client=scheduler_v1.CloudSchedulerClient(),
)

hello_router = APIRouter(route_class=HelloRoute)
delayed_router = APIRouter(route_class=DelayedRoute)
scheduled_route = APIRouter(route_class=ScheduledRoute)

@hello_router.get("/hello")
async def timed():
    return {"message": "It's the time of my life"}

@delayed_router.post("/delay")
async def delay_route():
    print("Task received:")
    return {"message": "delay"}

@scheduled_route.post("/schedule")
async def schedule_route():
    print("Scheduled!")
    return {"message": "schedule"}

@app.get("/schedule_trigger")
async def schedule_trigger():
    headers = { "Content-Type": "application/json", "ngrok-skip-browser-warning": "true" }
    print("scheduling")
    time.sleep(2)
    schedule_route.schedule(name="test", schedule="* * * * *", headers=headers)
    print("updating")
    time.sleep(2)
    schedule_route.update_schedule(name="test", schedule="9 * * * *")
    print("deleting")
    schedule_route.delete_schedule(name="test")
    return

@app.get("/trigger")
async def test():
    delay_route.delay(5)
    return 

app.include_router(hello_router)
app.include_router(delayed_router)
app.include_router(scheduled_route)