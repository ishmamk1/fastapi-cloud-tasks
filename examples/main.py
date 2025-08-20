from fastapi import FastAPI, APIRouter
from fastapi_cloud_tasks.core.hello_route import HelloRoute

from fastapi_cloud_tasks.core.delayed_route import DelayedRouteBuilder
from google.cloud import tasks_v2


app = FastAPI()

queue_path = "projects/fastapi-cloud-tasks/locations/us-east4/queues/tests"
base_url = "BASE_URL"

DelayedRoute = DelayedRouteBuilder(
    base_url=base_url,
    queue_path=queue_path,
    auto_create_queue=True,
    client=tasks_v2.CloudTasksClient()
)

hello_router = APIRouter(route_class=HelloRoute)
delayed_router = APIRouter(route_class=DelayedRoute)

@hello_router.get("/hello")
async def timed():
    return {"message": "It's the time of my life"}

@delayed_router.post("/delay")
async def delay_route():
    print("Task received:")
    return {"message": "delay"}

@app.get("/trigger")
async def test():
    delay_route.delay(5)
    return 

app.include_router(hello_router)
app.include_router(delayed_router)