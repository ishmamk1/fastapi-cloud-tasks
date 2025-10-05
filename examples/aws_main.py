from fastapi import FastAPI, APIRouter
from fastapi_cloud_tasks.hello_route import HelloRoute

from fastapi_cloud_tasks.delayed_route import AWSDelayedRouteBuilder
from fastapi_cloud_tasks.scheduled_route import AWSScheduleRouteBuilder

import boto3

import time

app = FastAPI()

base_url = "https://beeff23ad609.ngrok-free.app"

DelayedRoute = AWSDelayedRouteBuilder(
    base_url=base_url
)

ScheduledRoute = AWSScheduleRouteBuilder(
    base_url=base_url
)

hello_router = APIRouter(route_class=HelloRoute)
delayed_router = APIRouter(route_class=DelayedRoute)
schedule_router = APIRouter(route_class=ScheduledRoute)

@hello_router.get("/hello")
async def timed():
    return {"message": "It's the time of my life"}

@delayed_router.post("/delay")
async def delay_route():
    print("Task received:")
    return {"message": "delay"}

@app.get("/trigger")
async def test():
    headers = { 
        "Content-Type": "application/json",
        "ngrok-skip-browser-warning": "true"
    }
    delay_route.delay(5, headers=headers)
    return 

@schedule_router.post("/schedule")
async def schedule_route():
    print("Task received:")
    return {"message": "schedule"}

@app.get("/schedule_trigger")
async def test():
    headers = { 
        "Content-Type": "application/json",
        "ngrok-skip-browser-warning": "true"
    }
    schedule_route.schedule(name="Schedule", schedule="cron(* * * * ? *)", headers=headers)
    return 


app.include_router(schedule_router)
app.include_router(hello_router)
app.include_router(delayed_router)