import uuid
import boto3
import json

def aws_schedule_job(
        *,
        name: str= "SchedulerEventBridge",
        endpoint_url: str,
        schedule: str,
        headers: dict | None = None,
        body: dict | None = None, 
        http_method: str,
        lambda_arn: str
    ):
    
    eventbridge_client = boto3.client("events")
    try:
        # Create the scheduled rule
        response = eventbridge_client.put_rule(
            Name= name,
            ScheduleExpression=schedule,  # Every day at 8 AM UTC
            State="ENABLED",
            Description="FastAPI Cloud Tasks Scheduler"
        )

        rule_arn = response["RuleArn"]

        # Define the payload to send to Lambda
        payload = {
            "http_method": http_method,
            "endpoint_url": endpoint_url,
            "body": body or {},
            "headers": headers or {}
        }

        # Attach Lambda as target with input payload
        eventbridge_client.put_targets(
            Rule=name,
            Targets=[{
                "Id": f"LambdaTarget-{uuid.uuid4()}",
                "Arn": lambda_arn,
                "Input": json.dumps(payload)
            }]
        )
    except eventbridge_client.exceptions.ClientError as e:
        print("Error creating EventBridge rule:", e)
        raise
