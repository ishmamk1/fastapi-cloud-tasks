import boto3
import json

import io, zipfile, boto3
from pathlib import Path

import importlib.resources as pkg_resources
from typing import Dict

def package_lambda_code() -> bytes:
    """
    Packages delay_handler.py into a zip file in memory.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Read the lambda handler from the package
        with pkg_resources.open_binary("fastapi_cloud_tasks.providers.aws.resources", "delay_handler.py") as f:
            z.writestr("delay_handler.py", f.read())
    buffer.seek(0)
    return buffer.read()

def deploy_lambda(*, function_name: str = "DelayerLambda", role_arn: str, extra_env: Dict = None):
    """
    Deploys the packaged lambda to AWS.
    """
    lambda_client = boto3.client("lambda")
    iam = boto3.client("iam")
    code_bytes = package_lambda_code()

    try:
        response = lambda_client.create_function(
            FunctionName=function_name,
            Runtime="python3.11",
            Role=role_arn,
            Handler="delay_handler.lambda_handler",
            Code={"ZipFile": code_bytes},
            Environment={"Variables": extra_env or {}},
        )
        lambda_arn = response['Configuration']['FunctionArn']
        print(lambda_arn)
        return lambda_arn

    except lambda_client.exceptions.ResourceConflictException:
        lambda_arn = lambda_client.get_function(FunctionName=function_name)['Configuration']['FunctionArn']
        print(lambda_arn)
        return lambda_arn

def link_lambda_sqs(lambda_arn: str, queue_url: str):
    lambda_client = boto3.client("lambda")
    sqs_client = boto3.client("sqs")

    queue_arn = sqs_client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    try:
        lambda_client.create_event_source_mapping(
            EventSourceArn=queue_arn,
            FunctionName=lambda_arn,
            Enabled=True,
            BatchSize=10
        )
    except lambda_client.exceptions.ResourceConflictException:
        print("EventSource connection exists.")

def create_sqs_queue(queue_name: str):
    sqs_client = boto3.client("sqs")

    sqs_response = sqs_client.create_queue(
        QueueName=queue_name,
    )

    queue_url = sqs_response['QueueUrl']
    print("Queue URL", queue_url)
    return queue_url

def create_sqs_lambda_role(role_name="SqsLambdaRole"):
    iam = boto3.client("iam")

    assume_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }

    try:
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_policy)
        )
        role_arn = response["Role"]["Arn"]
    except iam.exceptions.EntityAlreadyExistsException:
        response = iam.get_role(RoleName=role_name)
        role_arn = response["Role"]["Arn"]

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sqs:SendMessage",
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                    "lambda:CreateFunction",
                    "lambda:InvokeFunction",
                    "lambda:GetFunction",
                    "iam:PassRole"
                ],
                "Resource": "*"
            }
        ]
    }

    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="SqsLambdaFullAccessPolicy",
        PolicyDocument=json.dumps(policy)
    )

    return role_arn

def create_scheduler_role(role_name="EventBridgeSchedulerRole"):
    iam = boto3.client("iam")

    assume_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "scheduler.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }

    try:
        response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_policy)
        )
        return response["Role"]["Arn"]
    except iam.exceptions.EntityAlreadyExistsException:
        response = iam.get_role(RoleName=role_name)
        return response["Role"]["Arn"]
