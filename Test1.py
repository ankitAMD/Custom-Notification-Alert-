#Version1 

import os
import time
from datetime import datetime, timedelta

import boto3

sns_arn = os.environ["SNS_ARN"]



def account_alias():
    return boto3.client("sts").get_caller_identity()["Account"]


def retrieve_events():
    client = boto3.client("logs")

    query = f"""fields @timestamp, @ingestionTime, @logStream, @log, @message
| sort @timestamp desc
| limit 100"""

    print(query)

    log_group = "demo-ec2-apache-logs"
    timeout = 300
    timeout_start = time.time()
    response = {}

    while len(response.get("results", {})) < 1 and (
        time.time() < timeout_start + timeout
    ):
        start_query_response = client.start_query(
            logGroupName=log_group,
            startTime=int((datetime.today() - timedelta(minutes=2)).timestamp()),
            endTime=int(datetime.now().timestamp()),
            queryString=query,
        )

        query_id = start_query_response["queryId"]

        response = {}

        while response == {} or response["status"] == "Running":
            print("Waiting for query to complete ...")
            time.sleep(1)
            response = client.get_query_results(queryId=query_id)

        print("Gathered results from CloudWatch !")
        print(f"Found {len(response.get('results', {}))} results")

    parsed_response = []
    for result in range(len(response["results"])):
        element = {
            "timestamp": response["results"][result][0]["value"],
            "username": response["results"][result][1]["value"],
            "action": response["results"][result][2]["value"],
            "service": response["results"][result][3]["value"],
        }

        parsed_response.append(element)

    return parsed_response


def lambda_handler(event, context):
    print(event)
    print(context)

    client = boto3.client("sns")
    retrieved_events = retrieve_events()

    message = "Greetings from IAC_Enforcement Lambda ! \n \n"
    message += f"On the account : {account_alias()} \n"
    message += f"We have detected {len(retrieved_events)} custom manual modification change within last 2 minutes: \n"
    for event in retrieved_events:
        message += str(event)
        message += "\n"

    message += "\n \n"
    message += "Please take appropriate actions. You can also access the iac_enforcement for additional details. \n \n"
    message += f"Email Generated: {str(datetime.today())}"
    resp = client.publish(
        TargetArn=sns_arn,
        Message=message,
        Subject=f"IAC Enforcement Alert for account {account_alias()}",
    )


if __name__ == "__main__":
    lambda_handler(event="", context="")
