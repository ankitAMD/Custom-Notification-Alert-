#Version2

import os
import time
from datetime import datetime, timedelta

import boto3

sns_arn = os.environ["SNS_ARN"]

def account_alias():
    return boto3.client("sts").get_caller_identity()["Account"]

def retrieve_events():
    client = boto3.client("logs")

    query = """fields @timestamp, @ingestionTime, @logStream, @log, @message
          | parse @message /^(?<ip>\S+) \S+ \S+ \[(?<date>[^\:]+):(?<time>[^\]]+)\] "(?<method>\S+) (?<url>\S+) (?<protocol>\S+)" (?<status>\d+) (?<bytes>\d+) "(?<referrer>[^"]*)" "(?<userAgent>[^"]*)"$/
          | display ip, date, time, method, url, protocol, status, bytes, referrer, userAgent
          | sort @timestamp desc
          | limit 100"""

    print(query)

    log_group = "demo-ec2-apache-logs"
    timeout = 800
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

        print("Gathered results from CloudWatch!")
        print(f"Found {len(response.get('results', {}))} results")

    parsed_response = []

    for result in response["results"]:
        element = {
            "timestamp": next((field["value"] for field in result if field["field"] == "@timestamp"), None),
            "ingestionTime": next((field["value"] for field in result if field["field"] == "@ingestionTime"), None),
            "logStream": next((field["value"] for field in result if field["field"] == "@logStream"), None),
            "log": next((field["value"] for field in result if field["field"] == "@log"), None),
            "ip": next((field["value"] for field in result if field["field"] == "ip"), None),
            "date": next((field["value"] for field in result if field["field"] == "date"), None),
            "time": next((field["value"] for field in result if field["field"] == "time"), None),
            "method": next((field["value"] for field in result if field["field"] == "method"), None),
            "url": next((field["value"] for field in result if field["field"] == "url"), None),
            "protocol": next((field["value"] for field in result if field["field"] == "protocol"), None),
            "status": next((field["value"] for field in result if field["field"] == "status"), None),
            "bytes": next((field["value"] for field in result if field["field"] == "bytes"), None),
            "referrer": next((field["value"] for field in result if field["field"] == "referrer"), None),
            "userAgent": next((field["value"] for field in result if field["field"] == "userAgent"), None),
        }

        parsed_response.append(element)

    return parsed_response

def lambda_handler(event, context):
    print(event)
    print(context)

    client = boto3.client("sns")
    retrieved_events = retrieve_events()

    message = "Greetings from notification_lambda!\n\n"
    message += f"On the account: {account_alias()}\n"
    message += f"We have detected {len(retrieved_events)} custom manual modification changes within the last 2 minutes:\n"
    for event in retrieved_events:
        message += str(event)
        message += "\n"

    message += "\n\n"
    message += "Please take appropriate actions. You can also access the demo-ec2-apache-logs for additional details.\n\n"
    message += f"Email Generated: {str(datetime.today())}"
    resp = client.publish(
        TargetArn=sns_arn,
        Message=message,
        Subject=f"IAC Enforcement Alert for account {account_alias()}",
    )

if __name__ == "__main__":
    lambda_handler(event="", context="")
