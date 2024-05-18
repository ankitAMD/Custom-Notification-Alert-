#Version4

 #### ----CODE RUNNED

import os
import time
from datetime import datetime, timedelta

import boto3

sns_arn = os.environ["SNS_ARN"]

def account_alias():
    return boto3.client("sts").get_caller_identity()["Account"]

def retrieve_events():
    client = boto3.client("logs")

    query = """fields @timestamp, @logStream, @log, @message
          | parse @message /^(?<parsed_ip>\S+) \S+ \S+ \[(?<parsed_date>[^\:]+):(?<parsed_time>[^\]]+)\] "(?<parsed_method>\S+) (?<parsed_url>\S+) (?<parsed_protocol>\S+)" (?<parsed_status>\d+) (?<parsed_bytes>\d+) "(?<parsed_referrer>[^"]*)" "(?<parsed_userAgent>[^"]*)"$/
          | fields parsed_ip as ip, parsed_date as date, parsed_time as time, parsed_method as method, parsed_url as url, parsed_protocol as protocol, parsed_status as status, parsed_bytes as bytes, parsed_referrer as referrer, parsed_userAgent as userAgent, @timestamp, @logStream, @log
          | sort @timestamp desc
          | limit 1000"""

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

        print("Gathered results from CloudWatch!")
        print(f"Found {len(response.get('results', {}))} results")

    parsed_response = []

    for result in response["results"]:
        element = {
            "timestamp": next((field["value"] for field in result if field["field"] == "@timestamp"), None),
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

    # Filter for status codes 404 and 200
    filtered_response = [event for event in parsed_response if event["status"] in ["404", "200"]]

    return filtered_response

def lambda_handler(event, context):
    print(event)
    print(context)

    client = boto3.client("sns")
    retrieved_events = retrieve_events()

    if not retrieved_events:
        print("No events with status 404 or 200 found.")
        return

    message = "Greetings from IAC_Enforcement Lambda!\n\n"
    message += f"On the account: {account_alias()}\n"
    message += f"We have detected {len(retrieved_events)} custom manual modification changes with status codes 404 or 200 within the last 2 minutes:\n"
    for event in retrieved_events:
        message += str(event)
        message += "\n"

    message += "\n\n"
    message += "Please take appropriate actions. You can also access the IAC enforcement for additional details.\n\n"
    message += f"Email Generated: {str(datetime.today())}"
    resp = client.publish(
        TargetArn=sns_arn,
        Message=message,
        Subject=f"IAC Enforcement Alert for account {account_alias()}",
    )

if __name__ == "__main__":
    lambda_handler(event="", context="")


