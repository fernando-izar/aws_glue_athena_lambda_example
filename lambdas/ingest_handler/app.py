# get S3 event, send to SQS (Simple Queue Service) and notify SNS (Simple Notification Service)
import os, json, boto3

sns = boto3.client("sns")
sqs = boto3.client("sqs")

TOPIC_ARN = os.environ["TOPIC_ARN"]
QUEUE_URL = os.environ["QUEUE_URL"]


def handler(event, context):
    # S3 event (ObjectCreated)
    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key = rec["s3"]["object"]["key"]

        # process file only in raw_orders/
        if not key.startswith("raw_orders/"):
            continue

        msg = {"bucket": bucket, "key": key}

        # send message to SQS (to act pipeline later)
        sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(msg))

        # publish SNS notification (monitoring/alert)
        sns.publish(
            TopicArn=TOPIC_ARN,
            Subject="Novo arquivo RAW_ORDERS recebido",
            Message=json.dumps(msg),
        )

    return {"status": "ok"}
