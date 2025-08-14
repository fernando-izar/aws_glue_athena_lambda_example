import os, json, boto3

glue = boto3.client("glue")

JOB_NAME = os.environ["JOB_NAME"]
CURATED_BUCKET = os.environ["CURATED_BUCKET"]
DDB_TABLE = os.environ["DDB_TABLE"]
GLUE_DATABASE = os.environ["GLUE_DATABASE"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def handler(event, context):
    for rec in event.get("Records", []):
        body = rec.get("body", "{}")
        try:
            msg = json.loads(body)
        except json.JSONDecodeError:
            msg = {"raw_body": body}

        bucket = msg.get("bucket")
        key = msg.get("key")
        if not bucket or not key:
            print("Invalid message:", body)
            continue
        if not key.lower().endswith(".csv"):
            print("Skipping non-CSV:", key)
            continue

        print(f"[start_glue_job] starting job={JOB_NAME} for {bucket}/{key}")
        args = {
            "--S3_INPUT_BUCKET": bucket,
            "--S3_INPUT_KEY": key,
            "--S3_CURATED_BUCKET": CURATED_BUCKET,
            "--DDB_TABLE": DDB_TABLE,
            "--SNS_TOPIC_ARN": SNS_TOPIC_ARN,
            "--GLUE_DATABASE": GLUE_DATABASE,
        }
        glue.start_job_run(JobName=JOB_NAME, Arguments=args)

    return {"ok": True}
