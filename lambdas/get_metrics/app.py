import os, json, boto3

table = boto3.resource("dynamodb").Table(os.environ["DDB_TABLE"])


def handler(event, context):
    qs = event.get("queryStringParameters") or {}
    date = qs.get("date") if isinstance(qs, dict) else None

    if not date:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "missing ?date=YYYY-MM-DD"}),
        }

    try:
        res = table.get_item(Key={"date": date})
        item = res.get("Item")
        if not item:
            return {
                "statusCode": 404,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"error": f"no data for date {date}"}),
            }

        # item already contains: date (str), total_sales (str), orders (number), units (number)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(item, default=str),
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)}),
        }
