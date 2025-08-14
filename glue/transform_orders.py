import sys, json, boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, sum as _sum, countDistinct

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "S3_INPUT_BUCKET",
        "S3_INPUT_KEY",
        "S3_CURATED_BUCKET",
        "DDB_TABLE",
        "SNS_TOPIC_ARN",
        "GLUE_DATABASE",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

raw_path = f's3://{args["S3_INPUT_BUCKET"]}/{args["S3_INPUT_KEY"]}'
df = spark.read.option("header", "true").csv(raw_path)

# types
df = (
    df.withColumn("quantity", col("quantity").cast("int"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn("date", to_date(col("date")))
)
df = df.withColumn("amount", col("quantity") * col("unit_price"))

# write Curated (Parquet partitioned by date)
curated_path = f's3://{args["S3_CURATED_BUCKET"]}/curated/orders'
(
    df.select(
        "order_id", "date", "customer_id", "product", "quantity", "unit_price", "amount"
    )
    .write.mode("append")
    .partitionBy("date")
    .parquet(curated_path)
)

# daily aggregates → DynamoDB
agg = df.groupBy("date").agg(
    _sum("amount").alias("total_sales"),
    countDistinct("order_id").alias("orders"),
    _sum("quantity").alias("units"),
)

items = []
for r in agg.collect():
    d = r["date"].strftime("%Y-%m-%d") if r["date"] else "unknown"
    items.append(
        {
            "date": d,
            "total_sales": str(r["total_sales"] or 0.0),
            "orders": int(r["orders"] or 0),
            "units": int(r["units"] or 0),
        }
    )

ddb = boto3.resource("dynamodb")
table = ddb.Table(args["DDB_TABLE"])
with table.batch_writer() as batch:
    for it in items:
        batch.put_item(Item=it)

# (opcional) update crawler from curated
glue = boto3.client("glue")
try:
    glue.start_crawler(
        Name=f"retail-curated-crawler-{args['JOB_NAME'].split('-',1)[-1]}"
    )
except Exception:
    pass

# notification SNS
boto3.client("sns").publish(
    TopicArn=args["SNS_TOPIC_ARN"],
    Subject="Glue ETL finished",
    Message=json.dumps({"input": raw_path, "curated": curated_path}),
)

job.commit()
