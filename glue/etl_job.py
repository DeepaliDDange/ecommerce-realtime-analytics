"""
Glue ETL Job: raw -> processed
------------------------------
Runs on a schedule (nightly via EventBridge Scheduler). Reads the raw JSON
that the Lambda landed in S3, cleans and standardises it, and writes
columnar Parquet to the "processed" zone, partitioned by date and category.
Parquet + partitioning is what makes Athena fast and cheap.

Why a DynamicFrame for the read?
  Reading via glue_context.create_dynamic_frame with a transformation_ctx
  lets Glue JOB BOOKMARKS work: each run only picks up files it has not
  processed before. That makes the nightly "append" idempotent instead of
  reprocessing (and duplicating) the entire history every night.

Job parameters(set as Default Arguments on the Glue job in the SAM template):
  --RAW_PATH         s3://<raw-bucket>/orders/
  --PROCESSED_PATH   s3://<processed-bucket>/orders_processed/
  --DATABASE         Glue catalog database (context/logging)
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "RAW_PATH", "PROCESSED_PATH", "DATABASE"]
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# 1. Read only NEW raw JSON since the last run (job bookmarks).

raw_dyf = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [args["RAW_PATH"]],
        "recurse": True,
    },
    format="json",
    transformation_ctx="raw_dyf",   # required for bookmarks to track files
)

raw_df = raw_dyf.toDF()

if len(raw_df.take(1)) == 0:
    print("No new raw data since last run. Exiting cleanly.")
    job.commit()
    sys.exit(0)

print(f"New raw record count: {raw_df.count()}")


# 2. Clean + standardise.

clean_df = (
    raw_df
    .dropDuplicates(["order_id"])
    .withColumn("order_ts", F.to_timestamp("order_timestamp"))
    .withColumn("order_date", F.to_date("order_ts"))
    .withColumn("order_hour", F.hour("order_ts"))
    .withColumn("total_amount", F.col("total_amount").cast("double"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("category", F.upper(F.trim(F.col("category"))))
    .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
)

processed_df = clean_df.select(
    "order_id",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "total_amount",
    "currency",
    "payment_method",
    "order_status",
    "shipping_city",
    "shipping_state",
    "order_ts",
    "order_hour",
    "order_date",
)


# 3. Write Snappy-compressed Parquet, partitioned by date + category.


(
    processed_df
    .coalesce(1)
    .write
    .mode("append")
    .partitionBy("order_date", "category")
    .option("compression", "snappy")
    .parquet(args["PROCESSED_PATH"])
)

print(f"Wrote {processed_df.count()} cleaned records to {args['PROCESSED_PATH']}")
job.commit()   # persists the job bookmark
