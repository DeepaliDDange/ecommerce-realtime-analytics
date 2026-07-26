# E-Commerce Real-Time Analytics Platform (AWS)

A production-style data platform that ingests e-commerce order events in
real time, serves live metrics, and curates a queryable analytics data lake.
Built entirely with serverless AWS services and deployed with Terraform.

This project demonstrates an **end-to-end data pipeline** — streaming
ingestion, real-time aggregation, a batch ETL layer, a partitioned Parquet
data lake, and SQL analytics — the kind of system retail and e-commerce
companies run in production.

---

## Architecture

```
                                                  ┌───────────────────────────┐
                                                  │  REAL-TIME PATH            │
 ┌──────────────┐    ┌──────────────┐    ┌────────┴─────┐                     │
 │ Order        │    │   Kinesis    │    │   Lambda     │── DynamoDB (live     │
 │ Generator    │──▶ │ Data Stream  │──▶ │  (transform  │   metrics: revenue  │
 │ (Python)     │    │              │    │   + route)   │   & counts /cat/day)│
 └──────────────┘    └──────────────┘    └────┬────┬────┘                     │
                                              │    │    └── SNS (high-value    │
                                              │    │         order alerts)     │
                                              │    └──────────────────────────┘
                                              ▼
                                    ┌──────────────────┐
                                    │  S3 RAW zone      │  (newline JSON,
                                    │  orders/y=/m=/d=  │   date-partitioned)
                                    └────────┬─────────┘
                                             │   BATCH PATH 
                  EventBridge Scheduler ────▶│
                  (cron 02:00 UTC)           ▼
                                    ┌──────────────────┐
                                    │  Glue ETL job     │  (JSON ➜ Parquet,
                                    │  + job bookmarks  │   clean, partition)
                                    └────────┬─────────┘
                                             ▼
                                    ┌──────────────────┐
                                    │  S3 PROCESSED     │  (Snappy Parquet,
                                    │  orders_processed │   date + category)
                                    └────────┬─────────┘
                                             │ (crawler runs on job success)
                                             ▼
                                    ┌──────────────────┐
                                    │  Glue Crawler ➜   │
                                    │  Glue Catalog     │
                                    └────────┬─────────┘
                                             ▼
                                    ┌──────────────────┐
                                    │  Athena (SQL)     │  
                                    └──────────────────┘
```

The platform splits into two paths:

- **Real-time path** — Kinesis streams order events to a Lambda that updates
  live aggregates in DynamoDB, fires SNS alerts for high-value orders, and
  lands the raw events in S3.
- **Batch path** — a nightly Glue job converts raw JSON into clean,
  partitioned Parquet; a crawler registers it in the Glue Catalog; Athena
  queries it with standard SQL.

## AWS services used 

| Service | Role in the platform |
|---|---|
| **Kinesis Data Streams** | Real-time ingestion backbone |
| **Lambda** | Stream processing: transform, route, aggregate, alert |
| **S3** | Data lake — raw and processed zones |
| **DynamoDB** | Low-latency store for live metrics |
| **SNS** | High-value order notifications |
| **Glue ETL** | Batch transform raw JSON → Parquet |
| **Glue Crawler** | Schema discovery / partition registration |
| **Glue Data Catalog** | Central metadata store for Athena |
| **Athena** | Serverless SQL over the data lake |
| **EventBridge Scheduler** | Triggers the nightly batch job |
| **CloudWatch** | Logs and metrics for Lambda / Glue |
| **IAM** | Least-privilege roles for every component |

## Repository layout

```
.
├── data-generator/        # Python producer -> Kinesis
├── lambda/                # Stream processor (Kinesis consumer)
├── glue/                  # PySpark ETL job (raw -> Parquet)
├── athena/                # Sample analytical SQL
├── sam/                   # Infrastructure as code (AWS SAM)
├── architecture/          # Detailed design notes

```

## Quickstart


```bash
# 1. Build and deploy the infrastructure
cd sam
sam build
sam deploy --guided
# then upload the Glue script once (see the UploadScriptCommand output)

# 2. Stream some orders (use the stream name from the deploy outputs)
cd ../data-generator
pip install -r requirements.txt
python generate_orders.py --stream <kinesis_stream_name> --total 500

# 3. Run the batch ETL job + crawler (or wait for the nightly schedule)
aws glue start-job-run --job-name <glue_job_name>

# 4. Query in Athena (see athena/queries.sql)
```

## Cost & cleanup

Everything here is serverless / pay-per-use, but **Kinesis provisioned
shards bill hourly even when idle**. Tear everything down when you are done:

```bash
cd terraform
terraform destroy
```

