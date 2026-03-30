# Real-Time Job Market Analytics Pipeline

A production-style, cloud-native data pipeline that ingests live job postings, processes them in real-time, and stores analytics-ready data in an AWS S3 data lake — fully orchestrated with Apache Airflow.

## Architecture
```
JSearch API → Python Producer → Apache Kafka → PySpark Batch → AWS S3 (Parquet) → Pandas Analysis
                                                      ↑
                                              Apache Airflow
                                           (Daily 9AM Schedule)
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Source | JSearch API (RapidAPI) — live job postings |
| Message Broker | Apache Kafka 7.4.0 (Docker) |
| Stream Processing | PySpark 4.1.1 |
| Orchestration | Apache Airflow 2.7.3 (Docker) |
| Cloud Storage | AWS S3 (Parquet data lake) |
| Infrastructure | Docker Compose |
| Analysis | Pandas, Jupyter Notebook |

## Key Findings (March 2026)

- **80 jobs** processed across 4 role categories (Data Engineer, Data Analyst, Analytics Engineer, BI Engineer)
- **Top hiring companies:** Amazon, AWS, Lensa, Deloitte, IBM
- **Top states:** Virginia, DC, Maryland, Massachusetts, New Jersey
- **91% On-site/Hybrid** vs 9% Remote for data roles
- **Avg salary range:** $131,400 – $182,800 (roles with listed pay)
- **Pipeline runs daily at 9AM** via Airflow DAG

## Project Structure
```
job-market-pipeline/
├── producer/
│   └── producer.py          # Fetches jobs from API, publishes to Kafka
├── consumer/
│   └── spark_consumer.py    # PySpark batch job: Kafka → S3 Parquet
├── dags/
│   └── job_market_dag.py    # Airflow DAG: orchestrates full pipeline
├── analysis/
│   └── insights.ipynb       # Jupyter notebook with insights
├── docker/
│   ├── docker-compose.yml   # Kafka + Spark + Airflow
│   ├── Dockerfile.airflow   # Custom Airflow image
│   └── Dockerfile.spark     # Custom Spark image
├── .env.example             # Environment variables template
└── README.md
```

## How to Run

### Prerequisites
- Docker Desktop
- Python 3.9+
- RapidAPI account (JSearch API key)
- AWS account (S3 bucket)

### Setup
```bash
# Clone repo
git clone https://github.com/patilvaishnavi101/job-market-pipeline
cd job-market-pipeline

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your API keys
```

### Environment Variables
```
RAPIDAPI_KEY=your_rapidapi_key
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_BUCKET_NAME=your_s3_bucket
AWS_REGION=us-east-1
```

### Run the Pipeline
```bash
# 1. Start all services (Kafka, Spark, Airflow)
cd docker && docker-compose up -d

# 2. Run producer manually (or let Airflow schedule it)
python producer/producer.py

# 3. Run Spark consumer
docker exec -u root spark /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /app/consumer/spark_consumer.py

# 4. Or trigger via Airflow UI at http://localhost:8080

# 5. Explore insights
jupyter notebook analysis/insights.ipynb
```

## Airflow DAG

The pipeline runs automatically every day at 9AM:
```
run_kafka_producer → run_spark_consumer
(PythonOperator)      (BashOperator)
```

## Skills Demonstrated
- Apache Kafka — real-time message streaming
- PySpark — distributed batch data processing
- Apache Airflow — pipeline orchestration and scheduling
- AWS S3 — cloud data lake with Parquet storage
- Docker Compose — multi-container infrastructure
- Python — producer, ETL, and analysis scripts
- REST API integration — live data ingestion
