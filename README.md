# Real-Time Job Market Analytics Pipeline

A production-style streaming data pipeline that ingests live job postings, 
processes them in real-time, and stores analytics-ready data in a Parquet data lake.

## Architecture
```
Job API (JSearch) → Python Producer → Apache Kafka → PySpark Structured Streaming → Parquet Data Lake
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python, JSearch API (RapidAPI) |
| Message Broker | Apache Kafka 7.4.0 (Docker) |
| Stream Processing | PySpark 4.1.1 Structured Streaming |
| Infrastructure | Docker Compose |
| Storage | Parquet (local / AWS S3) |
| Analysis | Pandas, Jupyter Notebook |

## Key Findings (March 2026 Sample)

- **34 jobs** processed in real-time across 4 role categories
- **Top hiring companies:** Amazon, AWS, Lensa, Deloitte
- **Top states:** Virginia, DC, Maryland, Massachusetts
- **91% On-site/Hybrid** vs 9% Remote for data roles
- **Avg salary range:** $131,400 – $182,800 (roles with listed pay)

## Project Structure
```
job-market-pipeline/
├── producer/           # Kafka producer — fetches jobs from API
├── consumer/           # PySpark Structured Streaming consumer
├── analysis/           # Jupyter notebook with insights
├── docker/             # Docker Compose for Kafka + Spark
├── data/               # Parquet output (gitignored)
└── README.md
```

## How to Run

### Prerequisites
- Docker Desktop
- Python 3.9+
- RapidAPI account (JSearch API key)

### Setup
```bash
# Clone repo
git clone https://github.com/patilvaishnavi101/job-market-pipeline
cd job-market-pipeline

# Install dependencies
pip install -r requirements.txt

# Add your API key
cp .env.example .env
# Edit .env and add your RAPIDAPI_KEY
```

### Run the Pipeline
```bash
# 1. Start Kafka and Spark
cd docker && docker-compose up -d

# 2. Run producer (sends jobs to Kafka)
python producer/producer.py

# 3. Run Spark consumer (reads from Kafka, writes Parquet)
docker exec -u root spark /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  /app/consumer/spark_consumer.py

# 4. Explore insights
jupyter notebook analysis/insights.ipynb
```

## Skills Demonstrated
- Real-time stream processing with Apache Kafka
- PySpark Structured Streaming
- Docker-based infrastructure
- Data lake design with Parquet format
- End-to-end pipeline from API to analytics