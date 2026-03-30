from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow/producer')

default_args = {
    'owner': 'vaishnavi',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def run_producer():
    import requests
    import json
    from kafka import KafkaProducer
    from dotenv import load_dotenv

    API_KEY = os.getenv("RAPIDAPI_KEY")

    def fetch_jobs(query):
        url = "https://jsearch.p.rapidapi.com/search"
        headers = {
            "X-RapidAPI-Key": API_KEY,
            "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
        }
        params = {"query": query, "page": "1", "num_pages": "1", "date_posted": "today"}
        response = requests.get(url, headers=headers, params=params)
        return response.json().get("data", [])

    def extract_fields(job):
        return {
            "job_id": job.get("job_id"),
            "title": job.get("job_title"),
            "company": job.get("employer_name"),
            "location": job.get("job_city"),
            "state": job.get("job_state"),
            "country": job.get("job_country"),
            "is_remote": job.get("job_is_remote"),
            "salary_min": job.get("job_min_salary"),
            "salary_max": job.get("job_max_salary"),
            "salary_currency": job.get("job_salary_currency"),
            "posted_at": job.get("job_posted_at_datetime_utc"),
            "required_skills": job.get("job_required_skills"),
            "description_snippet": (job.get("job_description") or "")[:200]
        }

    producer = KafkaProducer(
        bootstrap_servers="kafka:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    queries = ["data engineer", "data analyst", "analytics engineer", "BI engineer"]
    total = 0
    for query in queries:
        jobs = fetch_jobs(query)
        for job in jobs:
            record = extract_fields(job)
            producer.send("job_postings", value=record)
            total += 1

    producer.flush()
    print(f"Producer done — sent {total} jobs to Kafka")

with DAG(
    dag_id="job_market_pipeline",
    default_args=default_args,
    description="Real-time job market pipeline",
    schedule_interval="0 9 * * *",  # runs every day at 9am
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kafka", "job-market", "data-engineering"]
) as dag:

    task_producer = PythonOperator(
        task_id="run_kafka_producer",
        python_callable=run_producer,
    )

    task_consumer = BashOperator(
        task_id="run_spark_consumer",
        bash_command="""
        docker exec -u root spark /opt/spark/bin/spark-submit \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
          /app/consumer/spark_consumer.py
        """,
    )

    task_producer >> task_consumer