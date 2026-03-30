import json
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("RAPIDAPI_KEY")

def fetch_jobs(query="data engineer", location="United States"):
    url = "https://jsearch.p.rapidapi.com/search"
    headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }
    params = {
        "query": query,
        "page": "1",
        "num_pages": "1",
        "date_posted": "today"
    }
    response = requests.get(url, headers=headers, params=params)
    return response.json().get("data", [])

def create_producer():
    return KafkaProducer(
        bootstrap_servers="localhost:29092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

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

def run():
    producer = create_producer()
    queries = ["data engineer", "data analyst", "analytics engineer", "BI engineer"]

    print("Starting producer... sending job postings to Kafka")

    for query in queries:
        print(f"Fetching jobs for: {query}")
        jobs = fetch_jobs(query=query)
        for job in jobs:
            record = extract_fields(job)
            producer.send("job_postings", value=record)
            print(f"  Sent: {record['title']} at {record['company']}")
        time.sleep(2)  # small pause between queries

    producer.flush()
    print("All jobs sent to Kafka topic: job_postings")

if __name__ == "__main__":
    run()