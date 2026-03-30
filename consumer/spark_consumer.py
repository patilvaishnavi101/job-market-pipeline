from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, FloatType
)
import os

# Schema matching what producer sends
schema = StructType([
    StructField("job_id", StringType()),
    StructField("title", StringType()),
    StructField("company", StringType()),
    StructField("location", StringType()),
    StructField("state", StringType()),
    StructField("country", StringType()),
    StructField("is_remote", BooleanType()),
    StructField("salary_min", FloatType()),
    StructField("salary_max", FloatType()),
    StructField("salary_currency", StringType()),
    StructField("posted_at", StringType()),
    StructField("required_skills", StringType()),
    StructField("description_snippet", StringType())
])

def create_spark_session():
    return SparkSession.builder \
        .appName("JobMarketConsumer") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

def run():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Starting Spark consumer... reading from Kafka topic: job_postings")

    # Read from Kafka
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "job_postings") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Add a simple derived column
    df_enriched = df_parsed.withColumn(
        "work_type",
        when(col("is_remote") == True, "Remote")
        .otherwise("On-site / Hybrid")
    )

    # Write to local parquet files
    output_path = "data/job_postings"
    checkpoint_path = "data/checkpoints"

    os.makedirs(output_path, exist_ok=True)
    os.makedirs(checkpoint_path, exist_ok=True)

    query = df_enriched.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    print("Consumer running... will process for 60 seconds then stop.")
    query.awaitTermination(60)
    print("Done! Parquet files saved to data/job_postings/")

if __name__ == "__main__":
    run()