from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, FloatType
)
import os
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET     = os.getenv("AWS_BUCKET_NAME")

schema = StructType([
    StructField("job_id",              StringType()),
    StructField("title",               StringType()),
    StructField("company",             StringType()),
    StructField("location",            StringType()),
    StructField("state",               StringType()),
    StructField("country",             StringType()),
    StructField("is_remote",           BooleanType()),
    StructField("salary_min",          FloatType()),
    StructField("salary_max",          FloatType()),
    StructField("salary_currency",     StringType()),
    StructField("posted_at",           StringType()),
    StructField("required_skills",     StringType()),
    StructField("description_snippet", StringType())
])

def create_spark_session():
    spark = SparkSession.builder \
        .appName("JobMarketConsumer") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,"
                "org.apache.hadoop:hadoop-aws:3.4.0,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.streaming.metricsEnabled", "false") \
        .getOrCreate()

    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key", AWS_ACCESS_KEY)
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key", AWS_SECRET_KEY)
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.path.style.access", "false")

    return spark

def run():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Reading batch from Kafka topic: job_postings")

    # Batch read instead of streaming
    df_raw = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "job_postings") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()

    print(f"Records read from Kafka: {df_raw.count()}")

    df_parsed = df_raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    df_enriched = df_parsed.withColumn(
        "work_type",
        when(col("is_remote") == True, "Remote")
        .otherwise("On-site / Hybrid")
    )

    s3_output = f"s3a://{AWS_BUCKET}/job_postings"
    print(f"Writing {df_enriched.count()} records to S3: {s3_output}")

    df_enriched.write \
        .mode("overwrite") \
        .parquet(s3_output)

    print(f"Done! Parquet files saved to S3: {s3_output}")
    spark.stop()

if __name__ == "__main__":
    run()