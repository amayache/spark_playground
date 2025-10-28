# test_version.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
print(f"PySpark version: {spark.version}")

# This should match your PySpark version
required_kafka_connector = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark.version}"
print(f"Required Kafka connector: {required_kafka_connector}")