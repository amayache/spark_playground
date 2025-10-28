from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

class StreamingProcessor:
    def __init__(self):
        self.spark = self._create_spark_session()
        self.logger = self._setup_logging()
        self.queries = [] 
        
        self.transaction_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("status", StringType(), True),
            StructField("customer_country", StringType(), True),
            StructField("product_category", StringType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("source", StringType(), True),
            StructField("is_flash_sale", BooleanType(), True),
            StructField("platform", StringType(), True)
        ])
    
    def _create_spark_session(self):
        """Spark session"""
        return SparkSession.builder \
            .appName("Streaming") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
    
    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)
    
    def create_kafka_stream(self):
        """Create Kafka stream"""
        try:
            self.logger.info("Creating Kafka stream from localhost:9093")
            
            return (self.spark
                .readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9093")
                .option("subscribe", "transactions")
                .option("startingOffsets", "latest")
                .load())
                
        except Exception as e:
            self.logger.error(f"Failed to create Kafka stream: {e}")
            raise
    
    def parse_kafka_messages(self, kafka_df):
        """Parse JSON messages"""
        return (kafka_df
            .select(
                from_json(col("value").cast("string"), self.transaction_schema).alias("data"),
                col("timestamp")
            )
            .select("data.*", "timestamp")
            .filter(col("status") == "completed"))
    
    def store_raw_data(self, parsed_df):
        """Store raw parsed data to disk before processing"""
        self.logger.info("💾 Setting up raw data storage...")
        
        return (parsed_df
            .writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", "data/raw/transactions")
            .option("checkpointLocation", "checkpoints/raw_data")
            .option("partitionBy", "processing_date")
            .trigger(processingTime="30 seconds")  # Write every 30 seconds
            .start())
    

    def calculate_realtime_metrics(self, parsed_df):
        """Calculate metrics - FIXED for streaming limitations"""
        return (parsed_df
            .withWatermark("timestamp", "1 minute")
            .groupBy(
                window(col("timestamp"), "1 minute", "30 seconds"),
                col("product_category")
            )
            .agg(
                sum("total_amount").alias("revenue"),
                count("transaction_id").alias("order_count"),
                avg("total_amount").alias("avg_order_value"),
                approx_count_distinct("customer_id").alias("unique_customers"),
                sum("quantity").alias("total_items_sold")
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("product_category"),
                col("revenue"),
                col("order_count"),
                col("avg_order_value"),
                col("unique_customers"),
                col("total_items_sold")
            ))
    
    def start_realtime_dashboard(self, processing_time='10 seconds'):
        """Start dashboard"""
        self.logger.info("🚀 Starting Real-time Dashboard...")
        
        try:
            kafka_stream = self.create_kafka_stream()
            parsed_stream = self.parse_kafka_messages(kafka_stream)
            # Storing metrics
            self.store_raw_data(parsed_stream)
            self.logger.info("✅ Raw data storage started")
            # Calculate metrics
            category_metrics = self.calculate_realtime_metrics(parsed_stream)
            
            # Start the main query
            query = (category_metrics
                .writeStream
                .outputMode("update")
                .format("console")
                .option("truncate", "false")
                .option("numRows", 10)
                .trigger(processingTime=processing_time)
                .start())
            
            self.queries.append(query)
            self.logger.info("✅ Dashboard started successfully!")
            return query
            
        except Exception as e:
            self.logger.error(f"❌ Failed to start dashboard: {e}")
            return None
    
    def stop_all_queries(self):
        """Stop all running queries"""
        for query in self.queries:
            try:
                query.stop()
                self.logger.info("✅ Stopped query")
            except Exception as e:
                self.logger.error(f"❌ Failed to stop query: {e}")

def create_spark_session():
    return SparkSession.builder \
        .appName("EcommerceStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()