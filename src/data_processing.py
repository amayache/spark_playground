from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from config import SparkConfig

class DataProcessor:
    """Data processing and cleaning operations"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.config = SparkConfig()
    
    def load_raw_data(self) -> DataFrame:
        """Load raw transaction data with correct schema"""
        schema = StructType([
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
            StructField("product_category", StringType(), True) 
        ])
        
        print("Loading data from:", self.config.RAW_DATA_PATH)
        df = self.spark.read.schema(schema).json(self.config.RAW_DATA_PATH)
        print(f"Loaded {df.count()} raw records")
        return df
    
    def clean_data(self, df: DataFrame) -> DataFrame:
        """Clean and preprocess the data"""
        print("Cleaning data...")
        
        # Show initial stats
        print(f"Initial record count: {df.count()}")
        print("Status distribution:")
        df.groupBy("status").count().show()
        
        cleaned_df = (df
                .filter(col("status") == "completed")  # Only completed transactions
                .filter(col("total_amount") > 0)       # Valid amount
                .filter(col("quantity") > 0)           # Positive quantity
                .withColumn("transaction_timestamp", 
                           to_timestamp(col("transaction_date")))
                .withColumn("transaction_date", 
                           to_date(col("transaction_timestamp")))
                .dropna(subset=["customer_id", "product_id", "total_amount"])
               )
        
        print(f"After cleaning: {cleaned_df.count()} records")
        return cleaned_df
    
    def add_derived_columns(self, df: DataFrame) -> DataFrame:
        """Add derived columns for analytics"""
        print("Adding derived columns...")
        return (df
                .withColumn("hour_of_day", hour(col("transaction_timestamp")))
                .withColumn("day_of_week", dayofweek(col("transaction_timestamp")))
                .withColumn("is_weekend", 
                           when(col("day_of_week").isin([1, 7]), True).otherwise(False))
                .withColumn("amount_category",
                           when(col("total_amount") < 50, "Small")
                           .when(col("total_amount") < 200, "Medium")
                           .otherwise("Large"))
               )
    
    def process_data(self) -> DataFrame:
        """Complete data processing pipeline"""
        print("Starting data processing pipeline...")
        
        raw_df = self.load_raw_data()
        
        # Show sample of raw data
        print("Sample of raw data:")
        raw_df.show(5)
        print("Raw data schema:")
        raw_df.printSchema()
        
        cleaned_df = self.clean_data(raw_df)
        final_df = self.add_derived_columns(cleaned_df)
        
        # Show sample of processed data
        print("Sample of processed data:")
        final_df.select("transaction_id", "customer_id", "product_id", "total_amount", 
                       "transaction_date", "product_category", "amount_category").show(5)
        
        # Write processed data
        print(f"Writing processed data to: {self.config.PROCESSED_DATA_PATH}")
        (final_df.write
         .mode("overwrite")
         .parquet(self.config.PROCESSED_DATA_PATH))
        
        print("Data processing completed successfully!")
        return final_df