from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    return SparkSession.builder \
        .appName("DataViewer") \
        .master("local[1]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()



if __name__ == "__main__":
    spark = create_spark_session()
    try:

        # 0. Streaming status
        print("\n📋 PLAYGROUND")
        print("-" * 30)
        streaming_df = spark.read.parquet("data/raw/transactions")
        streaming_df.agg(count("transaction_id").alias("total_transaction")).show(truncate=False)
        # 1. Show sales performance
        print("\n💰 SALES PERFORMANCE")
        print("-" * 30)
        sales_df = spark.read.parquet("data/analytics/sales_performance")
        sales_df.orderBy("transaction_date").show(20, truncate=False)
        
        # 2. Show customer behavior
        print("\n👥 CUSTOMER BEHAVIOR ANALYSIS")
        print("-" * 30)
        customers_df = spark.read.parquet("data/analytics/customer_behavior")
        customers_df.orderBy(desc("total_spent")).show(15, truncate=False)
        
        # 3. Show product performance
        print("\n📦 TOP PRODUCTS")
        print("-" * 30)
        products_df = spark.read.parquet("data/analytics/product_performance")
        products_df.orderBy(desc("total_revenue")).show(15, truncate=False)
        
        # 4. Show hourly patterns
        print("\n🕒 HOURLY SALES PATTERNS")
        print("-" * 30)
        hourly_df = spark.read.parquet("data/analytics/hourly_patterns")
        hourly_df.orderBy("hour_of_day").show(24, truncate=False)
        
        # 5. Show country analysis
        print("\n🌎 COUNTRY ANALYSIS")
        print("-" * 30)
        country_df = spark.read.parquet("data/analytics/country_analysis")
        country_df.orderBy(desc("total_revenue")).show(truncate=False)
        
        # 6. Show category analysis
        print("\n📋 CATEGORY PERFORMANCE")
        print("-" * 30)
        category_df = spark.read.parquet("data/analytics/category_analysis")
        category_df.orderBy(desc("category_revenue")).show(truncate=False)
        
    except Exception as e:
        print(f"❌ Error reading analytics data: {e}")
    
    finally:
        spark.stop()