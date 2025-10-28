from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from data_processing import DataProcessor
from config import SparkConfig

class Analytics:
    """ analytics base class """
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.processor = DataProcessor(spark_session)
    
    def sales_performance_metrics(self, df: DataFrame) -> DataFrame:
        """Calculate key sales performance metrics"""
        print("Calculating sales performance metrics...")
        return (df
                .groupBy("transaction_date")
                .agg(
                    sum("total_amount").alias("daily_revenue"),
                    count("transaction_id").alias("daily_orders"),
                    avg("total_amount").alias("avg_order_value"),
                    count_distinct("customer_id").alias("unique_customers")
                )
                .orderBy("transaction_date")
               )
    
    def customer_behavior_analysis(self, df: DataFrame) -> DataFrame:
        """Analyze customer purchasing behavior"""
        print("Analyzing customer behavior...")
        customer_metrics = (df
                           .groupBy("customer_id")
                           .agg(
                               count("transaction_id").alias("total_orders"),
                               sum("total_amount").alias("total_spent"),
                               avg("total_amount").alias("avg_order_value"),
                               min("transaction_date").alias("first_purchase"),
                               max("transaction_date").alias("last_purchase")
                           ))
        
        # Calculate recency (days since last purchase)
        latest_date = df.agg(max("transaction_date")).collect()[0][0]
        
        return (customer_metrics
                .withColumn("days_since_last_purchase", 
                           datediff(lit(latest_date), col("last_purchase")))
                .withColumn("customer_segment",
                           when(col("total_spent") > 1000, "VIP")
                           .when(col("total_spent") > 500, "Premium")
                           .when(col("total_spent") > 100, "Regular")
                           .otherwise("Occasional")))
    
    def product_performance(self, df: DataFrame) -> DataFrame:
        """Analyze product performance"""
        print("Analyzing product performance...")
        return (df
                .groupBy("product_id", "product_category")
                .agg(
                    sum("quantity").alias("total_quantity_sold"),
                    sum("total_amount").alias("total_revenue"),
                    count("transaction_id").alias("times_purchased"),
                    count_distinct("customer_id").alias("unique_customers")
                )
                .orderBy(desc("total_revenue"))
               )
    
    def hourly_sales_patterns(self, df: DataFrame) -> DataFrame:
        """Analyze sales patterns by hour of day"""
        print("Analyzing hourly sales patterns...")
        return (df
                .groupBy("hour_of_day")
                .agg(
                    sum("total_amount").alias("hourly_revenue"),
                    count("transaction_id").alias("hourly_orders"),
                    avg("total_amount").alias("avg_order_value")
                )
                .orderBy("hour_of_day")
               )
    
    def country_wise_analysis(self, df: DataFrame) -> DataFrame:
        """Analyze sales by country"""
        print("Analyzing country-wise performance...")
        return (df
                .groupBy("customer_country")
                .agg(
                    sum("total_amount").alias("total_revenue"),
                    count("transaction_id").alias("total_orders"),
                    count_distinct("customer_id").alias("unique_customers"),
                    avg("total_amount").alias("avg_order_value")
                )
                .orderBy(desc("total_revenue"))
               )
    
    def category_analysis(self, df: DataFrame) -> DataFrame:
        """Analyze performance by product category"""
        print("Analyzing category performance...")
        return (df
                .groupBy("product_category")
                .agg(
                    sum("total_amount").alias("category_revenue"),
                    count("transaction_id").alias("category_orders"),
                    avg("total_amount").alias("avg_category_order_value"),
                    count_distinct("product_id").alias("unique_products")
                )
                .orderBy(desc("category_revenue"))
               )
    
    def run_complete_analysis(self):
        """Run all analytics and save results"""
        print("Starting complete analytics pipeline...")
        
        # Process data
        processed_df = self.processor.process_data()
        
        # Run various analytics
        analytics = {
            "sales_performance": self.sales_performance_metrics(processed_df),
            "customer_behavior": self.customer_behavior_analysis(processed_df),
            "product_performance": self.product_performance(processed_df),
            "hourly_patterns": self.hourly_sales_patterns(processed_df),
            "country_analysis": self.country_wise_analysis(processed_df),
            "category_analysis": self.category_analysis(processed_df)
        }
        
        # Save all analytics results
        config = SparkConfig()
        for name, result_df in analytics.items():
            output_path = f"{config.ANALYTICS_OUTPUT_PATH}/{name}"
            print(f"Saving {name} to {output_path}")
            (result_df.write
             .mode("overwrite")
             .parquet(output_path))
        
        print("All analytics completed and saved!")
        return analytics

def create_spark_session():
    """Create and configure Spark session"""
    return (SparkSession.builder
            .appName(SparkConfig.APP_NAME)
            .master(SparkConfig.MASTER)
            .config("spark.executor.memory", SparkConfig.MEMORY)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # For timestamp parsing
            .getOrCreate())

if __name__ == "__main__":
    spark = create_spark_session()
    
    try:
        analytics = Analytics(spark)
        results = analytics.run_complete_analysis()
        
        # Show sample results
        print("\n" + "="*60)
        print("ANALYTICS RESULTS SUMMARY")
        print("="*60)
        
        for name, df in results.items():
            print(f"\n{name.upper()} - First 5 rows:")
            df.show(5, truncate=False)
            
        print("\n🎉 Analytics completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during analytics: {e}")
        import traceback
        traceback.print_exc()
        raise
    
    finally:
        spark.stop()
        print("Spark session stopped.")