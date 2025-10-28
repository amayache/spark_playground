#!/usr/bin/env python3
"""
Run batch analytics on both historical and streaming data
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from spark_session import create_spark_session
from analytics import EcommerceAnalytics

def main():
    print("📊 Running Batch Analytics (Historical + Streaming Data)")
    print("=" * 60)
    
    spark = create_spark_session()
    
    try:
        # Run standard analytics on historical data
        analytics = EcommerceAnalytics(spark)
        results = analytics.run_complete_analysis()
        
        # If streaming data exists, analyze it too
        try:
            streaming_df = spark.read.parquet("data/processed/streaming")
            if streaming_df.count() > 0:
                print("\n📈 ANALYZING STREAMING DATA")
                print("-" * 30)
                
                streaming_metrics = streaming_df.groupBy("platform").agg(
                    sum("total_amount").alias("streaming_revenue"),
                    count("transaction_id").alias("streaming_orders")
                )
                
                print("Platform Performance (Streaming):")
                streaming_metrics.show()
                
        except:
            print("ℹ️  No streaming data found yet - run Kafka simulation first")
        
    except Exception as e:
        print(f"❌ Analytics error: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()