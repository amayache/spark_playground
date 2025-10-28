#!/usr/bin/env python3
"""
Debug script to check data generation and processing
"""

import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from pyspark.sql import SparkSession
from config import SparkConfig

def debug_data():
    """Debug the data generation and loading"""
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("DebugData") \
        .master("local[1]") \
        .getOrCreate()
    
    try:
        # Check if data file exists
        data_file = SparkConfig.RAW_DATA_PATH
        if not os.path.exists(data_file):
            print(f"❌ Data file not found: {data_file}")
            print("Please run: python src/data_generator.py")
            return
        
        print(f"✅ Data file exists: {data_file}")
        
        # Check file size
        file_size = os.path.getsize(data_file)
        print(f"📁 File size: {file_size} bytes")
        
        # Try to read first few lines
        print("📄 First 3 lines of data:")
        with open(data_file, 'r') as f:
            for i, line in enumerate(f):
                if i >= 3:
                    break
                print(f"  Line {i+1}: {line.strip()}")
        
        # Try to load with Spark
        from data_processing import DataProcessor
        
        processor = DataProcessor(spark)
        raw_df = processor.load_raw_data()
        
        print("✅ Data loaded successfully in Spark!")
        print(f"📊 Total records: {raw_df.count()}")
        print("🔍 Sample data:")
        raw_df.show(5)
        print("📋 Schema:")
        raw_df.printSchema()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()

if __name__ == "__main__":
    debug_data()