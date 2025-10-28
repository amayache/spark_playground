#!/usr/bin/env python3
import sys
import os
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

from spark_streaming import StreamingProcessor

def main():
    print("📡 Starting Spark Streaming Analytics...")
    
    processor = StreamingProcessor()
    query = processor.start_realtime_dashboard()
    
    if query:
        print("✅ Streaming started successfully!")
        print("📊 Waiting for transaction data...")
        print("⏹️  Press Ctrl+C to stop\n")
        
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            print("\n🛑 Stopping streaming...")
            processor.stop_all_queries()
    else:
        print("❌ Failed to start streaming")

if __name__ == "__main__":
    main()