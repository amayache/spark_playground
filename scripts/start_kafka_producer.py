#!/usr/bin/env python3
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__),'..', 'src'))

from kafka_simulator import KafkaTransactionProducer, KafkaTopicManager


def main():
    print("🎪 Starting Kafka Simulation for E-commerce Transactions")
    print("=" * 60)
    
    # Create topic (simulation)
    KafkaTopicManager.create_topic_if_not_exists()
    
    # Start producer
    producer = KafkaTransactionProducer()
    
    try:
        # Run for 5 minutes at 2 transactions per second
        producer.start_producing(duration=300, rate=2)
    except KeyboardInterrupt:
        print("\n🛑 Simulation stopped by user")
    except Exception as e:
        print(f"❌ Simulation error: {e}")

if __name__ == "__main__":
    main()