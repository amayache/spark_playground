import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
from data_generator import DataGenerator
from config import KafkaConfig

class KafkaTransactionProducer:
    """Simulates real-time transaction data streaming to Kafka"""
    
    def __init__(self, bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        self.data_generator = DataGenerator()
        self.running = False
    
    def generate_realtime_transaction(self, base_transaction):
        """Modify transaction for real-time characteristics"""
        transaction = base_transaction.copy()
        
        # Add real-time specific fields
        transaction['event_timestamp'] = datetime.now().isoformat()
        transaction['source'] = 'real-time'
        
        # Simulate real-time events
        if random.random() < 0.1:  # 10% chance of flash sale
            transaction['is_flash_sale'] = True
            transaction['total_amount'] = round(transaction['total_amount'] * 0.7, 2)  # 30% discount
        else:
            transaction['is_flash_sale'] = False
            
        # Simulate mobile vs web transactions
        transaction['platform'] = random.choice(['web', 'mobile', 'app'])
        
        return transaction
    
    def start_producing(self, topic=KafkaConfig.TOPIC, duration=300, rate=2):
        """
        Start producing transactions to Kafka
        
        Args:
            duration: How long to run in seconds
            rate: Transactions per second
        """
        self.running = True
        start_time = time.time()
        transaction_count = 0
        
        print(f"🚀 Starting Kafka producer for {duration} seconds...")
        print(f"📊 Target rate: {rate} transactions/second")
        print(f"📡 Topic: {topic}")
        
        try:
            while self.running and (time.time() - start_time) < duration:
                # Generate batch of transactions for this second
                for _ in range(rate):
                    base_transaction = self.data_generator.generate_transaction()
                    realtime_transaction = self.generate_realtime_transaction(base_transaction)
                    
                    # Send to Kafka
                    self.producer.send(topic, realtime_transaction)
                    transaction_count += 1
                    
                    print(f"📨 Sent transaction {transaction_count}: {realtime_transaction['transaction_id']}")
                
                # Wait until next second
                time.sleep(1)
                
                # Show progress
                elapsed = time.time() - start_time
                remaining = duration - elapsed
                print(f"⏱️  Progress: {elapsed:.0f}s / {duration}s | Transactions: {transaction_count}")
                
        except KeyboardInterrupt:
            print("\n🛑 Stopping producer...")
        except Exception as e:
            print(f"❌ Error in producer: {e}")
        finally:
            self.stop()
            print(f"✅ Production completed. Sent {transaction_count} transactions")
    
    def stop(self):
        """Stop the producer"""
        self.running = False
        self.producer.flush()
        self.producer.close()

class KafkaTopicManager:
    """Manages Kafka topics (simulation only)"""
    
    @staticmethod
    def create_topic_if_not_exists():
        """Simulate topic creation - in real setup, use kafka-admin"""
        print(f"📝 Note: In production, create topic: {KafkaConfig.TOPIC}")
        print("For simulation, we'll produce to the topic directly")