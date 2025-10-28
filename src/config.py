class SparkConfig:
    """Spark configuration settings"""
    APP_NAME = "playground-spark-app"
    MASTER = "local[*]"
    MEMORY = "2g"
    
    # Data paths
    RAW_DATA_PATH = "data/raw/transactions.json"
    PROCESSED_DATA_PATH = "data/processed/"
    ANALYTICS_OUTPUT_PATH = "data/analytics/"
    
    # Kafka config (for real-time processing simulation)
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9093"
    KAFKA_TOPIC = "transactions"
    KAFKA_SIMULATION_DURATION = 300

    STREAMING_BATCH_DURATION = 10 
    
class KafkaConfig:
    """Kafka configuration for simulation"""
    BOOTSTRAP_SERVERS = "localhost:9093"
    TOPIC = "transactions"
    NUM_PARTITIONS = 3
    REPLICATION_FACTOR = 1