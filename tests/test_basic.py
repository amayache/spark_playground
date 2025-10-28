import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

def test_data_generator():
    """Test data generator"""
    from data_generator import DataGenerator
    
    generator = DataGenerator()
    transaction = generator.generate_transaction()
    
    # Check required fields
    required_fields = ['transaction_id', 'customer_id', 'product_id', 'quantity', 
                      'total_amount', 'status', 'product_category']
    
    for field in required_fields:
        assert field in transaction, f"Missing field: {field}"
    
    assert transaction['quantity'] > 0
    assert transaction['total_amount'] > 0
    assert transaction['status'] in ['completed', 'pending', 'failed']
    
    print("✓ Data generator test passed")

def test_spark_import():
    """Test that Spark can be imported"""
    try:
        from pyspark.sql import SparkSession
        print("✓ Spark import test passed")
    except ImportError as e:
        print(f"✗ Spark import failed: {e}")
        raise

if __name__ == "__main__":
    test_data_generator()
    test_spark_import()
    print("All basic tests passed! ✅")