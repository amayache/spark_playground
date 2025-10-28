import pytest
from pyspark.sql import SparkSession
from src.data_processing import DataProcessor
from src.analytics import EcommerceAnalytics

class TestEcommerceAnalytics:
    
    @pytest.fixture(scope="class")
    def spark_session(self):
        return (SparkSession.builder
                .appName("TestEcommerceAnalytics")
                .master("local[2]")
                .config("spark.sql.adaptive.enabled", "false")
                .getOrCreate())
    
    @pytest.fixture
    def sample_data(self, spark_session):
        data = [
            ("TXN_001", "CUST_001", "PROD_001", 2, 25.0, 50.0, 
             "2024-01-15T10:30:00", "Credit Card", "completed", "USA"),
            ("TXN_002", "CUST_002", "PROD_002", 1, 100.0, 100.0, 
             "2024-01-15T14:45:00", "PayPal", "completed", "UK"),
            ("TXN_003", "CUST_001", "PROD_003", 3, 15.0, 45.0, 
             "2024-01-16T09:15:00", "Credit Card", "completed", "USA")
        ]
        
        schema = ["transaction_id", "customer_id", "product_id", "quantity", 
                 "unit_price", "total_amount", "transaction_date", 
                 "payment_method", "status", "customer_country"]
        
        return spark_session.createDataFrame(data, schema)
    
    def test_data_cleaning(self, spark_session, sample_data):
        processor = DataProcessor(spark_session)
        cleaned_df = processor.clean_data(sample_data)
        
        # Should only have completed transactions
        assert cleaned_df.filter(col("status") != "completed").count() == 0
        assert cleaned_df.count() == 3
    
    def test_sales_metrics(self, spark_session, sample_data):
        analytics = EcommerceAnalytics(spark_session)
        sales_df = analytics.sales_performance_metrics(sample_data)
        
        assert sales_df.count() == 2  # Two different dates
        assert "daily_revenue" in sales_df.columns

if __name__ == "__main__":
    pytest.main([__file__])