import json
import random
from datetime import datetime, timedelta
import uuid

class DataGenerator:
    """Generate transaction"""
    
    def __init__(self):
        self.countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Australia', 'Japan', 'Brazil']
        self.categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports']
        self.payment_methods = ['Credit Card', 'PayPal', 'Debit Card', 'Bank Transfer']
        self.products = self._generate_products()
        self.customers = self._generate_customers(100)
    
    def _generate_products(self):
        products = []
        for i in range(50):
            category = random.choice(self.categories)
            base_price = random.uniform(10, 1000)
            products.append({
                'product_id': f"PROD_{i:03d}",
                'product_name': f"{category} Product {i}",
                'category': category,
                'price': round(base_price, 2),
                'brand': f"Brand_{random.randint(1, 20)}"
            })
        return products
    
    def _generate_customers(self, count):
        customers = []
        for i in range(count):
            customers.append({
                'customer_id': f"CUST_{i:03d}",
                'name': f"Customer_{i}",
                'email': f"customer_{i}@example.com",
                'country': random.choice(self.countries),
                'signup_date': self._random_date()
            })
        return customers
    
    def _random_date(self, days_back=730):
        """Generate a random date"""
        random_days = random.randint(0, days_back)
        return (datetime.now() - timedelta(days=random_days)).strftime('%Y-%m-%d')
    
    def _random_datetime(self, days_back=30):
        """Generate a random datetime"""
        random_seconds = random.randint(0, days_back * 24 * 60 * 60)
        return (datetime.now() - timedelta(seconds=random_seconds)).isoformat()
    
    def generate_transaction(self):
        """Generate a single transaction"""
        customer = random.choice(self.customers)
        product = random.choice(self.products)
        quantity = random.randint(1, 5)
        
        return {
            'transaction_id': f"TXN_{str(uuid.uuid4())[:8]}",
            'customer_id': customer['customer_id'],
            'product_id': product['product_id'],
            'product_category': product['category'],
            'quantity': quantity,
            'unit_price': product['price'],
            'total_amount': round(product['price'] * quantity, 2),
            'transaction_date': self._random_datetime(),
            'payment_method': random.choice(self.payment_methods),
            'status': random.choice(['completed', 'pending', 'failed']),
            'customer_country': customer['country']
        }
    
    def generate_dataset(self, num_records=1000, output_file='data/raw/transactions.json'):
        """Generate a dataset of transactions"""
        import os
        os.makedirs('data/raw', exist_ok=True)
        
        with open(output_file, 'w') as f:
            for _ in range(num_records):
                transaction = self.generate_transaction()
                f.write(json.dumps(transaction) + '\n')
        
        print(f"Generated {num_records} transactions in {output_file}")

if __name__ == "__main__":
    generator = DataGenerator()
    generator.generate_dataset(5000)
    print("data generation completed!")