#!/usr/bin/env python3
"""
Kafka Producer: Reads CSV files and generates unique IDs for dimensions
"""

import csv
import json
import os
from pathlib import Path
from kafka import KafkaProducer
import time
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'mock_data')
CSV_DATA_DIR = os.getenv('CSV_DATA_DIR', '/data')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
DELAY_BETWEEN_MESSAGES = float(os.getenv('DELAY_BETWEEN_MESSAGES', '0.001'))

class DimensionKeyGenerator:
    """Generate unique IDs for dimensions"""
    
    def __init__(self):
        self.customer_keys = {}
        self.product_keys = {}
        self.seller_keys = {}
        self.store_keys = {}
        self.supplier_keys = {}
        self.date_keys = {}
        
        self.customer_counter = 0
        self.product_counter = 0
        self.seller_counter = 0
        self.store_counter = 0
        self.supplier_counter = 0
        self.date_counter = 0
    
    def get_customer_id(self, email):
        """Generate unique customer ID based on email"""
        email_norm = (email or '').strip().lower()
        if not email_norm or email_norm not in self.customer_keys:
            self.customer_counter += 1
            if email_norm:
                self.customer_keys[email_norm] = self.customer_counter
            else:
                return 0
        return self.customer_keys.get(email_norm, 0)
    
    def get_seller_id(self, email):
        """Generate unique seller ID based on email"""
        email_norm = (email or '').strip().lower()
        if not email_norm or email_norm not in self.seller_keys:
            self.seller_counter += 1
            if email_norm:
                self.seller_keys[email_norm] = self.seller_counter
            else:
                return 0
        return self.seller_keys.get(email_norm, 0)
    
    def get_product_id(self, name, brand, category):
        """Generate unique product ID"""
        key = f"{(name or '').strip().lower()}|{(brand or '').strip().lower()}|{(category or '').strip().lower()}"
        if key not in self.product_keys:
            self.product_counter += 1
            self.product_keys[key] = self.product_counter
        return self.product_keys.get(key, 0)
    
    def get_store_id(self, name, city):
        """Generate unique store ID"""
        key = f"{(name or '').strip().lower()}|{(city or '').strip().lower()}"
        if key not in self.store_keys:
            self.store_counter += 1
            self.store_keys[key] = self.store_counter
        return self.store_keys.get(key, 0)
    
    def get_supplier_id(self, email):
        """Generate unique supplier ID based on email"""
        email_norm = (email or '').strip().lower()
        if not email_norm or email_norm not in self.supplier_keys:
            self.supplier_counter += 1
            if email_norm:
                self.supplier_keys[email_norm] = self.supplier_counter
            else:
                return 0
        return self.supplier_keys.get(email_norm, 0)
    
    def get_date_id(self, date_str):
        """Generate date ID from date string"""
        if not date_str:
            return None
        try:
            date_str = date_str.strip()
            dt = datetime.strptime(date_str, '%m/%d/%Y')
            date_id = int(dt.strftime('%Y%m%d'))
            if date_id not in self.date_keys:
                self.date_counter += 1
                self.date_keys[date_id] = self.date_counter
            return self.date_keys[date_id]
        except:
            return None

def parse_date(date_str):
    """Parse date string to SQL format"""
    if not date_str:
        return None, None
    try:
        date_str = date_str.strip()
        dt = datetime.strptime(date_str, '%m/%d/%Y')
        return int(dt.strftime('%Y%m%d')), dt.strftime('%Y-%m-%d')
    except:
        return None, None

def json_serializer(data):
    """Serialize to JSON"""
    return json.dumps(data, default=str).encode('utf-8')

def read_csv_files(data_dir):
    """Generator that yields processed rows"""
    csv_files = sorted(Path(data_dir).glob('MOCK_DATA*.csv'))
    
    if not csv_files:
        logger.error(f"No CSV files found in {data_dir}")
        return
    
    logger.info(f"Found {len(csv_files)} CSV files")
    key_gen = DimensionKeyGenerator()
    
    for csv_file in csv_files:
        logger.info(f"Processing {csv_file.name}")
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                row_count = 0
                for row in reader:
                    # Generate dimension keys
                    customer_email = row.get('customer_email', '')
                    seller_email = row.get('seller_email', '')
                    supplier_email = row.get('supplier_email', '')
                    product_name = row.get('product_name', '')
                    product_brand = row.get('product_brand', '')
                    product_category = row.get('product_category', '')
                    store_name = row.get('store_name', '')
                    store_city = row.get('store_city', '')
                    sale_date = row.get('sale_date', '')
                    
                    # Generate IDs for keys
                    row['customer_id'] = key_gen.get_customer_id(customer_email)
                    row['seller_id'] = key_gen.get_seller_id(seller_email)
                    row['product_id'] = key_gen.get_product_id(product_name, product_brand, product_category)
                    row['store_id'] = key_gen.get_store_id(store_name, store_city)
                    row['supplier_id'] = key_gen.get_supplier_id(supplier_email)
                    
                    # Parse dates
                    sale_date_id, sale_date_sql = parse_date(sale_date)
                    row['sale_date_sql'] = sale_date_sql
                    
                    release_date_id, release_date_sql = parse_date(row.get('product_release_date', ''))
                    row['product_release_date_sql'] = release_date_sql
                    
                    expiry_date_id, expiry_date_sql = parse_date(row.get('product_expiry_date', ''))
                    row['product_expiry_date_sql'] = expiry_date_sql
                    
                    # Convert numeric types
                    row['product_price'] = float(row.get('product_price', 0) or 0)
                    row['product_quantity'] = int(row.get('product_quantity', 0) or 0)
                    row['product_weight'] = float(row.get('product_weight', 0) or 0)
                    row['product_rating'] = float(row.get('product_rating', 0) or 0)
                    row['product_reviews'] = int(row.get('product_reviews', 0) or 0)
                    row['sale_quantity'] = int(row.get('sale_quantity', 0) or 0)
                    row['sale_total_price'] = float(row.get('sale_total_price', 0) or 0)
                    
                    row_count += 1
                    yield row
                
                logger.info(f"Processed {row_count} rows from {csv_file.name}")
        except Exception as e:
            logger.error(f"Error reading {csv_file.name}: {e}")

def send_to_kafka():
    """Main producer loop"""
    logger.info(f"Connecting to Kafka: {KAFKA_BROKER}, Topic: {KAFKA_TOPIC}")

    producer = None
    for attempt in range(1, 16):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=json_serializer,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            break
        except Exception as e:
            logger.warning(f"Kafka is not ready yet (attempt {attempt}/15): {e}")
            time.sleep(5)

    if producer is None:
        raise RuntimeError("Could not connect to Kafka")
    
    message_count = 0
    batch_start = time.time()
    
    try:
        for row in read_csv_files(CSV_DATA_DIR):
            # Send with supplier_email as key for partitioning
            key = (row.get('supplier_email', '') or '').encode('utf-8')
            producer.send(KAFKA_TOPIC, key=key, value=row)
            message_count += 1
            
            if message_count % BATCH_SIZE == 0:
                elapsed = time.time() - batch_start
                rate = BATCH_SIZE / elapsed if elapsed > 0 else 0
                logger.info(f"Sent {message_count} messages (Rate: {rate:.1f} msg/s)")
                batch_start = time.time()
            
            time.sleep(DELAY_BETWEEN_MESSAGES)
        
        producer.flush()
        logger.info(f"Successfully sent {message_count} messages")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        raise
    finally:
        producer.close()

if __name__ == '__main__':
    send_to_kafka()
