#!/usr/bin/env python3
"""
PyFlink Job: Reads from Kafka and writes to PostgreSQL using Table API
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    
    # Create Kafka source table
    table_env.execute_sql("""
        CREATE TABLE kafka_source (
            id INT,
            customer_id BIGINT, customer_first_name STRING, customer_last_name STRING,
            customer_age INT, customer_email STRING, customer_country STRING,
            customer_postal_code STRING, customer_pet_type STRING, customer_pet_name STRING,
            customer_pet_breed STRING,
            seller_id BIGINT, seller_first_name STRING, seller_last_name STRING,
            seller_email STRING, seller_country STRING, seller_postal_code STRING,
            product_id BIGINT, product_name STRING, product_category STRING,
            product_price DOUBLE, product_quantity INT, product_weight DOUBLE,
            product_color STRING, product_size STRING, product_brand STRING,
            product_material STRING, product_rating DOUBLE, product_reviews INT,
            product_release_date_sql STRING, product_expiry_date_sql STRING,
            store_id BIGINT, store_name STRING, store_location STRING, store_city STRING,
            store_state STRING, store_country STRING, store_phone STRING, store_email STRING,
            supplier_id BIGINT, supplier_name STRING, supplier_contact STRING,
            supplier_email STRING, supplier_phone STRING, supplier_address STRING,
            supplier_city STRING, supplier_country STRING,
            sale_date_sql STRING, sale_quantity INT, sale_total_price DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'mock_data',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-consumer-group',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset',
            'json.ignore-parse-errors' = 'true'
        )
    """)
    
    # Helper function to create sink tables
    def pg_sink(name, schema):
        return f"""
            CREATE TABLE {name} ({schema}) WITH (
                'connector' = 'jdbc',
                'url' = 'jdbc:postgresql://postgres:5432/bigdata',
                'table-name' = 'star.{name}',
                'username' = 'admin',
                'password' = 'admin123'
            )
        """
    
    # Create dimension and fact tables
    table_env.execute_sql(pg_sink('dim_customer',
        'customer_id BIGINT PRIMARY KEY NOT ENFORCED, first_name STRING, last_name STRING, '
        'age INT, email STRING, country STRING, postal_code STRING, '
        'pet_type STRING, pet_name STRING, pet_breed STRING'))
    
    table_env.execute_sql(pg_sink('dim_product',
        'product_id BIGINT PRIMARY KEY NOT ENFORCED, name STRING, category STRING, '
        'price DECIMAL(10,2), weight DECIMAL(10,2), color STRING, size STRING, '
        'brand STRING, material STRING, rating DECIMAL(3,1), reviews INT, '
        'release_date DATE, expiry_date DATE'))
    
    table_env.execute_sql(pg_sink('dim_seller',
        'seller_id BIGINT PRIMARY KEY NOT ENFORCED, first_name STRING, last_name STRING, '
        'email STRING, country STRING, postal_code STRING'))
    
    table_env.execute_sql(pg_sink('dim_store',
        'store_id BIGINT PRIMARY KEY NOT ENFORCED, name STRING, location STRING, '
        'city STRING, state STRING, country STRING, phone STRING, email STRING'))
    
    table_env.execute_sql(pg_sink('dim_supplier',
        'supplier_id BIGINT PRIMARY KEY NOT ENFORCED, name STRING, contact STRING, '
        'email STRING, phone STRING, address STRING, city STRING, country STRING'))
    
    # Create statement set for parallel execution
    stmt_set = table_env.create_statement_set()
    
    # Insert into dim_customer
    stmt_set.add_insert_sql("""
        INSERT INTO dim_customer
        SELECT customer_id, customer_first_name, customer_last_name, customer_age,
               customer_email, customer_country, customer_postal_code,
               customer_pet_type, customer_pet_name, customer_pet_breed
        FROM kafka_source WHERE customer_id IS NOT NULL
    """)
    
    # Insert into dim_product
    stmt_set.add_insert_sql("""
        INSERT INTO dim_product
        SELECT product_id, product_name, product_category,
               CAST(product_price AS DECIMAL(10,2)), CAST(product_weight AS DECIMAL(10,2)),
               product_color, product_size, product_brand, product_material,
               CAST(product_rating AS DECIMAL(3,1)), product_reviews,
               CAST(product_release_date_sql AS DATE),
               CAST(product_expiry_date_sql AS DATE)
        FROM kafka_source WHERE product_id IS NOT NULL
    """)
    
    # Insert into dim_seller
    stmt_set.add_insert_sql("""
        INSERT INTO dim_seller
        SELECT seller_id, seller_first_name, seller_last_name,
               seller_email, seller_country, seller_postal_code
        FROM kafka_source WHERE seller_id IS NOT NULL
    """)
    
    # Insert into dim_store
    stmt_set.add_insert_sql("""
        INSERT INTO dim_store
        SELECT store_id, store_name, store_location, store_city,
               store_state, store_country, store_phone, store_email
        FROM kafka_source WHERE store_id IS NOT NULL
    """)
    
    # Insert into dim_supplier
    stmt_set.add_insert_sql("""
        INSERT INTO dim_supplier
        SELECT supplier_id, supplier_name, supplier_contact, supplier_email,
               supplier_phone, supplier_address, supplier_city, supplier_country
        FROM kafka_source WHERE supplier_id IS NOT NULL
    """)
    
    stmt_set.execute()

if __name__ == '__main__':
    main()
