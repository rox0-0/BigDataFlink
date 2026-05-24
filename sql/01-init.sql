-- Schema for star model
CREATE SCHEMA IF NOT EXISTS star;

-- Dimension: Customer
CREATE TABLE IF NOT EXISTS star.dim_customer (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    age INT,
    email VARCHAR(255),
    country VARCHAR(255),
    postal_code VARCHAR(20),
    pet_type VARCHAR(100),
    pet_name VARCHAR(255),
    pet_breed VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Seller
CREATE TABLE IF NOT EXISTS star.dim_seller (
    seller_id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    country VARCHAR(255),
    postal_code VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Product
CREATE TABLE IF NOT EXISTS star.dim_product (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10, 2),
    weight DECIMAL(8, 2),
    color VARCHAR(100),
    size VARCHAR(50),
    brand VARCHAR(255),
    material VARCHAR(255),
    rating DECIMAL(3, 1),
    reviews INT,
    release_date DATE,
    expiry_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Store
CREATE TABLE IF NOT EXISTS star.dim_store (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(100),
    country VARCHAR(255),
    phone VARCHAR(20),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Supplier
CREATE TABLE IF NOT EXISTS star.dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    contact VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(255),
    country VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS star.dim_date (
    date_id SERIAL PRIMARY KEY,
    sale_date DATE UNIQUE,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week VARCHAR(10)
);

-- Fact: Sales
CREATE TABLE IF NOT EXISTS star.fact_sales (
    sale_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES star.dim_date(date_id),
    customer_id INT REFERENCES star.dim_customer(customer_id),
    seller_id INT REFERENCES star.dim_seller(seller_id),
    product_id INT REFERENCES star.dim_product(product_id),
    store_id INT REFERENCES star.dim_store(store_id),
    supplier_id INT REFERENCES star.dim_supplier(supplier_id),
    quantity INT,
    total_price DECIMAL(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON star.fact_sales(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON star.fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_seller ON star.fact_sales(seller_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON star.fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_store ON star.fact_sales(store_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_supplier ON star.fact_sales(supplier_id);
