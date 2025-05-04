
CREATE TABLE IF NOT EXISTS dim_dates (
    date_id                 DATE PRIMARY KEY,              
    year                    INT NOT NULL,
    quarter                 INT NOT NULL,
    month                   INT NOT NULL,
    month_name              TEXT NOT NULL,
    week                    INT NOT NULL,
    day                     INT NOT NULL,
    day_of_week             INT NOT NULL,                  
    day_name                TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id             INT PRIMARY KEY,
    name                    VARCHAR(50),
    email                   VARCHAR(200),    
    telephone               TEXT,
    city                    VARCHAR(50),
    country                 VARCHAR(50),
    gender                  VARCHAR(10),
    date_of_birth           DATE,
    job_title               TEXT
);
CREATE TABLE IF NOT EXISTS dim_discounts (
    id                      SERIAL PRIMARY KEY,
    start_date_id           DATE NOT NULL,
    end_date_id             DATE NOT NULL,
    discount_rate           NUMERIC(3,2) NOT NULL,        
    description             TEXT,
    category                TEXT,
    sub_category            TEXT
);
CREATE TABLE IF NOT EXISTS dim_employees (
    employee_id             INT PRIMARY KEY,
    store_id                INT,
    name                    VARCHAR(255),
    position                VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS dim_products (
    product_id              INT PRIMARY KEY,
    category                VARCHAR(255),
    sub_category            VARCHAR(255),
    description_pt          TEXT,
    description_de          TEXT,
    description_fr          TEXT,
    description_es          TEXT,
    description_en          TEXT,
    description_zh          TEXT,
    color                   VARCHAR(50),
    sizes                   VARCHAR(255),
    production_cost         DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS dim_stores (
    store_id                INT PRIMARY KEY,
    country                 VARCHAR(255),
    city                    VARCHAR(255),
    store_name              VARCHAR(255),
    number_of_employees     INT,
    zip_code                VARCHAR(20),
    size                    VARCHAR(10),
    latitude                DECIMAL(9, 6),
    longitude               DECIMAL(9, 6)
);

CREATE TABLE IF NOT EXISTS fact_transactions (
    id                      SERIAL PRIMARY KEY,
    invoice_id              TEXT,
    line                    INT,
    customer_id             INT,
    product_id              INT,
    size                    VARCHAR(5),
    color                   VARCHAR(20),
    unit_price              FLOAT,
    quantity                INT,
    date                    DATE,
    datetime                DATE,
    discount                FLOAT,
    line_total              FLOAT,
    store_id                INT,
    employee_id             INT,
    currency                VARCHAR(10),
    currency_symbol         VARCHAR(10),
    sku                     TEXT,
    transaction_type        TEXT,
    payment_method          VARCHAR(20),
    invoice_total           NUMERIC(10)
);
CREATE TABLE IF NOT EXISTS dim_store_weather (
    id                      SERIAL PRIMARY KEY,
    store_id                INT,
    date_id                 DATE,
    temp_max                NUMERIC(5,2),
    temp_min                NUMERIC(5,2),
    is_rain                 BOOLEAN  
);
CREATE TABLE IF NOT EXISTS data_mart_weather_region_impact (
    id                      SERIAL PRIMARY KEY,
    country                 VARCHAR(50),
    city                    VARCHAR(50),
    store_size              VARCHAR(10),
    is_rain                 BOOLEAN,
    sale_date               DATE,
    transaction_count       INT,
    total_revenue           NUMERIC(12,2),
    avg_temp_max            NUMERIC(5,2),
    avg_temp_min            NUMERIC(5,2)
);

CREATE TABLE IF NOT EXISTS data_mart_product_weather_sensitivity (
    id                      SERIAL PRIMARY KEY,
    product_id              INT,
    category                VARCHAR(255),
    sub_category            VARCHAR(255),
    is_rain                 BOOLEAN,
    temperature_range       VARCHAR(10),
    transaction_count       INT,
    total_quantity          INT,
    total_revenue           NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS data_mart_store_size_weather_impact (
    id                      SERIAL PRIMARY KEY,
    store_size              VARCHAR(10),
    is_rain                 BOOLEAN,
    temperature_range       VARCHAR(10),
    transaction_count       INT,
    avg_transaction_value   NUMERIC(12,2),
    total_revenue           NUMERIC(12,2)
);
