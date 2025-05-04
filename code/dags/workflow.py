# link kestra https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/02-workflow-orchestration/flows/06_gcp_taxi.yaml
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pandas as pd
import logging
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import os
import requests
from utils.sql import run_sql_query, load_csv_to_postgres
from utils.logger import get_logger
from utils.discord import on_failure_callback, on_success_callback
# Config block
logger = get_logger(__name__)
default_args = {
    "owner": "airflow",
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}
PREFIX_PATH_DATA = Variable.get("PREFIX_PATH_DATA", default_var="/tmp/data/")
PREFIX_PATH_SQL = Variable.get("PREFIX_PATH_SQL", default_var="/sql/")
WEATHER_API_ENDPOINT = Variable.get(
    "WEATHER_API_ENDPOINT", default_var="https://api.open-meteo.com/v1/forecast")
# Code logic


def import_dim_customer_data(**context):
    """
    Import data from staging_customer to dim_customer with renamed column
    """
    try:
        sql = """
            INSERT INTO dim_customers (
                customer_id, name , email , telephone , city , country , gender , date_of_birth, job_title
            )
            SELECT
                "Customer ID", "Name", "Email" , "Telephone", "City", "Country", "Gender", CAST("Date Of Birth" AS date), "Job Title"
            FROM staging_customers
            ON CONFLICT (customer_id) DO NOTHING;
        """
        records = run_sql_query(sql)
    except Exception as e:
        logger.error(f"Error in import_dim_customer_task : {e}")
        raise e


def import_dim_discounts(**context):
    """
    Import data from staging_discounts to dim_discounts with renamed column
    """
    try:
        sql = """
            INSERT INTO dim_discounts (
                start_date_id , end_date_id , discount_rate , description , category , sub_category
            )
            SELECT
                CAST("Start" AS date), CAST("End" AS date), "Discont" , "Description", "Category", "Sub Category"
            FROM staging_discounts
        """
        records = run_sql_query(sql)
    except Exception as e:
        logger.error(f"Error in import_dim_discounts_task : {e}")
        raise e


def import_dim_employees(**context):
    """
    Import data from staging_employees to dim_employees with renamed column
    """
    try:
        sql = """
            INSERT INTO dim_employees (
                employee_id , store_id , name , position
            )
            SELECT
                "Employee ID", "Store ID", "Name" , "Position"
            FROM staging_employees
            ON CONFLICT (employee_id) DO NOTHING;
        """
        records = run_sql_query(sql)
    except Exception as e:
        logger.error(f"Error in import_dim_employees : {e}")
        raise e


def import_dim_products(**context):
    """
    Import data from staging_products to dim_products with renamed column
    """
    try:
        sql = """
            INSERT INTO dim_products (
                product_id , category , sub_category , description_pt, description_de, description_fr, description_es, description_en, description_zh, color, sizes, production_cost
            )
            SELECT
                "Product ID", "Category", "Sub Category" , "Description PT" , "Description DE", "Description FR", "Description ES", "Description EN" , "Description ZH", "Color", "Sizes" , "Production Cost"
            FROM staging_products
            ON CONFLICT (product_id) DO NOTHING;
        """
        records = run_sql_query(sql)
    except Exception as e:
        logger.error(f"Error in import_dim_customer_task : {e}")
        raise e


def import_dim_stores(**context):
    """
    Import data from staging_stores to dim_stores with renamed column
    """
    try:
        sql = """
            INSERT INTO dim_stores (
                store_id , country , city , store_name, number_of_employees, zip_code, latitude, longitude, size
            )
            SELECT
                "Store ID", "Country", "City" , "Store Name" , "Number of Employees", "ZIP Code", "Latitude", "Longitude",
                CASE
                    WHEN "Number of Employees" <= 7 THEN 'Small'
                    WHEN "Number of Employees" < 10 THEN 'Medium'
                    ELSE 'Large'
                END AS status
            FROM staging_stores
            ON CONFLICT (store_id) DO NOTHING;
        """
        records = run_sql_query(sql)
    except Exception as e:
        logger.error(f"Error in import_dim_customer_task : {e}")
        raise e


def import_fact_transaction(**context):
    """
    Import data from staging_transactions to fact_transactions with renamed column
    """
    try:
        sql = """
            INSERT INTO fact_transactions (
                invoice_id , line , customer_id, product_id, size, color, unit_price, quantity, date, datetime, discount, line_total, store_id, employee_id, currency, currency_symbol, sku, transaction_type, payment_method, invoice_total
            )
            SELECT
                "Invoice ID", "Line", "Customer ID" , "Product ID" , "Size", "Color", "Unit Price", "Quantity", CAST("Date" AS date), CAST("Date" AS timestamp) , "Discount", "Line Total", "Store ID", "Employee ID", "Currency", "Currency Symbol", "SKU", "Transaction Type", "Payment Method", "Invoice Total"
            FROM staging_transactions
            ON CONFLICT DO NOTHING;
        """
        records = run_sql_query(sql)
    except Exception as e:
        logger.error(f"Error in import_dim_customer_task : {e}")
        raise e


def get_min_max_date_transaction(**kwargs):
    """
    Get MAX(Date) and Min(Date) from staging_transactions and push to xcom
    """
    try:
        sql = """
            SELECT MIN("Date"), MAX("Date")
            FROM staging_transactions
        """
        records = run_sql_query(sql)
        logger.info(f"records : {records}")
        if records:
            data = records[0]
            kwargs['ti'].xcom_push(key='min_max_date_transactions', value=data)
    except Exception as e:
        logger.error(f"Error :{e}")
        raise e


def import_dim_date_data(**kwargs):
    try:
        ti = kwargs['ti']
        min_max_date_transactions = ti.xcom_pull(
            task_ids='load_data_group_task.get_min_max_date_transaction', key='min_max_date_transactions')
        logger.info(
            f"Min Date {min_max_date_transactions} {type(min_max_date_transactions) } ")
        sql = f"""
            INSERT INTO dim_dates (
                date_id, year, quarter, month, month_name, week, day, day_of_week, day_name
            )
            SELECT
                g.date_id,
                EXTRACT(YEAR FROM g.date_id)::int,
                EXTRACT(QUARTER FROM g.date_id)::int,
                EXTRACT(MONTH FROM g.date_id)::int,
                TRIM(TO_CHAR(g.date_id, 'Month')),
                EXTRACT(WEEK FROM g.date_id)::int,
                EXTRACT(DAY FROM g.date_id)::int,
                EXTRACT(DOW FROM g.date_id)::int,
                TRIM(TO_CHAR(g.date_id, 'Day'))
            FROM (
                SELECT generate_series(
                    '{min_max_date_transactions[0]}'::date,
                    '{min_max_date_transactions[1]}'::date,
                    INTERVAL '1 day'
                ) AS date_id
            ) g
            ON CONFLICT (date_id) DO NOTHING;
        """
        records = run_sql_query(sql)
        logger.info(f"records : {records}")
        if records:
            data = records[0]
            kwargs['ti'].xcom_push(key='min_max_date_transactions', value=data)
    except Exception as e:
        logger.error(f"Error :{e}")
        raise e


def fetch_weather_data(**context):
    """
    Fetches weather data for each store location from Open-Meteo API and save it in postgresql
    """
    logging.info("Fetching weather data from Open-Meteo API")
    execution_date = (context['execution_date'] -
                      timedelta(days=1)).strftime('%Y-%m-%d')
    try:
        # Load store locations
        get_store_data_sql = """
            SELECT "Store ID", "Latitude" , "Longitude"
            FROM staging_stores
        """
        # Data set error, meteo api only return 92 day past dataset
        # ti = context['ti']
        # get_min_max_date_transactions = ti.xcom_pull(task_ids='transform_data.get_min_max_date_transaction', key='min_max_date_transactions')
        # logger.info(f"get_min_max_date_transactions {get_min_max_date_transactions}")
        store_records = run_sql_query(get_store_data_sql)
        all_weather_data = []
        for row in store_records:
            store_id = row[0]
            latitude = row[1]
            longitude = row[2]

            # Configure API parameters
            params = {
                'latitude': latitude,
                'longitude': longitude,
                'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum',
                'timezone': 'auto',
                "past_days": 92
            }

            response = requests.get(WEATHER_API_ENDPOINT, params=params)
            if response.status_code == 200:
                weather_data = response.json()
                all_dates = weather_data['daily']['time']
                for ind, date in enumerate(all_dates):
                    temp_max = weather_data['daily']['temperature_2m_max'][ind]
                    temp_min = weather_data['daily']['temperature_2m_min'][ind]
                    precipitation = weather_data['daily']['precipitation_sum'][ind]
                    logger.info(f"precipitation {precipitation}")
                    is_rain = precipitation > 1.0 if precipitation is not None else False
                    record = {
                        'date_id': date,
                        'store_id': store_id,
                        'temp_max': temp_max,
                        'temp_min': temp_min,
                        "is_rain": is_rain
                    }
                    all_weather_data.append(record)
            else:
                logging.error(
                    f"Failed to fetch weather data for store {store_id}: {response.text}")

        # Create a dataframe and save to file
        weather_df = pd.DataFrame(all_weather_data)
        weather_file = f"/tmp/data/weather/{execution_date}.csv"
        os.makedirs(os.path.dirname(weather_file), exist_ok=True)
        weather_df.to_csv(weather_file, index=False)

        # Push to XCom for later use
        context['ti'].xcom_push(key='weather_file', value=weather_file)
        return weather_file

    except Exception as e:
        logging.error(f"Error in fetch_weather_data: {str(e)}")
        raise e


def import_weather_data(**context):
    """
    Import Weather data
    """

    logger.info("Running import_weather_data")
    ti = context['ti']
    file_name = ti.xcom_pull(
        task_ids='load_data_group_task.fetch_weather_data', key='weather_file')
    load_csv_to_postgres(table_name="dim_store_weather", file=file_name)


def transformation_task(**context):
    """
    Transformation task to analyze correlation between weather and sales data
    Answers key business questions:
    1. How weather affects retail revenue by region/store tier
    2. Which products sell better during weather changes
    3. How different store sizes (Small/Medium/Large) respond to weather changes
    """
    logger.info("Running transformation_task")
    try:
        # 1. Weather impact by store region/tier
        region_weather_sql = """
            INSERT INTO data_mart_weather_region_impact 
            (country, city, store_size, is_rain, sale_date, transaction_count, total_revenue, avg_temp_max, avg_temp_min)
            SELECT 
                s.country, s.city, s.size as store_size,
                w.is_rain, 
                DATE_TRUNC('day', f.date) as sale_date,
                COUNT(DISTINCT f.invoice_id) as transaction_count,
                SUM(f.invoice_total) as total_revenue,
                AVG(w.temp_max) as avg_temp_max,
                AVG(w.temp_min) as avg_temp_min
            FROM fact_transactions f
            JOIN dim_stores s ON f.store_id = s.store_id
            JOIN dim_store_weather w ON f.store_id = w.store_id AND f.date = w.date_id
            GROUP BY s.country, s.city, s.size, w.is_rain, DATE_TRUNC('day', f.date)
        """
        run_sql_query(region_weather_sql)

        # 2. Product sensitivity to weather
        product_weather_sql = """
            INSERT INTO data_mart_product_weather_sensitivity
            (product_id, category, sub_category, is_rain, temperature_range, transaction_count, total_quantity, total_revenue)
            SELECT 
                p.product_id, p.category, p.sub_category,
                w.is_rain,
                CASE 
                    WHEN w.temp_max < 10 THEN 'cold'
                    WHEN w.temp_max BETWEEN 10 AND 25 THEN 'moderate'
                    ELSE 'hot'
                END as temperature_range,
                COUNT(f.invoice_id) as transaction_count,
                SUM(f.quantity) as total_quantity,
                SUM(f.line_total) as total_revenue
            FROM fact_transactions f
            JOIN dim_products p ON f.product_id = p.product_id
            JOIN dim_store_weather w ON f.store_id = w.store_id AND f.date = w.date_id
            GROUP BY p.product_id, p.category, p.sub_category, w.is_rain, 
                    CASE 
                        WHEN w.temp_max < 10 THEN 'cold'
                        WHEN w.temp_max BETWEEN 10 AND 25 THEN 'moderate'
                        ELSE 'hot'
                    END
        """
        run_sql_query(product_weather_sql)

        # 3. Store size response to weather changes
        store_size_weather_sql = """
            INSERT INTO data_mart_store_size_weather_impact
            (store_size, is_rain, temperature_range, transaction_count, avg_transaction_value, total_revenue)
            SELECT 
                s.size as store_size,
                w.is_rain,
                CASE 
                    WHEN w.temp_max < 10 THEN 'cold'
                    WHEN w.temp_max BETWEEN 10 AND 25 THEN 'moderate'
                    ELSE 'hot'
                END as temperature_range,
                COUNT(DISTINCT f.invoice_id) as transaction_count,
                AVG(f.invoice_total) as avg_transaction_value,
                SUM(f.invoice_total) as total_revenue
            FROM fact_transactions f
            JOIN dim_stores s ON f.store_id = s.store_id
            JOIN dim_store_weather w ON f.store_id = w.store_id AND f.date = w.date_id
            GROUP BY s.size, w.is_rain, 
                    CASE 
                        WHEN w.temp_max < 10 THEN 'cold'
                        WHEN w.temp_max BETWEEN 10 AND 25 THEN 'moderate'
                        ELSE 'hot'
                    END
        """
        run_sql_query(store_size_weather_sql)

        logger.info("Successfully completed weather-sales correlation analysis")

    except Exception as e:
        logger.error(f"Error in transformation_task: {e}")
        raise e


with DAG(
    "analytics_data_with_weather",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    dagrun_timeout=timedelta(minutes=45),
    description="Analyze correlation between weather and sales data",
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback
) as dag:
    logger.info("Running DAG import_data_from_file_storage_to_postgresql")
    start_dag = EmptyOperator(dag=dag, task_id="start_task")

    # Ingest task
    with TaskGroup(
        group_id="ingestion_data",
    ) as ingestion_data:
        transactions_columns = ["invoice_id",  "line", "customer_id", "product_id", "size", "color", "unit_price", "quantity", "date", "discount",
                                "line_total", "store_id", "employee_id", "currency", "currency_symbol", "sku", "transaction_type", "payment_method", "invoice_total"]
        import_customers_data_task = PythonOperator(
            task_id='import_staging_customers_data',
            python_callable=load_csv_to_postgres,
            op_kwargs={
                'table_name': "staging_customers",
                'file': PREFIX_PATH_DATA + "customers.csv",
            },
        )
        import_discounts_data_task = PythonOperator(
            task_id='import_staging_discounts_data',
            python_callable=load_csv_to_postgres,
            op_kwargs={
                'table_name': "staging_discounts",
                'file': PREFIX_PATH_DATA + "discounts.csv",
            },
        )
        import_employees_data_task = PythonOperator(
            task_id='import_staging_employees_data',
            python_callable=load_csv_to_postgres,
            op_kwargs={
                'table_name': "staging_employees",
                'file': PREFIX_PATH_DATA + "employees.csv",
            },
        )

        import_products_data_task = PythonOperator(
            task_id='import_staging_products_data',
            python_callable=load_csv_to_postgres,
            op_kwargs={
                'table_name': "staging_products",
                'file': PREFIX_PATH_DATA + "products.csv",
            },
        )
        import_stores_data_task = PythonOperator(
            task_id='import__staging_stores_data',
            python_callable=load_csv_to_postgres,
            op_kwargs={
                'table_name': "staging_stores",
                'file': PREFIX_PATH_DATA + "stores.csv",
            },
        )
        import_transactions_data_task = PythonOperator(
            task_id='import_staging_transactions_data',
            python_callable=load_csv_to_postgres,
            op_kwargs={
                'table_name': "staging_transactions",
                'file': PREFIX_PATH_DATA + "transactions.csv",
            },
        )

    # Load Task
    with TaskGroup(group_id="load_data_group_task") as load_data_group_task:
        logger.info("Running load_data_group_task")
        create_dim_fact_table = SQLExecuteQueryOperator(
            task_id="create_dim_fact_table",
            sql=PREFIX_PATH_SQL + "create_table.sql",
            split_statements=True,
            return_last=False,
            conn_id="postgres_conn"
        )
        get_min_max_date_transaction_task = PythonOperator(
            task_id='get_min_max_date_transaction',
            python_callable=get_min_max_date_transaction,
        )
        import_dim_date_data_task = PythonOperator(
            task_id="import_dim_date_data",
            python_callable=import_dim_date_data
        )
        fetch_weather_data_task = PythonOperator(
            task_id="fetch_weather_data",
            python_callable=fetch_weather_data,
        )
        import_weather_data_task = PythonOperator(
            task_id="import_weather_data",
            python_callable=import_weather_data
        )
        import_dim_customer_task = PythonOperator(
            task_id="import_dim_customer",
            python_callable=import_dim_customer_data
        )
        import_dim_discounts_task = PythonOperator(
            task_id="import_dim_discounts",
            python_callable=import_dim_discounts
        )
        import_dim_employees_task = PythonOperator(
            task_id="import_dim_employees",
            python_callable=import_dim_employees
        )
        import_dim_products_task = PythonOperator(
            task_id="import_dim_products",
            python_callable=import_dim_products
        )
        import_dim_stores_task = PythonOperator(
            task_id="import_dim_stores",
            python_callable=import_dim_stores
        )
        import_fact_transactions_task = PythonOperator(
            task_id="import_fact_transactions",
            python_callable=import_fact_transaction
        )
        truncate_staging_table_task = SQLExecuteQueryOperator(
            task_id="truncate_staging_table",
            sql=PREFIX_PATH_SQL + "truncate_staging_table.sql",
            split_statements=True,
            return_last=False,
            conn_id="postgres_conn"
        )
        create_dim_fact_table >> get_min_max_date_transaction_task >> import_dim_date_data_task >> fetch_weather_data_task >> import_weather_data_task >> \
            [import_dim_customer_task, import_dim_discounts_task, import_dim_employees_task,
             import_dim_products_task, import_dim_stores_task, import_fact_transactions_task] >> truncate_staging_table_task
    # Transformation task
    with TaskGroup(
        group_id="transformation_group_task"
    ) as transformation_group_task:
        logger.info("Start transformation task")
        transformation_task = PythonOperator(
            task_id="weather_sales_analysis",
            python_callable=transformation_task,
            dag=dag
        )

    # Define the workflow
    start_dag >> ingestion_data >> load_data_group_task >> transformation_group_task
