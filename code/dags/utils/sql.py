from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import pandas as pd
from sqlalchemy import create_engine
from .logger import get_logger
logger = get_logger(__name__)

def run_sql_query(sql: str):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    records = postgres_hook.get_records(sql)
    return records

def load_csv_to_postgres(table_name : str, file :str, chunksize=10000):
    try:
        # Fetch connection details securely from Airflow
        conn = BaseHook.get_connection("postgres_conn")

        # Create SQLAlchemy engine without hardcoding credentials
        logger.info(f"Login to database {conn.schema}")
        engine = create_engine(
            f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
        )

        # Process CSV in chunks to avoid memory issues
        for chunk in pd.read_csv(file, chunksize=chunksize):
            chunk.to_sql(table_name, engine, if_exists="append", index=False)
            logger.info(f"Loaded chunk of {len(chunk)} rows to {table_name}")

        logger.info(f"Successfully completed loading {file} to {table_name}")

    except Exception as e:
        logger.error(f"Error loading CSV to PostgreSQL: {str(e)}")
        raise e

    finally:
        # Ensure engine resources are released
        if "engine" in locals():
            engine.dispose()