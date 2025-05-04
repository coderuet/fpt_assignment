from sqlalchemy import create_engine
import requests as rq
import pandas as pd
import datetime
import sys
engine = create_engine(
    "postgresql://tienthanh2612:llxx@192.168.1.162:5300/fpt_assignment")
print(sys.executable)


def get_weather_data():
    print("xxx")


def main():
    # This shows which Python interpreter you're using
    sql = """
        SELECT 
            f.store_id,
            f.date,
            COUNT(f.invoice_id) AS transaction_count,
            SUM(f.invoice_total) AS invoice_total_by_store_date,
            d.product_id,
            SUM(quantity)
        FROM fact_transactions f
        JOIN dim_products d ON f.product_id = d.product_id
        GROUP BY f.store_id, f.date, d.category
    """
    merge_df = pd.read_sql_query(sql, con=engine)
    category_sales = merge_df.agg({
        'unit_price': 'sum',
        'quantity': 'sum'
    }).reset_index()
    # category_pivot = category_sales.pivot_table(
    #     index=['store_id', 'date'],
    #     columns='category',
    #     values=['quantity', 'amount'],
    #     fill_value=0
    # ).reset_index()
    print("123", category_sales)


main()
