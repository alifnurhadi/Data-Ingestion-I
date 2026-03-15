import os
from datetime import datetime

import polars as pl
from sqlalchemy import create_engine

# since the data still small i might use select * instead of defining each columns


def _get_engine():
    """Builds the connection string using Airflow's Docker environment variables."""
    conn_str = (
        f"mysql+pymysql://"
        f"{os.getenv('MYSQL_USER')}:"
        f"{os.getenv('MYSQL_PASSWORD')}@"
        f"mysql:3306/"  # container name for mysql at my setup on yml
        f"{os.getenv('MYSQL_DATABASE')}"
    )
    return create_engine(conn_str)


def parse_dateformat(value):
    """parsing value for ambigue data formats"""
    if value is None:
        return None
    val_str = str(value).strip().upper()
    if val_str in ["NULL", "KOSONG", "NONE", ""]:
        return None
    formats = ["%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"]
    for fmt in formats:
        try:
            return datetime.strptime(val_str, fmt).date()
        except ValueError:
            continue
    return None


def task_clean_sales():
    """Airflow callable to clean sales_raw."""
    engine = _get_engine()
    df = pl.read_database(query="SELECT * FROM sales_raw", connection=engine)

    if df.is_empty():
        print("sales_raw is empty. Skipping.")
        return

    df = df.with_columns(
        pl.col("price")
        .cast(pl.Utf8)
        .str.replace_all(r"\.", "")
        .cast(pl.Int64)
        .alias("price")
    )

    df.write_database(
        table_name="sales_raw", connection=engine, if_table_exists="replace"
    )
    print(f"sales_raw -> {len(df)} rows cleaned and replaced ✅")


def task_clean_customers():
    """Airflow callable to clean customers_raw."""
    engine = _get_engine()
    df = pl.read_database(query="SELECT * FROM customers_raw", connection=engine)

    if df.is_empty():
        print("customers_raw is empty. Skipping.")
        return

    df = df.with_columns(
        pl.col("dob").map_elements(parse_dateformat, return_dtype=pl.Date).alias("dob")
    )

    df.write_database(
        table_name="customers_raw", connection=engine, if_table_exists="replace"
    )
    print(f"customers_raw -> {len(df)} rows cleaned and replaced ✅")
