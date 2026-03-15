import os
from datetime import date, datetime

import dotenv
import polars as pl
from sqlalchemy import create_engine

dotenv.load_dotenv()

MYSQL_CONN_STR = (
    f"mysql+pymysql://"
    f"{os.getenv('MYSQL_USER')}:"
    f"{os.getenv('MYSQL_PASSWORD')}@"
    f"localhost:3306/"
    f"{os.getenv('MYSQL_DATABASE')}"
)


def parse_dateformat(value: str):
    if value is None or value == "NULL":
        return None

    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def main():

    files = os.listdir("/Users/alif/Documents/Data Engineer/initFile")

    columnMap = {}
    dfMap = {}
    for file in files:
        if not file.endswith(".csv"):
            continue
        keyTable = file.split(".")[0]
        df = pl.read_csv(f"/Users/alif/Documents/Data Engineer/initFile/{file}")
        dfColumn = [col.lower() for col in df.columns]

        if keyTable == "sales_raw":
            df = df.with_columns(
                pl.col("price").str.replace_all(r"\.", "").cast(pl.Int64).alias("price")
            )

        if keyTable == "customers_raw":
            df = df.with_columns(
                pl.when(pl.col("dob").str.to_uppercase().str.contains("NULL|KOSONG"))
                .then(None)
                .otherwise(pl.col("dob"))
                .alias("dob")
            )
            df = df.with_columns(
                pl.col("dob").map_elements(
                    lambda x: parse_dateformat(x), return_dtype=pl.Date
                )
            )
        columnMap[keyTable] = tuple(dfColumn)
        dfMap[keyTable] = df

    engine = create_engine(MYSQL_CONN_STR)
    with engine.begin() as conn:
        for table in dfMap.keys():
            insertColumn = f"({', '.join(columnMap[table])})"
            valuesColumn = list(dfMap[table].iter_rows())
            placeholders = f"({', '.join(['%s'] * len(columnMap[table]))})"
            query = f"INSERT INTO {table} {insertColumn} VALUES {placeholders}"

            conn.exec_driver_sql(query, valuesColumn)
            print(f"{table} → {len(valuesColumn)} rows inserted ✅")


if __name__ == "__main__":
    main()
