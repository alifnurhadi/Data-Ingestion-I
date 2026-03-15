import json
import logging
import os

import polars as pl
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

SCHEMA: dict = {
    "id": pl.Int64,
    "customer_id": pl.Int64,
    "address": pl.Utf8,
    "city": pl.Utf8,
    "province": pl.Utf8,
    "created_at": pl.Datetime,
}


def get_daily_file(base_folder: str, execution_date: str) -> str:
    """getting the filename inside folder dedicated to be the place of file that will be ingested"""

    date_str = execution_date.replace("-", "")
    exact_name = f"customer_address_{date_str}.csv"
    exact_path = os.path.join(base_folder, exact_name)

    if os.path.exists(exact_path):
        logger.info("File found: %s", exact_path)
        return exact_path

    files = sorted(
        f
        for f in os.listdir(base_folder)
        if f.endswith(".csv") and os.path.isfile(os.path.join(base_folder, f))
    )
    if not files:
        raise FileNotFoundError(
            f"No CSV found in {base_folder} for date {execution_date}"
        )

    fallback = os.path.join(base_folder, files[0])
    logger.warning("Exact file not found, fallback -> %s", fallback)
    return fallback


def _load_mapping(mapping_path: str) -> dict:
    """set of dictionaries where it will be used to reconstruct raw data from daily ingestions csv file"""
    with open(mapping_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _read_csv(file_path: str) -> pl.DataFrame:
    """Read new daily CSV with expected schema."""

    logger.info("Reading: %s", file_path)
    df = pl.read_csv(file_path, schema_overrides=SCHEMA)
    logger.info("Rows read: %d", len(df))
    return df


def _clean_text_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Strip whitespace, uppercase, remove embedded newlines."""

    return df.with_columns(
        [
            pl.col(col)
            .str.strip_chars()
            .str.strip_prefix(" ")
            .str.strip_suffix(" ")
            .str.to_uppercase()
            .str.replace_all(r"\n", " ")
            for col in df.columns[2:5]
        ]
    )


def _map_regions(df: pl.DataFrame, mapping: dict) -> pl.DataFrame:
    """Standardise city and province using MappingDaerah keyword lists."""

    df = df.with_columns(
        pl.col("city")
        .str.extract_all("|".join(mapping["city"]))
        .map_elements(lambda x: x[0] if len(x) > 0 else None, return_dtype=pl.Utf8)
        .alias("city_mapped"),
        pl.col("province")
        .str.extract_all("|".join(mapping["province"]))
        .map_elements(lambda x: x[0] if len(x) > 0 else None, return_dtype=pl.Utf8)
        .alias("province_mapped"),
        pl.col("city")
        .str.extract_all("|".join(mapping["city_prefix"]))
        .map_elements(lambda x: x[0] if len(x) > 0 else None, return_dtype=pl.Utf8)
        .alias("city_prefix_mapped"),
        pl.col("province")
        .str.extract_all("|".join(mapping["province_prefix"]))
        .map_elements(lambda x: x[0] if len(x) > 0 else None, return_dtype=pl.Utf8)
        .alias("province_prefix_mapped"),
    )

    df = df.with_columns(
        pl.concat_str(
            ["province_prefix_mapped", "province_mapped"], separator=" "
        ).alias("province_final_mapped"),
        pl.concat_str(["city_prefix_mapped", "city_mapped"], separator=" ").alias(
            "city_final_mapped"
        ),
    )

    df = df.with_columns(
        pl.when(pl.col("city_final_mapped").is_not_null())
        .then(pl.col("city_final_mapped"))
        .otherwise(pl.col("city"))
        .alias("city"),
        pl.when(pl.col("province_final_mapped").is_not_null())
        .then(pl.col("province_final_mapped"))
        .otherwise(pl.col("province"))
        .alias("province"),
    )

    return df.drop(df.columns[6:])


def _load_to_mysql(df: pl.DataFrame, conn_str: str, table: str) -> None:
    """Write DataFrame to MySQL"""

    engine = create_engine(conn_str)

    with engine.begin() as conn:
        conn.execute(
            text(f"""
            CREATE TABLE IF NOT EXISTS `{table}` (
                `id`          BIGINT       NOT NULL,
                `customer_id` BIGINT       NOT NULL,
                `address`     TEXT,
                `city`        VARCHAR(255),
                `province`    VARCHAR(255),
                `created_at`  DATETIME,
                PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        )

    with engine.begin() as conn:
        df.write_database(table_name=table, connection=conn, if_table_exists="append")

    logger.info("Loaded to MySQL — table: %s, rows: %d", table, len(df))


def _archive_file(file_path: str, archive_folder: str) -> None:
    """Move processed file to archive subfolder."""
    os.makedirs(archive_folder, exist_ok=True)
    dest = os.path.join(archive_folder, os.path.basename(file_path))
    os.rename(file_path, dest)
    logger.info("Archived: %s -> %s", file_path, dest)


def task_ingest(
    base_folder, mapping_path, conn_str, target_table, archive_folder, execution_date
):
    """Full ingest pipeline: find → read → clean → map → load → archive."""
    file_path = get_daily_file(base_folder, execution_date)
    mapping = _load_mapping(mapping_path)

    df = _read_csv(file_path)
    df = _clean_text_columns(df)
    df = _map_regions(df, mapping)

    logger.info("Sample:\n%s", df.head(3))

    _load_to_mysql(df, conn_str, target_table)
    _archive_file(file_path, archive_folder)
