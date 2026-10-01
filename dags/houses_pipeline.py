"""Airflow DAG for final assignment No. 8: PySpark -> ClickHouse."""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import shutil
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Sequence

import clickhouse_connect
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


LOGGER = logging.getLogger(__name__)

PUBLIC_URL = "https://disk.yandex.ru/d/bhf2M8C557AFVw"
PUBLIC_ARCHIVE_PATH = "/archive (12).zip"
ARCHIVE_MD5 = "0818373a143517407605f3725acf3db0"
CSV_NAME = "russian_houses.csv"
SOURCE_ENCODING = "utf-16"
SPARK_CSV_ENCODING = "UTF-8"
MIN_VALID_YEAR = 1800

RAW_COLUMNS = [
    "house_id",
    "latitude",
    "longitude",
    "maintenance_year",
    "square",
    "population",
    "region",
    "locality_name",
    "address",
    "full_address",
    "communal_service_id",
    "description",
]

CLICKHOUSE_COLUMNS = RAW_COLUMNS + ["decade", "source_filename"]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS analytics.houses
(
    house_id UInt64,
    latitude Nullable(Float64),
    longitude Nullable(Float64),
    maintenance_year Nullable(Int32),
    square Nullable(Float64),
    population Nullable(Int32),
    region LowCardinality(Nullable(String)),
    locality_name Nullable(String),
    address Nullable(String),
    full_address Nullable(String),
    communal_service_id Nullable(Int64),
    description Nullable(String),
    decade Nullable(Int32),
    source_filename LowCardinality(String),
    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY house_id
"""

TOP_25_SQL = """
SELECT
    house_id,
    region,
    locality_name,
    address,
    square,
    maintenance_year
FROM analytics.houses
WHERE square > 60
ORDER BY square DESC, house_id ASC
LIMIT 25
"""


def _clickhouse_settings() -> dict[str, object]:
    return {
        "host": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
        "username": os.getenv("CLICKHOUSE_USER", "airflow"),
        "password": os.getenv("CLICKHOUSE_PASSWORD", "airflow"),
        "database": os.getenv("CLICKHOUSE_DATABASE", "analytics"),
        "connect_timeout": 10,
        "send_receive_timeout": 300,
    }


def _get_clickhouse_client(attempts: int = 12):
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            client = clickhouse_connect.get_client(**_clickhouse_settings())
            client.command("SELECT 1")
            return client
        except Exception as error:  # connection can lag behind container healthcheck
            last_error = error
            LOGGER.warning("ClickHouse connection attempt %s/%s failed: %s", attempt, attempts, error)
            time.sleep(min(attempt * 2, 10))
    raise RuntimeError("Could not connect to ClickHouse") from last_error


def _download_source(data_dir: Path) -> Path:
    archive_path = data_dir / "raw" / "houses.zip"
    csv_path = data_dir / "raw" / CSV_NAME
    archive_path.parent.mkdir(parents=True, exist_ok=True)

    if csv_path.exists():
        LOGGER.info("Using existing extracted file: %s", csv_path)
        return csv_path

    if not archive_path.exists() or _md5(archive_path) != ARCHIVE_MD5:
        LOGGER.info("Downloading source archive from %s", PUBLIC_URL)
        response = requests.get(
            "https://cloud-api.yandex.net/v1/disk/public/resources/download",
            params={"public_key": PUBLIC_URL, "path": PUBLIC_ARCHIVE_PATH},
            timeout=60,
        )
        response.raise_for_status()
        download_url = response.json()["href"]
        temporary_path = archive_path.with_suffix(".zip.part")
        with requests.get(download_url, stream=True, timeout=300) as download:
            download.raise_for_status()
            with temporary_path.open("wb") as target:
                for chunk in download.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        target.write(chunk)
        temporary_path.replace(archive_path)

    actual_md5 = _md5(archive_path)
    if actual_md5 != ARCHIVE_MD5:
        raise ValueError(f"Archive checksum mismatch: expected {ARCHIVE_MD5}, got {actual_md5}")

    with zipfile.ZipFile(archive_path) as source_zip:
        member = source_zip.getinfo(CSV_NAME)
        destination = (archive_path.parent / member.filename).resolve()
        if archive_path.parent.resolve() not in destination.parents:
            raise ValueError("Unsafe path in ZIP archive")
        source_zip.extract(member, archive_path.parent)

    return csv_path


def _md5(path: Path) -> str:
    digest = hashlib.md5()  # noqa: S324 - verifies the publisher-provided checksum
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_header(csv_path: Path) -> None:
    # ``utf-16`` consumes the BOM and correctly detects the little-endian source.
    with csv_path.open("r", encoding=SOURCE_ENCODING, newline="") as source:
        header = next(csv.reader(source))
    if header != RAW_COLUMNS:
        raise ValueError(f"Unexpected CSV header: {header}")


def _inspect_physical_lines(csv_path: Path) -> tuple[int, int]:
    physical_line_count = 0
    blank_line_count = 0
    with csv_path.open("r", encoding=SOURCE_ENCODING) as source:
        for line in source:
            physical_line_count += 1
            blank_line_count += int(not line.strip())
    return physical_line_count, blank_line_count


def _ensure_spark_readable_copy(csv_path: Path, data_dir: Path) -> Path:
    """Transcode UTF-16LE to splittable UTF-8 without changing source records."""
    utf8_path = data_dir / "normalized" / "russian_houses_utf8.csv"
    if utf8_path.exists() and utf8_path.stat().st_mtime_ns >= csv_path.stat().st_mtime_ns:
        return utf8_path

    utf8_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = utf8_path.with_suffix(".csv.part")
    with (
        csv_path.open("r", encoding=SOURCE_ENCODING, newline="") as source,
        temporary_path.open("w", encoding="utf-8", newline="") as target,
    ):
        shutil.copyfileobj(source, target, length=1024 * 1024)
    temporary_path.replace(utf8_path)
    LOGGER.info("Created UTF-8 working copy for parallel Spark reads: %s", utf8_path)
    return utf8_path


def _string_or_null(column_name: str):
    value = F.trim(F.col(column_name))
    return F.when((value == "") | F.col(column_name).isNull(), F.lit(None)).otherwise(value)


def _number_text(column_name: str):
    return F.regexp_replace(
        F.regexp_replace(_string_or_null(column_name), "[\\s\\u00a0]", ""),
        ",",
        ".",
    )


def _clean(raw_df: DataFrame) -> DataFrame:
    numeric_year = _number_text("maintenance_year").cast("double").cast("integer")
    return raw_df.select(
        _number_text("house_id").cast("long").alias("house_id"),
        _number_text("latitude").cast("double").alias("latitude"),
        _number_text("longitude").cast("double").alias("longitude"),
        F.when(numeric_year.between(MIN_VALID_YEAR, date.today().year), numeric_year)
        .otherwise(F.lit(None))
        .cast("integer")
        .alias("maintenance_year"),
        _number_text("square").cast("double").alias("square"),
        _number_text("population").cast("double").cast("integer").alias("population"),
        _string_or_null("region").alias("region"),
        _string_or_null("locality_name").alias("locality_name"),
        _string_or_null("address").alias("address"),
        _string_or_null("full_address").alias("full_address"),
        _number_text("communal_service_id").cast("double").cast("long").alias("communal_service_id"),
        _string_or_null("description").alias("description"),
    )


def _collect_dicts(frame: DataFrame) -> list[dict[str, object]]:
    return [row.asDict(recursive=True) for row in frame.collect()]


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def run_pipeline() -> None:
    data_dir = Path(os.getenv("HOUSES_DATA_DIR", "/opt/airflow/data"))
    output_dir = data_dir / "output"
    expected_count = int(os.getenv("EXPECTED_ROW_COUNT", "590707"))
    csv_path = _download_source(data_dir)
    _validate_header(csv_path)
    spark_csv_path = _ensure_spark_readable_copy(csv_path, data_dir)

    spark = (
        SparkSession.builder.appName("houses-final-assignment")
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .config("spark.local.dir", str(data_dir / "spark-tmp"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_schema = StructType([StructField(name, StringType(), True) for name in RAW_COLUMNS])
    raw_df: DataFrame | None = None
    houses_df: DataFrame | None = None
    try:
        raw_df = (
            spark.read.option("header", True)
            .option("encoding", SPARK_CSV_ENCODING)
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "FAILFAST")
            .schema(raw_schema)
            .csv(str(spark_csv_path))
            .persist(StorageLevel.DISK_ONLY)
        )

        row_count = raw_df.count()
        LOGGER.info("CSV row count: %s", row_count)
        if row_count != expected_count:
            raise ValueError(f"Expected {expected_count} rows, read {row_count}")

        physical_line_count, blank_line_count = _inspect_physical_lines(csv_path)
        empty_record_filter = F.lit(True)
        for column_name in RAW_COLUMNS:
            empty_record_filter = empty_record_filter & (
                F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")
            )
        empty_record_count = raw_df.where(empty_record_filter).count()
        LOGGER.info(
            "Physical lines (including header): %s; blank physical lines: %s; empty parsed records: %s",
            physical_line_count,
            blank_line_count,
            empty_record_count,
        )
        if physical_line_count != expected_count + 1:
            raise ValueError(
                f"Expected {expected_count + 1} physical lines including header, got {physical_line_count}"
            )
        if blank_line_count or empty_record_count:
            raise ValueError("The source contains blank lines or empty records")

        houses_df = _clean(raw_df).persist(StorageLevel.DISK_ONLY)
        invalid_id_count = houses_df.where(F.col("house_id").isNull()).count()
        duplicate_id_count = (
            houses_df.where(F.col("house_id").isNotNull())
            .groupBy("house_id")
            .count()
            .where(F.col("count") > 1)
            .count()
        )
        if invalid_id_count or duplicate_id_count:
            raise ValueError(
                f"Invalid house IDs: {invalid_id_count}; duplicated IDs: {duplicate_id_count}"
            )

        numeric_quality = houses_df.agg(
            F.count(F.when(F.col("maintenance_year").isNull(), 1)).alias("missing_years"),
            F.count(F.when(F.col("square").isNull(), 1)).alias("missing_squares"),
            F.count(F.when(F.col("latitude").isNull() | F.col("longitude").isNull(), 1)).alias(
                "missing_coordinates"
            ),
        ).first()
        LOGGER.info("Numeric-field quality: %s", numeric_quality.asDict())

        year_stats = houses_df.agg(
            F.avg("maintenance_year").alias("average_year"),
            F.expr("percentile_approx(maintenance_year, 0.5, 1000000)").alias("median_year"),
        )

        top_regions = (
            houses_df.where(F.col("region").isNotNull())
            .groupBy("region")
            .count()
            .orderBy(F.desc("count"), F.asc("region"))
            .limit(10)
        )
        top_cities = (
            houses_df.where(F.col("locality_name").isNotNull())
            .groupBy("locality_name")
            .count()
            .orderBy(F.desc("count"), F.asc("locality_name"))
            .limit(10)
        )

        area_base = houses_df.where(F.col("region").isNotNull() & F.col("square").isNotNull())
        max_window = Window.partitionBy("region").orderBy(F.desc("square"), F.asc("house_id"))
        min_window = Window.partitionBy("region").orderBy(F.asc("square"), F.asc("house_id"))
        max_area_houses = (
            area_base.withColumn("position", F.row_number().over(max_window))
            .where(F.col("position") == 1)
            .select(
                "region", "house_id", "locality_name", "address", "square"
            )
            .withColumn("extreme", F.lit("max"))
        )
        min_area_houses = (
            area_base.withColumn("position", F.row_number().over(min_window))
            .where(F.col("position") == 1)
            .select(
                "region", "house_id", "locality_name", "address", "square"
            )
            .withColumn("extreme", F.lit("min"))
        )
        area_extremes = max_area_houses.unionByName(min_area_houses).orderBy("region", "extreme")

        houses_with_decade = houses_df.withColumn(
            "decade",
            F.when(
                F.col("maintenance_year").isNotNull(),
                (F.floor(F.col("maintenance_year") / 10) * 10).cast("integer"),
            ),
        )
        buildings_by_decade = (
            houses_with_decade.where(F.col("decade").isNotNull())
            .groupBy("decade")
            .count()
            .orderBy("decade")
        )

        analytics = {
            "source": PUBLIC_URL,
            "row_count": row_count,
            "physical_line_count_including_header": physical_line_count,
            "blank_line_count": blank_line_count,
            "empty_record_count": empty_record_count,
            "numeric_quality": numeric_quality.asDict(),
            "year_statistics": _collect_dicts(year_stats)[0],
            "top_10_regions": _collect_dicts(top_regions),
            "top_10_cities": _collect_dicts(top_cities),
            "area_extremes_by_region": _collect_dicts(area_extremes),
            "buildings_by_decade": _collect_dicts(buildings_by_decade),
        }
        _write_json(output_dir / "analytics.json", analytics)
        LOGGER.info("Analytics results:\n%s", json.dumps(analytics, ensure_ascii=False, indent=2, default=str))

        load_df = (
            houses_with_decade.withColumn("source_filename", F.lit(CSV_NAME))
            .select(*CLICKHOUSE_COLUMNS)
            .repartition(4)
        )
        client = _get_clickhouse_client()
        try:
            client.command(CREATE_TABLE_SQL)
            client.command("TRUNCATE TABLE analytics.houses")
        finally:
            client.close()

        # Airflow imports DAGs under a generated module name. A top-level callback
        # would make Spark workers try to import that temporary name. Keeping the
        # callback local makes cloudpickle ship its code by value.
        insert_settings = _clickhouse_settings()
        insert_columns = tuple(CLICKHOUSE_COLUMNS)

        def insert_partition(rows: Iterable[Row]) -> None:
            import clickhouse_connect as worker_clickhouse_connect

            worker_client = worker_clickhouse_connect.get_client(**insert_settings)
            batch: list[Sequence[object]] = []
            try:
                for row in rows:
                    batch.append(tuple(row[column] for column in insert_columns))
                    if len(batch) >= 5_000:
                        worker_client.insert("houses", batch, column_names=insert_columns)
                        batch.clear()
                if batch:
                    worker_client.insert("houses", batch, column_names=insert_columns)
            finally:
                worker_client.close()

        load_df.foreachPartition(insert_partition)

        client = _get_clickhouse_client()
        try:
            loaded_count = int(client.command("SELECT count() FROM analytics.houses"))
            if loaded_count != row_count:
                raise ValueError(f"ClickHouse contains {loaded_count} rows; expected {row_count}")

            result = client.query(TOP_25_SQL)
            top_25 = [dict(zip(result.column_names, row)) for row in result.result_rows]
            _write_json(output_dir / "top_25_houses_over_60.json", top_25)
            LOGGER.info(
                "Top 25 houses with square > 60 m2:\n%s",
                json.dumps(top_25, ensure_ascii=False, indent=2, default=str),
            )
        finally:
            client.close()
    finally:
        if houses_df is not None:
            houses_df.unpersist()
        if raw_df is not None:
            raw_df.unpersist()
        spark.stop()


with DAG(
    dag_id="houses_final_assignment",
    description="Download, validate and analyze Russian houses with PySpark, then load ClickHouse",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "student", "retries": 1, "retry_delay": timedelta(seconds=15)},
    tags=["pyspark", "clickhouse", "final-assignment"],
) as dag:
    run_all_steps = PythonOperator(
        task_id="download_transform_analyze_load_and_query",
        python_callable=run_pipeline,
        execution_timeout=None,
    )

