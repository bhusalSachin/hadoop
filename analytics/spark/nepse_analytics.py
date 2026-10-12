#!/usr/bin/env python3
"""Simple PySpark analytics job for NEPSE market data stored in HDFS."""

import os
import subprocess
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from config import HDFS_ANALYTICS_DIR, HDFS_PROCESSED_DIR, OUTPUT_TABLES, PROCESSED_SOURCE_DIR


def resolve_processed_source() -> str:
    """Prefer the uploaded HDFS dataset when available; otherwise use a proper file:// URI for the local source files."""
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-test", "-e", HDFS_PROCESSED_DIR],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return HDFS_PROCESSED_DIR
    except FileNotFoundError:
        pass

    if os.path.exists(str(PROCESSED_SOURCE_DIR)):
        return f"file://{PROCESSED_SOURCE_DIR}"
    return HDFS_PROCESSED_DIR


PRICE_SCHEMA = StructType(
    [
        StructField("average_120_days", DoubleType(), True),
        StructField("average_180_days", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("close_ltp", DoubleType(), True),
        StructField("close_ltp_percent", DoubleType(), True),
        StructField("difference", DoubleType(), True),
        StructField("difference_percent", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("high_52_weeks", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("low_52_weeks", DoubleType(), True),
        StructField("ltp", DoubleType(), True),
        StructField("open", DoubleType(), True),
        StructField("previous_close", DoubleType(), True),
        StructField("range", DoubleType(), True),
        StructField("range_percent", DoubleType(), True),
        StructField("symbol", StringType(), True),
        StructField("transactions", DoubleType(), True),
        StructField("turnover", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("vwap", DoubleType(), True),
        StructField("vwap_percent", DoubleType(), True),
        StructField("trade_date", StringType(), True),
        StructField("instrument_type", StringType(), True),
        StructField("pct_from_52w_high", DoubleType(), True),
        StructField("pct_vs_ma120", DoubleType(), True),
    ]
)

INDEX_SCHEMA = StructType(
    [
        StructField("trade_date", StringType(), True),
        StructField("index_id", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("pct_change", DoubleType(), True),
        StructField("point_change", DoubleType(), True),
        StructField("turnover", DoubleType(), True),
    ]
)


def load_price_dataframe(spark: SparkSession) -> DataFrame:
    """Load all processed today_share_price CSV files from HDFS or the local source directory."""
    source_path = resolve_processed_source()
    df = (
        spark.read.option("header", "true")
        .option("recursiveFileLookup", "true")
        .schema(PRICE_SCHEMA)
        .csv(source_path)
    )
    return normalize_price_dataframe(df.filter(F.input_file_name().contains("today_share_price.csv")))


def load_index_dataframe(spark: SparkSession) -> DataFrame:
    """Load all processed index_history CSV files from HDFS or the local source directory."""
    source_path = resolve_processed_source()
    df = (
        spark.read.option("header", "true")
        .option("recursiveFileLookup", "true")
        .schema(INDEX_SCHEMA)
        .csv(source_path)
    )
    return normalize_index_dataframe(df.filter(F.input_file_name().contains("index_history.csv")))


def normalize_price_dataframe(df: DataFrame) -> DataFrame:
    """Cast the raw CSV columns to the expected data types and clean obvious nulls."""
    numeric_columns = [
        "average_120_days",
        "average_180_days",
        "close",
        "close_ltp",
        "close_ltp_percent",
        "difference",
        "difference_percent",
        "high",
        "high_52_weeks",
        "low",
        "low_52_weeks",
        "ltp",
        "open",
        "previous_close",
        "range",
        "range_percent",
        "transactions",
        "turnover",
        "volume",
        "vwap",
        "vwap_percent",
        "pct_from_52w_high",
        "pct_vs_ma120",
    ]

    for column_name in numeric_columns:
        df = df.withColumn(
            column_name,
            F.when(F.col(column_name).isNull() | (F.trim(F.col(column_name).cast("string")) == ""), None)
            .otherwise(F.col(column_name).cast("double")),
        )

    df = df.withColumn("symbol", F.trim(F.col("symbol").cast("string")))
    df = df.withColumn("instrument_type", F.trim(F.col("instrument_type").cast("string")))
    df = df.withColumn("trade_date", F.to_date(F.col("trade_date"), "yyyy-MM-dd"))
    return df


def normalize_index_dataframe(df: DataFrame) -> DataFrame:
    """Clean the normalized index history dataset."""
    numeric_columns = ["open", "high", "low", "close", "pct_change", "point_change", "turnover"]
    for column_name in numeric_columns:
        df = df.withColumn(
            column_name,
            F.when(F.col(column_name).isNull() | (F.trim(F.col(column_name).cast("string")) == ""), None)
            .otherwise(F.col(column_name).cast("double")),
        )
    df = df.withColumn("trade_date", F.to_date(F.col("trade_date"), "yyyy-MM-dd"))
    return df


def write_result(df: DataFrame, output_dir: str) -> None:
    """Write a Spark DataFrame to HDFS in CSV format."""
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_dir)


def build_market_summary(df: DataFrame) -> DataFrame:
    """Market-level summary for the current dataset."""
    return df.agg(
        F.countDistinct("symbol").alias("number_of_stocks"),
        F.sum("volume").alias("total_volume"),
        F.sum("turnover").alias("total_turnover"),
        F.avg("close").alias("average_close_price"),
        F.max("close").alias("highest_close_price"),
        F.min("close").alias("lowest_close_price"),
        F.count("*").alias("number_of_records"),
        F.countDistinct("trade_date").alias("trading_dates_available"),
    )


def build_stock_performance(df: DataFrame) -> DataFrame:
    """Compute stock statistics and percentage change from the latest trading day."""
    stock_summary = df.groupBy("symbol").agg(
        F.avg("close").alias("average_close_price"),
        F.max("close").alias("max_close_price"),
        F.min("close").alias("min_close_price"),
        F.sum("volume").alias("total_trading_volume"),
        F.sum("turnover").alias("total_turnover"),
        F.count("*").alias("number_of_trading_days"),
    )

    window = Window.partitionBy("symbol").orderBy(F.col("trade_date").desc())
    latest_record = (
        df.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank")
        .withColumn(
            "percentage_change",
            F.when(F.col("open").isNotNull() & (F.col("open") != 0), ((F.col("close") - F.col("open")) / F.col("open")) * 100)
            .otherwise(None),
        )
        .select("symbol", "percentage_change", "trade_date")
    )

    return stock_summary.join(latest_record, on="symbol", how="left").orderBy(F.col("percentage_change").desc_nulls_last())


def build_top_gainers(df: DataFrame, limit_value: int = 10) -> DataFrame:
    """Return the top stock gainers by recent percentage change."""
    return (
        build_stock_performance(df)
        .filter(F.col("percentage_change").isNotNull())
        .orderBy(F.col("percentage_change").desc())
        .limit(limit_value)
    )


def build_top_losers(df: DataFrame, limit_value: int = 10) -> DataFrame:
    """Return the top stock losers by recent percentage change."""
    return (
        build_stock_performance(df)
        .filter(F.col("percentage_change").isNotNull())
        .orderBy(F.col("percentage_change").asc())
        .limit(limit_value)
    )


def build_most_traded(df: DataFrame, limit_value: int = 10) -> DataFrame:
    """Return the most traded stocks by volume."""
    stock_summary = df.groupBy("symbol").agg(
        F.sum("volume").alias("total_trading_volume"),
        F.sum("turnover").alias("total_turnover"),
        F.avg("close").alias("average_close_price"),
    )
    return stock_summary.orderBy(F.col("total_trading_volume").desc()).limit(limit_value)


def build_daily_trends(df: DataFrame) -> DataFrame:
    """Summarize market movement by trading date."""
    return (
        df.groupBy("trade_date")
        .agg(
            F.sum("volume").alias("total_volume"),
            F.sum("turnover").alias("total_turnover"),
            F.avg("close").alias("average_close_price"),
            F.countDistinct("symbol").alias("number_of_traded_stocks"),
            F.avg(((F.col("close") - F.col("open")) / F.col("open") * 100)).alias("market_pct_change"),
        )
        .orderBy("trade_date")
    )


def build_data_quality(df: DataFrame) -> DataFrame:
    """Collect basic data quality statistics."""
    spark = df.sparkSession
    all_columns = df.columns
    null_condition = F.lit(False)
    for column_name in all_columns:
        null_condition = null_condition | F.col(column_name).isNull() | (F.trim(F.col(column_name).cast("string")) == "")

    total_input = df.count()
    null_records = df.filter(null_condition).count()
    valid_records = df.filter(F.col("symbol").isNotNull() & F.col("trade_date").isNotNull()).count()
    duplicates = df.count() - df.dropDuplicates(["symbol", "trade_date"]).count()

    distinct_stocks = df.filter(F.col("symbol").isNotNull()).select("symbol").distinct().count()
    distinct_dates = df.filter(F.col("trade_date").isNotNull()).select("trade_date").distinct().count()

    return spark.createDataFrame(
        [
            (
                total_input,
                valid_records,
                null_records,
                duplicates,
                distinct_stocks,
                distinct_dates,
            )
        ],
        [
            "total_input_records",
            "valid_records",
            "null_records",
            "duplicate_records",
            "distinct_stocks",
            "distinct_trading_dates",
        ],
    )


def build_sector_analysis(df: DataFrame) -> DataFrame | None:
    """Create a sector summary only if the field exists in the dataset."""
    if "sector" not in df.columns:
        return None
    return (
        df.groupBy("sector")
        .agg(
            F.countDistinct("symbol").alias("number_of_stocks"),
            F.avg("close").alias("average_close_price"),
            F.sum("volume").alias("total_volume"),
            F.sum("turnover").alias("total_turnover"),
        )
        .orderBy(F.col("total_turnover").desc_nulls_last())
    )


def main() -> None:
    spark = SparkSession.builder.appName("nepse_big_data_analytics").getOrCreate()

    price_df = load_price_dataframe(spark)
    index_df = load_index_dataframe(spark)

    # Minimal quality checks for invalid records.
    valid_price_df = price_df.filter(
        F.col("symbol").isNotNull()
        & (F.trim(F.col("symbol")) != "")
        & F.col("trade_date").isNotNull()
        & F.col("close").isNotNull()
        & F.col("open").isNotNull()
        & F.col("volume").isNotNull()
    ).dropDuplicates(["symbol", "trade_date"])

    market_summary_df = build_market_summary(valid_price_df)
    stock_performance_df = build_stock_performance(valid_price_df)
    top_gainers_df = build_top_gainers(valid_price_df, 10)
    top_losers_df = build_top_losers(valid_price_df, 10)
    most_traded_df = build_most_traded(valid_price_df, 10)
    daily_trends_df = build_daily_trends(valid_price_df)
    data_quality_df = build_data_quality(price_df)
    sector_analysis_df = build_sector_analysis(valid_price_df)

    write_result(market_summary_df, OUTPUT_TABLES["market_summary"])
    write_result(stock_performance_df, OUTPUT_TABLES["stock_performance"])
    write_result(top_gainers_df, OUTPUT_TABLES["top_gainers"])
    write_result(top_losers_df, OUTPUT_TABLES["top_losers"])
    write_result(most_traded_df, OUTPUT_TABLES["most_traded"])
    write_result(daily_trends_df, OUTPUT_TABLES["daily_trends"])
    write_result(data_quality_df, OUTPUT_TABLES["data_quality"])

    if sector_analysis_df is not None:
        write_result(sector_analysis_df, OUTPUT_TABLES["sector_analysis"])

    print("Market summary:")
    market_summary_df.show(truncate=False)

    print("Top gainers:")
    top_gainers_df.show(truncate=False)

    print("Top losers:")
    top_losers_df.show(truncate=False)

    print("Daily trends:")
    daily_trends_df.show(truncate=False)

    if index_df.count() > 0:
        print("Index history preview:")
        index_df.select("trade_date", "index_id", "close", "pct_change").show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    spark = None
    main()
