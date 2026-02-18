# jobs/modern_spark_job.py
"""
KaremX Order ETL Spark Job.

Processes raw order data through multiple transformation stages:
- Normalizes country values
- Parses date formats
- Cleans amount fields
- Validates and filters records
- Generates daily revenue summaries
"""

import sys
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
INPUT_PATH: str = "data/raw_orders.csv"
OUTPUT_CLEAN_PATH: str = "data/clean_orders"
OUTPUT_SUMMARY_PATH: str = "data/daily_summary"
SPARK_APP_NAME: str = "KaremX_OrderETL"

COUNTRY_MAPPING: dict[str, str] = {
    "us": "USA",
    "usa": "USA",
    "u.s.a": "USA",
    "united states": "USA",
    "uk": "UK",
    "u.k": "UK",
    "united kingdom": "UK",
    "gh": "GHANA",
    "ghana": "GHANA",
}

VALID_STATUSES: list[str] = ["PAID", "SHIPPED"]
DECIMAL_PLACES: int = 2
AMOUNT_PRECISION: float = 0.0


def _read_raw_data(spark: SparkSession) -> DataFrame:
    """
    Read raw order data from CSV file.
    
    Args:
        spark: Spark session instance.
        
    Returns:
        DataFrame: Raw order data.
        
    Raises:
        Exception: If file cannot be read.
    """
    logger.info(f"Reading raw orders from {INPUT_PATH}")
    df = spark.read.csv(
        INPUT_PATH,
        header=True,
        inferSchema=False
    )
    initial_count = df.count()
    logger.info(f"Initial row count: {initial_count}")
    return df


def _normalize_and_clean(df: DataFrame) -> DataFrame:
    """
    Apply all normalization and cleaning transformations.
    
    Args:
        df: Input DataFrame.
        
    Returns:
        DataFrame: Cleaned and normalized DataFrame.
    """
    logger.info("Starting normalization transformations")
    
    country_cleaned = F.lower(F.trim(F.col("country")))

    # Normalize country values
    logger.info("Normalizing country values")
    df = df.withColumn(
        "country",
        F.when(F.col("country").isNull() | (F.trim(F.col("country")) == ""), F.lit("UNKNOWN"))
        .when(country_cleaned.isin("us", "usa", "u.s.a", "united states"), F.lit("USA"))
        .when(country_cleaned.isin("uk", "u.k", "united kingdom"), F.lit("UK"))
        .when(country_cleaned.isin("gh", "ghana"), F.lit("GHANA"))
        .otherwise(F.upper(F.trim(F.col("country"))))
    )

    # Parse date formats
    logger.info("Parsing date formats")
    parsed_date = F.coalesce(
        F.expr("try_to_date(order_date, 'yyyy-MM-dd')"),
        F.expr("try_to_date(order_date, 'MM/dd/yyyy')"),
        F.expr("try_to_date(order_date, 'yyyy/MM/dd')"),
        F.expr("try_to_date(order_date, 'dd-MM-yyyy')"),
    )
    df = df.withColumn("order_date", F.date_format(parsed_date, "yyyy-MM-dd"))
    
    # Clean and convert amount field
    logger.info("Cleaning and converting amount field")
    # Spark 4 runs in ANSI mode by default; use try_cast so bad numeric values become null.
    df = df.withColumn("amount", F.expr("try_cast(regexp_replace(amount, ',', '') as double)"))
    
    # Normalize status field
    logger.info("Normalizing status field")
    df = df.withColumn("status", F.upper(F.trim(F.col("status"))))
    
    # Trim string fields
    logger.info("Trimming string fields")
    df = df.withColumn("order_id", F.trim(F.col("order_id")))
    df = df.withColumn("customer_id", F.trim(F.col("customer_id")))
    
    # Round amount to specified decimal places
    logger.info(f"Rounding amount to {DECIMAL_PLACES} decimal places")
    df = df.withColumn("amount", F.round(F.col("amount"), DECIMAL_PLACES))
    
    logger.info("Normalization transformations completed")
    return df


def _filter_valid_records(df: DataFrame) -> DataFrame:
    """
    Filter to keep only valid, business-critical records.
    
    Args:
        df: Input DataFrame.
        
    Returns:
        DataFrame: Filtered DataFrame.
    """
    logger.info("Filtering to valid records")
    df = df.filter(
        F.col("order_id").isNotNull() &
        (F.trim(F.col("order_id")) != "") &
        F.col("customer_id").isNotNull() &
        (F.trim(F.col("customer_id")) != "") &
        F.col("order_date").isNotNull() &
        F.col("amount").isNotNull() &
        (F.col("amount") > AMOUNT_PRECISION) &
        F.col("status").isin(*VALID_STATUSES)
    )
    
    # Deduplicate by order_id
    logger.info("Deduplicating by order_id")
    df = df.dropDuplicates(["order_id"])
    
    cleaned_count = df.count()
    logger.info(f"Cleaned row count: {cleaned_count}")
    return df


def _write_cleaned_data(df: DataFrame) -> None:
    """
    Write cleaned and normalized data to output path.
    
    Args:
        df: Cleaned DataFrame.
        
    Raises:
        Exception: If write operation fails.
    """
    logger.info(f"Writing cleaned orders to {OUTPUT_CLEAN_PATH}")
    df.select("order_id", "customer_id", "country", "order_date", "amount", "status") \
        .write.mode("overwrite").csv(OUTPUT_CLEAN_PATH, header=True)
    logger.info("Cleaned orders written successfully")


def _generate_daily_summary(df: DataFrame) -> None:
    """
    Generate and write daily revenue summary.
    
    Args:
        df: Cleaned DataFrame.
        
    Raises:
        Exception: If aggregation or write operation fails.
    """
    logger.info("Calculating daily revenue summary")
    daily_summary = df.groupBy("order_date") \
        .agg(F.round(F.sum("amount"), DECIMAL_PLACES).alias("total_revenue")) \
        .orderBy("order_date")
    
    summary_count = daily_summary.count()
    logger.info(f"Summary days: {summary_count}")
    
    logger.info(f"Writing daily summary to {OUTPUT_SUMMARY_PATH}")
    daily_summary.write.mode("overwrite").csv(OUTPUT_SUMMARY_PATH, header=True)
    logger.info("Daily summary written successfully")


def main() -> int:
    """
    Execute the ETL pipeline.
    
    Returns:
        int: Exit code (0 for success, 1 for failure).
    """
    spark: SparkSession | None = None
    try:
        logger.info("=" * 60)
        logger.info(f"Starting {SPARK_APP_NAME} ETL pipeline")
        logger.info("=" * 60)
        
        # Initialize Spark session
        logger.info("Initializing Spark session")
        spark = SparkSession.builder \
            .appName(SPARK_APP_NAME) \
            .getOrCreate()
        
        # Read raw data
        df = _read_raw_data(spark)
        
        # Apply transformations
        df = _normalize_and_clean(df)
        
        # Filter valid records
        df = _filter_valid_records(df)
        
        # Write outputs
        _write_cleaned_data(df)
        _generate_daily_summary(df)
        
        logger.info("=" * 60)
        logger.info(f"{SPARK_APP_NAME} ETL pipeline completed successfully")
        logger.info("=" * 60)
        return 0

    except FileNotFoundError as e:
        logger.error(f"Input file not found: {str(e)}", exc_info=True)
        return 1
    except Exception as e:
        logger.error(f"ETL pipeline failed with unexpected error: {str(e)}", exc_info=True)
        return 1
    finally:
        if spark:
            logger.info("Stopping Spark session")
            spark.stop()
            logger.info("Spark session stopped successfully")


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
