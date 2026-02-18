"""
AWS Glue ETL Job: CSV to Parquet Transformation
Purpose: Transform eCommerce behaviour CSV data from Raw Zone to Parquet format in Processed Zone
This is Step 1 of a 2-step process (CSV → Parquet → Iceberg)

CSV Schema:
- event_time: When event happened (UTC); timestamp
- event_type: Event type [view, cart, remove_from_cart, purchase]; string
- product_id: Product ID; string
- category_id: Product category ID; string (MUST remain string, not numeric)
- category_code: Category meaningful name (if present); string
- brand: Brand name in lower case (if present); string
- price: Product price; double
- user_id: Permanent user ID; string
- user_session: User session ID; string

Note: Glue crawler may infer complex struct types for columns with mixed data.
This script normalizes all columns to strings first, then applies proper type casting.
The script processes ALL CSV files in the source location via the Glue Catalog table.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, to_timestamp, substring, trim, when
from pyspark.sql.types import DoubleType, TimestampType, StringType
import logging

# Configure logging for CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def validate_required_columns(df, required_columns):
    """
    Validate that all required columns exist in the DataFrame.
    Requirement 7.1: Required column validation
    """
    missing_columns = [col_name for col_name in required_columns if col_name not in df.columns]
    if missing_columns:
        error_msg = f"Missing required columns: {missing_columns}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    logger.info(f"All required columns present: {required_columns}")
    return True

def log_data_quality_metrics(df, stage_name):
    """
    Log data quality metrics for monitoring.
    Requirement 7.5: Data quality reporting
    """
    record_count = df.count()
    logger.info(f"[{stage_name}] Record count: {record_count}")
    return record_count

def detect_and_log_invalid_records(df, required_columns):
    """
    Detect records with null values in required columns.
    Requirement 7.3: Type conversion validation
    """
    null_counts = {}
    for col_name in required_columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_counts[col_name] = null_count
        if null_count > 0:
            logger.warning(f"Column '{col_name}' has {null_count} null values")
    
    return null_counts

# Initialize Glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_database',
    'source_table',
    'target_s3_path'
])

logger.info(f"Starting Glue ETL Job: {args['JOB_NAME']}")
logger.info(f"Source: {args['source_database']}.{args['source_table']}")
logger.info(f"Target: {args['target_s3_path']}")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    # Read from Glue Data Catalog (Raw Zone)
    logger.info("Reading data from Glue Data Catalog")
    source_dyf = glueContext.create_dynamic_frame.from_catalog(
        database=args['source_database'],
        table_name=args['source_table'],
        transformation_ctx="source_dyf"
    )
    
    # Convert to Spark DataFrame for transformations
    df = source_dyf.toDF()
    
    # Data Quality Check: Initial count
    initial_count = log_data_quality_metrics(df, "Initial Load")
    
    # Validate required columns
    required_columns = ['event_time', 'event_type', 'product_id', 'user_id']
    validate_required_columns(df, required_columns)
    
    # Data Transformations
    logger.info("Applying data transformations")
    
    # First, handle struct types from crawler by extracting the actual values
    # The crawler may infer complex struct types for columns with mixed data
    logger.info("Flattening struct columns if present")
    for column in df.columns:
        col_type = str(df.schema[column].dataType)
        if "struct" in col_type.lower():
            # Extract the first non-null field from the struct
            logger.info(f"Column '{column}' is a struct type, flattening...")
            # Get struct fields
            struct_fields = df.schema[column].dataType.fieldNames()
            # Try to extract the first field that looks like data (not 'null')
            for field in struct_fields:
                if field.lower() != 'null':
                    df = df.withColumn(column, col(f"{column}.{field}"))
                    break
        # Cast everything to string first to ensure clean data
        df = df.withColumn(column, col(column).cast(StringType()))
    
    # Now apply proper transformations with correct data types
    df_transformed = df \
        .withColumn("event_time", col("event_time").cast(TimestampType())) \
        .withColumn("event_date", to_date(col("event_time"))) \
        .withColumn("event_type", trim(col("event_type"))) \
        .withColumn("product_id", trim(col("product_id"))) \
        .withColumn("category_id", trim(col("category_id"))) \
        .withColumn("category_code", trim(col("category_code"))) \
        .withColumn("brand", trim(col("brand"))) \
        .withColumn("price", col("price").cast(DoubleType())) \
        .withColumn("user_id", trim(col("user_id"))) \
        .withColumn("user_session", trim(col("user_session")))
    
    # Handle null values for optional fields
    df_transformed = df_transformed \
        .withColumn("category_code", when(col("category_code") == "", None)
                    .otherwise(col("category_code"))) \
        .withColumn("brand", when(col("brand") == "", None)
                    .otherwise(col("brand")))
    
    # Log transformation metrics
    transformation_count = log_data_quality_metrics(df_transformed, "After Transformation")
    
    # Detect invalid records (null values in required columns after transformation)
    logger.info("Checking for invalid records")
    null_counts = detect_and_log_invalid_records(df_transformed, required_columns)
    invalid_record_count = sum(null_counts.values())
    
    # Filter out records with null values in required columns
    if invalid_record_count > 0:
        logger.warning(f"Filtering out {invalid_record_count} invalid records with null required fields")
        for col_name in required_columns:
            df_transformed = df_transformed.filter(col(col_name).isNotNull())
    
    valid_count = log_data_quality_metrics(df_transformed, "After Filtering Invalid Records")
    
    # Remove duplicates based on composite key
    logger.info("Removing duplicate records based on composite key")
    composite_key = ['event_time', 'user_id', 'product_id', 'event_type']
    df_deduplicated = df_transformed.dropDuplicates(composite_key)
    
    final_count = log_data_quality_metrics(df_deduplicated, "After Deduplication")
    duplicate_count = valid_count - final_count
    logger.info(f"Duplicate records removed: {duplicate_count}")
    
    # Write to Parquet format with partitioning
    logger.info("Writing data to Parquet format")
    logger.info(f"Target path: {args['target_s3_path']}")
    
    df_deduplicated.write \
        .mode("overwrite") \
        .partitionBy("event_date", "event_type") \
        .parquet(args['target_s3_path'])
    
    # Final Data Quality Report
    logger.info("=" * 80)
    logger.info("DATA QUALITY REPORT - CSV TO PARQUET")
    logger.info("=" * 80)
    logger.info(f"Initial record count: {initial_count}")
    logger.info(f"Invalid records (null required fields): {invalid_record_count}")
    logger.info(f"Valid records after filtering: {valid_count}")
    logger.info(f"Duplicate records removed: {duplicate_count}")
    logger.info(f"Final record count written: {final_count}")
    logger.info(f"Data quality success rate: {(final_count / initial_count * 100):.2f}%")
    logger.info(f"Output format: Parquet (partitioned by event_date, event_type)")
    logger.info("=" * 80)
    
    logger.info("CSV to Parquet transformation completed successfully")
    job.commit()

except Exception as e:
    logger.error(f"ETL job failed with error: {str(e)}", exc_info=True)
    raise
