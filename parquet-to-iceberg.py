"""
AWS Glue ETL Job: Load Parquet Data into Iceberg Table
Purpose: Read Parquet files from Processed Zone and write to an Iceberg table
This is Step 2 of a 2-step process (CSV → Parquet → Iceberg)
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_s3_path',
    'target_database',
    'target_table',
    'iceberg_s3_path'
])

logger.info(f"Starting Glue ETL Job: {args['JOB_NAME']}")

for param in ['source_s3_path', 'target_database', 'target_table', 'iceberg_s3_path']:
    if not args.get(param) or args[param].strip() == '':
        raise ValueError(f"Required parameter '{param}' is empty or missing.")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    existing = spark.conf.get("spark.sql.catalog.glue_catalog")
    logger.info(f"glue_catalog already registered: {existing}")
except Exception:
    logger.info("glue_catalog NOT registered. Configuring manually.")
    spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", args['iceberg_s3_path'])
    spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    logger.info("glue_catalog configured manually")

# Debug: log catalog configs
all_conf = spark.sparkContext.getConf().getAll()
for key, value in all_conf:
    if 'catalog' in key.lower() or 'iceberg' in key.lower():
        logger.info(f"  {key} = {value}")

try:
    # Step 1: Read Parquet files
    logger.info(f"Reading Parquet files from: {args['source_s3_path']}")
    df = spark.read.parquet(args['source_s3_path'])

    record_count = df.count()
    logger.info(f"Records read: {record_count}")
    df.printSchema()

    if record_count == 0:
        logger.warning("No records found. Exiting.")
        job.commit()
        sys.exit(0)

    # Step 2: Create Iceberg table via CTAS under glue_catalog
    database = args['target_database']
    table_name = args['target_table']
    iceberg_path = args['iceberg_s3_path']
    catalog_table = f"glue_catalog.{database}.{table_name}"

    logger.info(f"Target: {catalog_table}")
    df.createOrReplaceTempView("parquet_source")

    create_sql = f"""
    CREATE TABLE {catalog_table}
    USING iceberg
    LOCATION '{iceberg_path}'
    AS SELECT * FROM parquet_source
    """
    logger.info(f"Executing:\n{create_sql}")
    spark.sql(create_sql)

    # Step 3: Verify
    cnt = spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog_table}").collect()[0]['cnt']

    logger.info("=" * 60)
    logger.info("PARQUET TO ICEBERG COMPLETE")
    logger.info(f"  Records loaded: {record_count}")
    logger.info(f"  Total in table: {cnt}")
    logger.info(f"  Table:          {catalog_table}")
    logger.info("=" * 60)

    job.commit()

except Exception as e:
    logger.error(f"ETL job failed: {str(e)}", exc_info=True)
    raise
