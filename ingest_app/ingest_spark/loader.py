
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

import logging

def create_spark_session():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CSV to Parquet with PartitionOverwrite") \
        .getOrCreate()

    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark

def define_file_schema():
    # Define the file schema
    schema = StructType([
        StructField("effective_date", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("asset_id", StringType(), True), 
        StructField("asset_value", DecimalType(25, 2), True)
    ])
    return schema

def create_spark_dataframe(spark_session, source_file_path):
    # Sample data about customers from different months
    # data = [("Rajesh", "2023", "08"), ("Sunita", "2023", "09")]
    # columns = ["name", "year", "month"]

    logging.info("Reading the file %s", source_file_path)
    # df = spark_session.createDataFrame(data, schema=columns)
    # df = spark_session.createDataFrame(data, schema=schema)
    df = spark_session.read \
        .format("csv") \
        .option("header", "true") \
        # .option("inferSchema", "true") \
        .option("schema", schema) \
        .load(source_file_path)
    return df 

def write_spark_dataframe(df):
    # Write data partitioned by year and month
    # df.write.partitionBy("year", "month").mode("overwrite").format("parquet").save("/workspaces/df-data-ingestion/ingest_app/data/output/")
    df.write \
    .partitionBy("effective_date") \
    .mode("overwrite") \
    .format("parquet") \
    .save(target_file_path)

def load_file_to_table(source_file_path, target_file_path):
    spark = create_spark_session()
    schema = define_file_schema()
    df = create_spark_dataframe(spark_session=spark)
    write_spark_dataframe(df)
    return df.shape()
    