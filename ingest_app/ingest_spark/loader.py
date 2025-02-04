from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql.functions import col

from ingest_app import settings as sc

import logging


def create_spark_session(warehouse_path) -> SparkSession:
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("Spark Loader in Ingestion Workflow")
        .config("spark.sql.warehouse.dir", warehouse_path)
        .enableHiveSupport()
        .getOrCreate()
    )

    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark


def define_file_schema() -> StructType:
    # Define the file schema
    schema = StructType(
        [
            StructField("effective_date", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("asset_id", StringType(), True),
            StructField("asset_value", DecimalType(25, 2), True),
        ]
    )
    return schema


def create_spark_dataframe(
    spark: SparkSession, source_file_path: str, schema: StructType
):
    # Sample data about customers from different months
    # data = [("Rajesh", "2023", "08"), ("Sunita", "2023", "09")]
    # columns = ["name", "year", "month"]

    logging.info("Reading the file %s", source_file_path)
    # df = spark.createDataFrame(data, schema=columns)
    # df = spark.createDataFrame(data, schema=schema)
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("schema", schema)
        .load(source_file_path)
    )
    return df


def create_target_database(spark: SparkSession, target_database_name: str):
    # Create database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_database_name};")


def write_spark_dataframe(
    spark: SparkSession,
    df: DataFrame,
    qual_target_table_name: str,
    partition_keys: list[str],
):
    # Write data partitioned by year and month
    # df.write.partitionBy("year", "month").mode("overwrite").format("parquet").save("/workspaces/df-data-ingestion/ingest_app/data/output/")

    # df.write \
    # .partitionBy("effective_date") \
    # .mode("overwrite") \
    # .format("parquet") \
    # .save(target_file_path)

    # spark.catalog.refreshTable
    # NoSuchObjectException
    # spark.catalog.tableExists

    # Drop the table for testing
    # spark.sql(f"drop table if exists {qual_target_table_name}")

    logging.info("Loading the table %s", qual_target_table_name)

    # Check if table exists
    if spark.catalog.tableExists(tableName=qual_target_table_name):
        logging.debug("Table exists. Insert/overwrite the partition.")

        # Get the target table columns list. Use this to arrange the source dataframe such that the partition keys are at the end of the list.
        target_table_columns = spark.catalog.listColumns(
            tableName=qual_target_table_name
        )
        # df.select() does not work with type list[Column]. It works with list[str].
        target_table_column_names = [c.name for c in target_table_columns]

        df.select(target_table_column_names).write.insertInto(
            tableName=qual_target_table_name, overwrite=True
        )
    else:
        logging.debug("Table does not exist. Creating a new table with partition.")
        df.write.saveAsTable(
            name=qual_target_table_name,
            format="parquet",
            mode="overwrite",
            partitionBy=partition_keys,
        )


def validate_load(
    spark: SparkSession,
    source_df: DataFrame,
    qual_target_table_name: str,
    cur_eff_date: str,
):
    df = spark.sql(
        f"SELECT COUNT(1) AS ROW_COUNT FROM {qual_target_table_name} WHERE EFFECTIVE_DATE='{cur_eff_date}';"
    )
    target_record_count = df.first()["ROW_COUNT"]
    source_record_count = source_df.count()

    if source_record_count == target_record_count:
        logging.info(
            f"Load is successful. Source Record Count = {source_record_count}, Target Record Count = {target_record_count}"
        )
    else:
        logging.error(
            f"Load is unsuccessful. Source Record Count = {source_record_count}, Target Record Count = {target_record_count}"
        )

    return target_record_count


def view_loaded_data_sample(spark: SparkSession, qual_target_table_name: str, cur_eff_date: str):
    df = spark.sql(
        f"SELECT * FROM {qual_target_table_name} WHERE EFFECTIVE_DATE='{cur_eff_date}';"
    )
    df.printSchema()
    df.show(2)


def load_file_to_table(
    source_file_path: str,
    qual_target_table_name: str,
    target_database_name: str,
    target_table_name: str,
    partition_keys: list[str],
    cur_eff_date: str,
):
    spark = create_spark_session(warehouse_path=sc.warehouse_path)
    schema = define_file_schema()
    df = create_spark_dataframe(
        spark=spark, source_file_path=source_file_path, schema=schema
    )
    create_target_database(spark=spark, target_database_name=target_database_name)
    write_spark_dataframe(
        spark=spark,
        df=df,
        qual_target_table_name=qual_target_table_name,
        partition_keys=partition_keys,
    )
    view_loaded_data_sample(
        spark=spark,
        qual_target_table_name=qual_target_table_name,
        cur_eff_date=cur_eff_date,
    )
    target_record_count = validate_load(
        spark=spark,
        source_df=df,
        qual_target_table_name=qual_target_table_name,
        cur_eff_date=cur_eff_date,
    )

    return target_record_count
