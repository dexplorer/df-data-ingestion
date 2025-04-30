from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
import importlib
import logging
import argparse
import json
from utils import spark_io as ufs


def derive_struct_schema_from_str(target_table_schema: list[dict]) -> T.StructType:
    mod_name = "pyspark.sql.types"
    mod = importlib.import_module(mod_name)

    resolved_schema = []
    for column in target_table_schema:
        # col_name = schema_row[1]
        # str_type = schema_row[2]
        col_name = column["column_name"]
        str_type = column["data_type"]

        struct_type = "StringType"
        struct_type_func = getattr(mod, struct_type)()
        if str_type.startswith("decimal"):
            _, _, suffix = str_type.rpartition("(")
            precision, _, scale = suffix.replace(")", "").rpartition(",")
            struct_type = "DecimalType"
            if precision and scale:
                struct_type_func = getattr(mod, struct_type)(int(precision), int(scale))
            else:
                struct_type_func = getattr(mod, struct_type)()

        new_schema = (col_name, struct_type_func, True)
        resolved_schema.append(new_schema)

    struct_schema = T.StructType(
        [T.StructField(f[0], f[1], f[2]) for f in resolved_schema]
    )
    # In target_table_schema, the Spark type is provided as string.
    # Using getattr() makes that string to be evaluated as a method of types module.

    return struct_schema


def create_spark_dataframe(
    spark: SparkSession, source_file_path: str, schema: T.StructType
):
    logging.info("Reading the file %s", source_file_path)
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("schema", schema)
        .load(source_file_path)
    )
    return df


def create_target_database(
    spark: SparkSession, target_database_name: str, warehouse_path: str
):
    # spark.sql(f"DROP DATABASE IF EXISTS {target_database_name} CASCADE;")
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS {target_database_name} LOCATION '{warehouse_path}/{target_database_name}';"
    )
    # spark.sql(f"GRANT ALL ON DATABASE {target_database_name} TO USER ec2-user;")


def write_spark_dataframe(
    spark: SparkSession,
    df: DataFrame,
    qual_target_table_name: str,
    partition_keys: list[str],
    load_type: str,
):

    # Drop the table for testing
    # spark.sql(f"drop table if exists {qual_target_table_name}")

    logging.info("Loading the table %s", qual_target_table_name)

    # Check if table exists
    if spark.catalog.tableExists(tableName=qual_target_table_name):
        # Get the target table columns list.
        # Use this to arrange the source dataframe such that the partition keys are at the end of the list.
        target_table_columns = spark.catalog.listColumns(
            tableName=qual_target_table_name
        )
        # df.select() does not work with type list[Column]. It works with list[str].
        target_table_column_names = [c.name for c in target_table_columns]

        if load_type == "incremental" and partition_keys:
            logging.info(
                "Table %s exists. Overwriting the partitions if they exist.",
                qual_target_table_name,
            )
            # Overwrite dynamic partitions only (enable spark conf option), idempotent
            df.select(target_table_column_names).write.insertInto(
                tableName=qual_target_table_name, overwrite=True
            )
        elif load_type == "incremental":
            logging.info("Table %s exists. Appending data.", qual_target_table_name)
            logging.info(
                "***Caution***. Re-run of the process could load duplicates in the target."
            )
            # Append only, not idempotent
            df.select(target_table_column_names).write.insertInto(
                tableName=qual_target_table_name
            )
        elif load_type == "full" and partition_keys:
            logging.info(
                "Table %s exists. Overwriting the entire table with new partitions.",
                qual_target_table_name,
            )
            # Overwrite entire table and define partitions on new table, idempotent
            df.write.saveAsTable(
                name=qual_target_table_name,
                format="parquet",
                mode="overwrite",
                partitionBy=partition_keys,
            )
        elif load_type == "full":
            logging.info(
                "Table %s exists. Overwriting the entire table.", qual_target_table_name
            )
            # Overwrite entire table, idempotent
            df.write.saveAsTable(
                name=qual_target_table_name,
                format="parquet",
                mode="overwrite",
            )
        else:
            raise ValueError(
                "Ingestion pattern is not supported. Check load_type and partition keys in metadata."
            )
    else:
        if partition_keys:
            logging.info(
                "Table %s does not exist. Creating a new table with partition.",
                qual_target_table_name,
            )
            df.write.saveAsTable(
                name=qual_target_table_name,
                format="parquet",
                partitionBy=partition_keys,
            )
        else:
            logging.info(
                "Table %s does not exist. Creating a new table.", qual_target_table_name
            )
            df.write.saveAsTable(
                name=qual_target_table_name,
                format="parquet",
                partitionBy=partition_keys,
            )
        # spark.sql(f"GRANT ALL ON TABLE {qual_target_table_name} TO USER ec2-user;")


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
            "Load is successful. Source Record Count = %d, Target Record Count = %d",
            source_record_count,
            target_record_count,
        )
    else:
        logging.error(
            "Load is unsuccessful. Source Record Count = %d, Target Record Count = %d",
            source_record_count,
            target_record_count,
        )

    return target_record_count


def view_loaded_data_sample(
    spark: SparkSession, qual_target_table_name: str, cur_eff_date: str
):
    df = spark.sql(
        f"SELECT * FROM {qual_target_table_name} WHERE EFFECTIVE_DATE='{cur_eff_date}';"
    )
    df.printSchema()
    df.show(2)


def load_file_to_table(
    spark: SparkSession,
    source_file_path: str,
    qual_target_table_name: str,
    target_database_name: str,
    partition_keys: str,
    cur_eff_date: str,
    target_table_schema: str,
    load_type: str,
    warehouse_path: str,
    debug: str, 
):
    print("Databases in catalog:")
    logging.info(spark.catalog.listDatabases())
    print(spark.catalog.listDatabases())

    # logging.info("Tables in database %s", target_database_name)
    # logging.info(spark.catalog.listTables(dbName=target_database_name))

    schema = derive_struct_schema_from_str(
        target_table_schema=json.loads(target_table_schema)
    )  # convert to list[dict]
    df = create_spark_dataframe(
        spark=spark, source_file_path=source_file_path, schema=schema
    )
    create_target_database(
        spark=spark,
        target_database_name=target_database_name,
        warehouse_path=warehouse_path,
    )
    write_spark_dataframe(
        spark=spark,
        df=df,
        qual_target_table_name=qual_target_table_name,
        partition_keys=json.loads(partition_keys),  # convert to list[str]
        load_type=load_type,
    )
    if debug == 'y':
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


def main():
    parser = argparse.ArgumentParser(description="Spark Application - Loader")
    parser.add_argument(
        "--warehouse_path",
        help="Hive or Spark warehouse path.",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--spark_master_uri",
        help="Spark resource or cluster manager",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--spark_history_log_dir",
        help="Spark log events directory.",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--spark_local_dir",
        help="Spark local work directory.",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--postgres_uri",
        help="Postgres host which is used as hive metastore.",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--source_file_path",
        help="",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--qual_target_table_name",
        help="",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--target_database_name",
        help="",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--partition_keys",
        help="",
        # nargs="*",  # 0 or many values
        nargs=None,  # 1 value (comma separated string)
        required=True,
    )
    parser.add_argument(
        "--cur_eff_date",
        help="",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--target_table_schema",
        help="",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--load_type",
        help="",
        nargs=None,  # 1 value
        required=True,
    )
    parser.add_argument(
        "--debug",
        help="Set the logging level to DEBUG",
        nargs="?",  # 0-or-1 argument values
        const="y",  # default when the argument is provided with no value
        default="n",  # default when the argument is not provided
        required=False,
    )

    # Get the arguments
    args = vars(parser.parse_args())
    warehouse_path = args["warehouse_path"]
    spark_master_uri = args["spark_master_uri"]
    spark_history_log_dir = args["spark_history_log_dir"]
    spark_local_dir = args["spark_local_dir"]
    postgres_uri = args["postgres_uri"]
    source_file_path = args["source_file_path"]
    qual_target_table_name = args["qual_target_table_name"]
    target_database_name = args["target_database_name"]
    partition_keys = args["partition_keys"]
    cur_eff_date = args["cur_eff_date"]
    target_table_schema = args["target_table_schema"]
    load_type = args["load_type"]
    debug = args["debug"]

    if debug == "y":
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.info("Requested log level: %s", str(log_level))

    # Create Spark session
    spark = ufs.create_spark_session(
        warehouse_path=warehouse_path,
        spark_master_uri=spark_master_uri,
        spark_history_log_dir=spark_history_log_dir,
        spark_local_dir=spark_local_dir,
        postgres_uri=postgres_uri,
    )

    # Load the file
    records = load_file_to_table(
        spark=spark,
        source_file_path=source_file_path,
        qual_target_table_name=qual_target_table_name,
        target_database_name=target_database_name,
        partition_keys=partition_keys,
        cur_eff_date=cur_eff_date,
        target_table_schema=target_table_schema,
        load_type=load_type,
        warehouse_path=warehouse_path,
        debug=debug,
    )

    logging.info("%d records are loaded into %s.", records, qual_target_table_name)


if __name__ == "__main__":
    main()
