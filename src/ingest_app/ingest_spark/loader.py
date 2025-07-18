# from pyspark.sql.functions import col
import importlib
import logging

# from pyspark.sql.types import StructType, StructField, StringType, DecimalType
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

# from pyspark import SparkFiles


def derive_struct_schema_from_str(str_schema: str) -> T.StructType:
    mod_name = "pyspark.sql.types"
    mod = importlib.import_module(mod_name)

    str_schema_resolved = []
    for column in str_schema:
        # col_name = schema_row[1]
        # str_type = schema_row[2]
        col_name = column["column_name"]
        str_type = column["data_type"]

        struct_type = "StringType"
        struct_type_func = getattr(mod, struct_type)()
        if str_type.startswith("decimal"):
            _, _, suffix = str_type.rpartition("(")
            # precision_scale = f"{match}{suffix}"
            # struct_type = f"DecimalType{precision_scale}"
            precision, _, scale = suffix.replace(")", "").rpartition(",")
            struct_type = "DecimalType"
            if precision and scale:
                struct_type_func = getattr(mod, struct_type)(int(precision), int(scale))
            else:
                struct_type_func = getattr(mod, struct_type)()

        new_schema = (col_name, struct_type_func, True)
        str_schema_resolved.append(new_schema)

    struct_schema = T.StructType(
        # [T.StructField(f[0], eval(f"T.{f[1]}"), f[2]) for f in str_schema_resolved]
        # [T.StructField(f[0], getattr(mod, f[1])(), f[2]) for f in str_schema_resolved]
        [T.StructField(f[0], f[1], f[2]) for f in str_schema_resolved]
    )
    # In myScstr_schema_resolvedhema, the Spark type is provided as string.
    # Using eval makes that string to be evaluated as a method of types module.

    return struct_schema


def create_spark_dataframe(
    spark: SparkSession, source_file_path: str, schema: T.StructType
):
    # Sample data about customers from different months
    # data = [("Rajesh", "2023", "08"), ("Sunita", "2023", "09")]
    # columns = ["name", "year", "month"]

    logging.info("Reading the file %s", source_file_path)
    # print(source_file_path.split('/')[-1])
    # df = spark.createDataFrame(data, schema=columns)
    # df = spark.createDataFrame(data, schema=schema)
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("schema", schema)
        .load(source_file_path)
        # .load(SparkFiles.get(source_file_path.split('/')[-1]))
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
    load_type: str,
):

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

    db_name = qual_target_table_name.split(".")[0]
    print("tables in database %s", db_name)
    logging.info(spark.catalog.listTables(dbName=db_name))
    print(spark.catalog.listTables(dbName=db_name))

    logging.info("Loading the table %s", qual_target_table_name)

    # Check if table exists
    if spark.catalog.tableExists(tableName=qual_target_table_name):
        # Get the target table columns list. Use this to arrange the source dataframe such that the partition keys are at the end of the list.
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
    # df.show(2)


def load_file_to_table(
    spark: SparkSession,
    source_file_path: str,
    qual_target_table_name: str,
    target_database_name: str,
    partition_keys: list[str],
    cur_eff_date: str,
    str_schema: str,
    load_type: str,
):
    # spark.sparkContext.addFile(source_file_path)

    schema = derive_struct_schema_from_str(str_schema=str_schema)
    df = create_spark_dataframe(
        spark=spark, source_file_path=source_file_path, schema=schema
    )
    create_target_database(spark=spark, target_database_name=target_database_name)
    write_spark_dataframe(
        spark=spark,
        df=df,
        qual_target_table_name=qual_target_table_name,
        partition_keys=partition_keys,
        load_type=load_type,
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
