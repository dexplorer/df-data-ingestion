from pyspark.sql import SparkSession


def create_spark_session(warehouse_path) -> SparkSession:
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("Spark Extracter in Distribution Workflow")
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            "jdbc:derby:;databaseName=/workspaces/datalake/user/hive/metastore_db;create=true",
        )
        .config(
            "spark.driver.extraJavaOptions",
            "-Dderby.system.home=/workspaces/datalake/user/hive",
        )
        .config("spark.sql.warehouse.dir", warehouse_path)
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.catalog.listDatabases()

    # Enable dynamic partition overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    return spark
